import type { PoolInfoActor, SwapTx } from '../idl/poolInfo.js';
import type { Config } from './config.js';
import { headers as canisterHeaders, rowFromTx } from './csv.js';
import type { CsvSink, SinkStats } from './csvSink.js';
import type { Logger } from './logger.js';
import { parallelLimit } from './parallel.js';
import { canisterSeamMs } from './restPaging.js';
import { withRetry, type WarnFn } from './retry.js';
import { pushBounded, type CanisterState, type IngestionState } from './state.js';
import { buildManifest, type ManifestEntry, type StorageTarget } from './storageTarget.js';

// The slice of the canister actor the archive load needs (injectable for tests).
export type StorageReader = Pick<PoolInfoActor, 'getBaseRecord'>;

export async function fetchCanisterFull(
  storage: StorageReader,
  storageId: string,
  pageSize: number,
  sink: CsvSink,
  writeHeader: boolean,
  logger: Logger,
): Promise<{ written: number; total: number; recentHashes: string[]; sinkStats: SinkStats; maxTimestampSec: bigint }> {
  const warn: WarnFn = (msg) => logger.warn(msg, { storageId });

  const head = await withRetry(
    () => storage.getBaseRecord(0n, 1n, []),
    `${storageId} head`,
    warn
  );
  const total = Number(head.totalElements);
  logger.info('canister total', { storageId, total });

  if (total === 0) {
    if (writeHeader) await sink.append([], true);
    const sinkStats = await sink.close();
    return { written: 0, total, recentHashes: [], sinkStats, maxTimestampSec: 0n };
  }

  let written = 0;
  let offset = 0;
  let headerPending = writeHeader;
  let maxTimestampSec = 0n;
  const recentHashes: string[] = [];

  while (offset < total) {
    const batchSize = Math.min(pageSize, total - offset);
    const page = await withRetry(
      () => storage.getBaseRecord(BigInt(offset), BigInt(batchSize), []),
      `${storageId} offset=${offset}`,
      warn
    );
    const txs = page.content || [];
    if (!txs.length) break; // handled below: stopping short of `total` is an error

    const rows = txs.map((tx: SwapTx) => rowFromTx(storageId, tx));
    await sink.append(rows, headerPending);
    headerPending = false;
    written += rows.length;

    pushBounded(recentHashes, txs.filter((tx) => tx.hash).map((tx) => tx.hash));
    for (const tx of txs) if (tx.timestamp > maxTimestampSec) maxTimestampSec = tx.timestamp;

    if (written % (pageSize * 5) === 0 || written >= total) {
      logger.info('canister progress', { storageId, written, total, pct: Number(((written / total) * 100).toFixed(1)) });
    }

    offset += txs.length;
  }

  // An empty page while offset < total would otherwise end the canister silently short and
  // get it marked completed with missing rows.
  if (written < total) {
    throw new Error(`Canister ${storageId} paging ended at ${written} of ${total} rows (incomplete); not marking it completed`);
  }

  const sinkStats = await sink.close();
  return { written, total, recentHashes, sinkStats, maxTimestampSec };
}

export async function runFull(
  cfg: Pick<Config, 'pageSize' | 'concurrency' | 'runId' | 'mode'>,
  getStorage: (storageId: string) => Promise<StorageReader>,
  target: StorageTarget,
  logger: Logger,
  state: IngestionState,
  orderedStorages: string[],
  newestStorageId: string,
): Promise<void> {
  logger.info('starting full load', { canisters: orderedStorages.length, pageSize: cfg.pageSize, concurrency: cfg.concurrency });

  const manifestEntries: ManifestEntry[] = [];
  let saveQueue: Promise<void> = Promise.resolve();
  const queueStateSave = async () => {
    saveQueue = saveQueue.then(() => target.saveState(state));
    await saveQueue;
  };

  const tasks = orderedStorages.map((storageId, idx) => async () => {
    const fileName = `${String(idx).padStart(4, '0')}_${storageId}.csv`;
    const markedCompleted = state.canisters[storageId]?.completed === true;

    if (target.supportsResumeSkip && markedCompleted) {
      logger.info('skipping completed canister', { storageId });
      return;
    }

    logger.info('starting canister', { storageId, index: idx + 1, of: orderedStorages.length });
    const sink = target.createSink(fileName, canisterHeaders);

    const result = await fetchCanisterFull(await getStorage(storageId), storageId, cfg.pageSize, sink, true, logger);

    const canState: CanisterState = {
      lastTotal: result.total,
      lastRun: new Date().toISOString(),
      recentHashes: result.recentHashes,
      completed: storageId !== newestStorageId,
      maxTimestampSec: Number(result.maxTimestampSec),
    };
    state.canisters[storageId] = canState;
    state.lastRunAt = canState.lastRun;
    state.latestStorageId = newestStorageId;
    manifestEntries.push({
      storageId,
      total: result.total,
      written: result.written,
      sink: result.sinkStats,
    });
    await queueStateSave();
  });

  // On the first failure no new canister starts and this waits for in-flight ones to finish
  // (their sinks commit or fail) before rethrowing, so the caller announces/releases safely.
  await parallelLimit(tasks, cfg.concurrency, (err, idx) =>
    logger.error('canister failed', { storageId: orderedStorages[idx], error: String((err as Error)?.message || err) }));
  const totalRows = manifestEntries.reduce((sum, entry) => sum + entry.written, 0);

  state.mode = 'full';
  state.latestStorageId = newestStorageId;
  state.lastRunAt = new Date().toISOString();
  // Record the canister↔REST seam in epoch ms (canister timestamps are seconds; the REST
  // feed is ms) and mark the one-time archive complete so future runs use only backfill +
  // REST incremental. The seam comes from EVERY canister's persisted maxTimestampSec, not
  // just those processed in this run (a local resume skips completed ones). If any canister
  // has no known timestamp the seam is unknown, so the archive is NOT marked complete:
  // backfill would otherwise fail later with no floor.
  let seamSec = 0;
  let seamKnown = true;
  for (const id of orderedStorages) {
    const ts = state.canisters[id]?.maxTimestampSec;
    if (ts === undefined) seamKnown = false;
    else if (ts > seamSec) seamSec = ts;
  }
  if (seamKnown && seamSec > 0) {
    state.canisterMaxTxTime = canisterSeamMs(BigInt(seamSec));
    state.canisterArchiveComplete = true;
  } else {
    logger.warn('canister seam unknown (a canister has no recorded timestamp, or all are empty); not marking the archive complete', { seamKnown, seamSec });
  }
  await target.saveState(state);
  await target.writeManifest(buildManifest(cfg, manifestEntries));

  logger.info('full load done', { rows: totalRows, files: manifestEntries.length, newestStorageId, canisterMaxTxTime: state.canisterMaxTxTime });
}

