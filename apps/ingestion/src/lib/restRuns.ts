import type { Config } from './config.js';
import type { Logger } from './logger.js';
import type { StorageTarget, ManifestEntry } from './storageTarget.js';
import { buildManifest } from './storageTarget.js';
import type { IngestionState, RestState } from './state.js';
import { defaultRestState, pushBounded, RECENT_HASHES_LIMIT } from './state.js';
import type { FindParams } from './restClient.js';
import type { RestFindData } from '../idl/restTx.js';
import { restHeaders, rowFromRestTx } from './restCsv.js';
import {
  computeBackfillFloor, reachedFloor, isShortPage,
  reachedWatermark, maxTxTime, newTxs,
} from './restPaging.js';

export type RestDeps = {
  fetchData: (p: FindParams) => Promise<RestFindData>;
  now: () => number;
};

export async function runBackfill(cfg: Config, target: StorageTarget, logger: Logger, state: IngestionState, deps: RestDeps): Promise<void> {
  const rest = state.rest ?? defaultRestState();
  const endSnapshot = rest.backfillCursor?.endSnapshot ?? deps.now();
  const floor = computeBackfillFloor(state.canisterMaxTxTime, cfg.backfillOverlapMs, cfg.backfillFloor);
  let page = rest.backfillCursor?.nextPage ?? 1;
  logger.info('starting backfill', {
    floor, endSnapshot, fromPage: page,
    restPageSize: cfg.restPageSize, pagesPerFile: cfg.backfillPagesPerFile,
  });

  const manifestEntries: ManifestEntry[] = [];
  let totalWritten = 0;
  let total = 0;
  let maxSeen = 0;
  let done = false;

  // Each batch is written to its own committed file BEFORE the durable cursor advances,
  // so an interrupted run never leaves the cursor past data that was not persisted.
  while (!done) {
    const startPage = page;
    const fileName = `transactions_backfill_${endSnapshot}_p${String(startPage).padStart(6, '0')}.csv`;
    let sink: ReturnType<StorageTarget['createSink']> | undefined;
    let fileRows = 0;
    let writeHeader = true;
    let pagesInFile = 0;
    let oldestTxTime: number | undefined;

    while (pagesInFile < cfg.backfillPagesPerFile) {
      const data = await deps.fetchData({
        baseUrl: cfg.restBaseUrl, page, limit: cfg.restPageSize,
        actionTypes: cfg.actionTypes, begin: floor, end: endSnapshot,
      });
      total = data.totalElements;
      const content = data.content;
      if (content.length === 0) { done = true; break; }

      if (!sink) sink = target.createSink(fileName, restHeaders, cfg.mode);
      await sink.append(content.map(rowFromRestTx), writeHeader);
      writeHeader = false;
      fileRows += content.length;
      maxSeen = maxTxTime(content, maxSeen);
      oldestTxTime = content[content.length - 1]?.txTime;
      pagesInFile += 1;

      const stop = reachedFloor(content, floor) || isShortPage(content, cfg.restPageSize);
      logger.info('backfill page', {
        page, rows: content.length, fileRows, oldestTxTime,
        msAboveFloor: oldestTxTime !== undefined ? oldestTxTime - floor : undefined,
        totalElements: total,
      });
      page += 1;
      if (stop) { done = true; break; }
    }

    if (sink) {
      const stats = await sink.close(); // commits this file to S3
      manifestEntries.push({ storageId: `rest-backfill-p${startPage}`, total, written: fileRows, sink: stats } as ManifestEntry);
      totalWritten += fileRows;
      // Only now is the batch durable — advance the cursor past it.
      rest.backfillCursor = { endSnapshot, nextPage: page };
      rest.backfillFloor = floor;
      state.rest = rest;
      await target.saveState(state);
      logger.info('backfill batch committed', {
        file: stats.location, startPage, endPage: page - 1, rows: fileRows,
        oldestTxTime, cumulativeRows: totalWritten,
      });
    }
  }

  rest.backfillComplete = true;
  // Seed the incremental watermark to the newest record backfill captured (it pages
  // from the snapshot down to the floor, so page 1 holds the newest rows). Without
  // this, the first incremental run would rescan the entire REST history.
  const seed = maxSeen > 0 ? maxSeen : endSnapshot;
  if ((rest.incrementalWatermark ?? 0) < seed) rest.incrementalWatermark = seed;
  state.rest = rest;
  state.mode = 'backfill';
  state.lastRunAt = new Date().toISOString();
  await target.saveState(state);
  await target.writeManifest(buildManifest(cfg, manifestEntries));
  logger.info('backfill done', {
    files: manifestEntries.length, rows: totalWritten, floor, endSnapshot,
    watermark: rest.incrementalWatermark,
  });
}

// Crash window: the incremental file is committed (sink.close) BEFORE the watermark/hash
// state is saved, and the two cannot be made one atomic step on S3. To keep a crash in that
// window from duplicating rows, the run first persists `pendingIncremental` (runId, file
// name, the hashes it emitted, the newest txTime) and only clears it together with the
// watermark advance. A later run that finds a pending record checks whether that file was
// actually committed: if yes, it folds the hashes/watermark in (rows are not re-emitted);
// if no, it discards the record and re-fetches normally. Remaining at-least-once cases
// (e.g. more than RECENT_HASHES_LIMIT rows in one run, a crash after saveState but before the
// manifest, backfill batches re-run under a new runId) are absorbed by the Silver stage,
// which de-duplicates on tx_hash.
async function resolvePendingIncremental(
  target: StorageTarget, logger: Logger, state: IngestionState, rest: RestState,
): Promise<void> {
  const pending = rest.pendingIncremental;
  if (!pending) return;
  const committed = await target.hasCommittedFile(pending.runId, pending.fileName, 'incremental');
  if (committed) {
    pushBounded(rest.recentTxHashes, pending.hashes, RECENT_HASHES_LIMIT);
    if ((rest.incrementalWatermark ?? 0) < pending.maxSeen) rest.incrementalWatermark = pending.maxSeen;
    logger.warn('recovered committed incremental file from an interrupted run', {
      runId: pending.runId, fileName: pending.fileName, hashes: pending.hashes.length, watermark: rest.incrementalWatermark,
    });
  } else {
    logger.warn('discarding pending incremental record: its file was never committed', {
      runId: pending.runId, fileName: pending.fileName,
    });
  }
  delete rest.pendingIncremental;
  state.rest = rest;
  await target.saveState(state);
}

export async function runIncrementalRest(cfg: Config, target: StorageTarget, logger: Logger, state: IngestionState, deps: RestDeps): Promise<void> {
  const rest = state.rest ?? defaultRestState();
  // Without a watermark seeded by backfill, incremental would scan the whole REST history
  // with an unbounded known-hash set.
  if (rest.backfillComplete !== true) {
    throw new Error('Incremental requires a completed backfill (state.rest.backfillComplete). Run --mode backfill or --mode sync first.');
  }
  await resolvePendingIncremental(target, logger, state, rest);

  const watermark = rest.incrementalWatermark ?? 0;
  const end = deps.now();
  const known = new Set(rest.recentTxHashes);
  let page = 1;
  logger.info('starting rest incremental', { watermark, end });

  // The sink is created lazily on the first fresh row: an empty run leaves no 0-byte,
  // header-less CSV behind and registers no prefix, so nothing empty is announced.
  const fileName = `transactions_incremental_${cfg.runId}.csv`;
  let sink: ReturnType<StorageTarget['createSink']> | undefined;
  const emittedHashes: string[] = [];
  let written = 0;
  let total = 0;
  let maxSeen = watermark;
  let writeHeader = true;

  for (;;) {
    const data = await deps.fetchData({
      baseUrl: cfg.restBaseUrl, page, limit: cfg.restPageSize,
      actionTypes: cfg.actionTypes, end,
    });
    total = data.totalElements;
    const content = data.content;
    if (content.length === 0) break;

    const fresh = newTxs(content, known);
    if (fresh.length > 0) {
      if (!sink) sink = target.createSink(fileName, restHeaders, cfg.mode);
      await sink.append(fresh.map(rowFromRestTx), writeHeader);
      writeHeader = false;
      written += fresh.length;
      for (const t of fresh) { known.add(t.txHash); emittedHashes.push(t.txHash); }
    }
    maxSeen = maxTxTime(content, maxSeen);

    if (reachedWatermark(content, watermark, cfg.incrementalOverlapMs) || isShortPage(content, cfg.restPageSize)) break;
    page += 1;
  }

  const entries: ManifestEntry[] = [];
  if (sink) {
    // Record intent durably BEFORE the file commits (see the crash-window note above).
    rest.pendingIncremental = { runId: cfg.runId, fileName, hashes: emittedHashes.slice(-RECENT_HASHES_LIMIT), maxSeen };
    state.rest = rest;
    await target.saveState(state);
    const stats = await sink.close();
    entries.push({ storageId: 'rest-incremental', total, written, sink: stats });
  }
  const recentTxHashes: string[] = [];
  pushBounded(recentTxHashes, Array.from(known), RECENT_HASHES_LIMIT);
  rest.incrementalWatermark = maxSeen;
  rest.recentTxHashes = recentTxHashes;
  delete rest.pendingIncremental;
  state.rest = rest;
  state.mode = 'incremental';
  state.lastRunAt = new Date().toISOString();
  await target.saveState(state);
  await target.writeManifest(buildManifest(cfg, entries));
  logger.info('rest incremental done', { written, watermark: maxSeen, wroteFile: sink !== undefined });
}

export async function runSync(cfg: Config, target: StorageTarget, logger: Logger, state: IngestionState, deps: RestDeps): Promise<void> {
  if (state.rest?.backfillComplete !== true) {
    await runBackfill({ ...cfg, mode: 'backfill' }, target, logger, state, deps);
  }
  await runIncrementalRest({ ...cfg, mode: 'incremental' }, target, logger, state, deps);
}
