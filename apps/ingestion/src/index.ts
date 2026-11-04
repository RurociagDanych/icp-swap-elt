import { Actor, HttpAgent } from '@dfinity/agent';
import { baseStorageidlFactory, type BaseStorageActor } from './idl/baseStorage.js';
import { poolInfoidlFactory, type PoolInfoActor } from './idl/poolInfo.js';
import { announceRun } from './lib/announce.js';
import { parseArgs, USAGE } from './lib/config.js';
import { runFull, type StorageReader } from './lib/fullRun.js';
import { createLogger } from './lib/logger.js';
import { fetchTransactions } from './lib/restClient.js';
import { runBackfill, runIncrementalRest, runSync, type RestDeps } from './lib/restRuns.js';
import { withRetry, type WarnFn } from './lib/retry.js';
import { createFinalizer, createShutdownHandler, type RunStatus } from './lib/shutdown.js';
import { createStorageTarget } from './lib/storageTarget.js';

// Fargate's default stop grace is 30s before SIGKILL; leave room to release the lock.
const SIGNAL_ANNOUNCE_TIMEOUT_MS = 20_000;

async function getActor<T>(agent: HttpAgent, canisterId: string, factory: unknown): Promise<T> {
  return Actor.createActor(factory as any, { agent, canisterId }) as T;
}

async function run(): Promise<void> {
  if (process.argv.slice(2).includes('--help')) {
    process.stdout.write(USAGE);
    return;
  }
  const cfg = parseArgs();
  const logger = createLogger({
    base: { runId: cfg.runId, mode: cfg.mode },
    logFile: cfg.s3Bucket ? undefined : cfg.logFile,
  });
  for (const warning of cfg.warnings) logger.warn(warning);
  const warn: WarnFn = (msg) => logger.warn(msg);
  const target = createStorageTarget(cfg, warn);

  // Announce + release in one idempotent step shared by the normal exit and signal paths.
  // Fargate stops a task with SIGTERM (grace period) before SIGKILL; without this a
  // stopped task would leak its lock until the TTL and never tell the transformation stage
  // about the files it had already committed (backfill batches advance the cursor).
  let holdsLock = false;
  const finalize = createFinalizer({
    logger,
    announce: (status) => announceRun(cfg, target, logger, status, warn),
    release: async () => {
      if (holdsLock) {
        holdsLock = false;
        await target.releaseRunLock();
      }
    },
  });
  const shutdown = createShutdownHandler({
    logger, finalize, exit: (code) => process.exit(code), announceTimeoutMs: SIGNAL_ANNOUNCE_TIMEOUT_MS,
  });
  process.on('SIGTERM', () => void shutdown('SIGTERM', 143));
  process.on('SIGINT', () => void shutdown('SIGINT', 130));

  holdsLock = await target.acquireRunLock();
  if (!holdsLock) {
    logger.warn('another run holds the lock; exiting without work');
    return;
  }

  let status: RunStatus = 'failed';
  try {
    const state = await target.loadState();
    const restDeps: RestDeps = {
      fetchData: (p) => fetchTransactions(p, warn),
      now: () => Date.now(),
    };

    if (cfg.mode === 'canister' || cfg.mode === 'full') {
      const agent = new HttpAgent({ host: cfg.host });
      await agent.syncTime();
      const baseStorage = await getActor<BaseStorageActor>(agent, cfg.baseStorageCanisterId, baseStorageidlFactory);
      const storages = await withRetry(() => baseStorage.baseStorage(), 'baseStorage list', warn);
      if (!storages.length) throw new Error('No storage canisters found');
      logger.info('discovered storage canisters', { count: storages.length });
      const newestStorageId = storages[0];
      const getStorage = (id: string): Promise<StorageReader> => getActor<PoolInfoActor>(agent, id, poolInfoidlFactory);
      await runFull(cfg, getStorage, target, logger, state, [...storages].reverse(), newestStorageId);
    } else if (cfg.mode === 'backfill') {
      await runBackfill(cfg, target, logger, state, restDeps);
    } else if (cfg.mode === 'incremental') {
      await runIncrementalRest(cfg, target, logger, state, restDeps);
    } else {
      await runSync(cfg, target, logger, state, restDeps);
    }
    status = 'succeeded';
  } finally {
    // Announce first (a failed announcement never masks the run's own error), then release.
    await finalize(status);
  }
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
