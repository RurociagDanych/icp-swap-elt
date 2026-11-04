import type { Logger } from './logger.js';

export type RunStatus = 'succeeded' | 'failed';

export type FinalizeFn = (status: RunStatus, opts?: { announceTimeoutMs?: number }) => Promise<void>;

// Shutdown for a run: announce what was written, THEN release the lock (releasing first
// would let the next run start while the announcement is still in flight). Idempotent, so
// the normal exit path and a SIGTERM racing it share one execution. A failed announcement
// never masks the run's own error; for a successful run it is thrown after the lock is released.
export function createFinalizer(deps: {
  logger: Logger;
  announce: (status: RunStatus) => Promise<void>;
  release: () => Promise<void>;
}): FinalizeFn {
  let done: Promise<void> | undefined;
  return (status, opts = {}) => {
    done ??= (async () => {
      let announceError: unknown;
      let timer: NodeJS.Timeout | undefined;
      try {
        const pending = deps.announce(status);
        pending.catch(() => {}); // a late failure after a timeout must not be unhandled
        if (opts.announceTimeoutMs !== undefined) {
          const timeout = new Promise<never>((_, reject) => {
            timer = setTimeout(() => reject(new Error(`announcement timed out after ${opts.announceTimeoutMs}ms`)), opts.announceTimeoutMs);
          });
          await Promise.race([pending, timeout]);
        } else {
          await pending;
        }
      } catch (err) {
        announceError = err;
      } finally {
        if (timer) clearTimeout(timer);
      }
      try {
        await deps.release();
      } catch (err) {
        deps.logger.error('could not release run lock', { error: String((err as Error)?.message || err) });
      }
      if (announceError !== undefined) {
        if (status === 'succeeded') throw announceError;
        deps.logger.error('could not announce failed run', { error: String((announceError as Error)?.message || announceError) });
      }
    })();
    return done;
  };
}

// SIGTERM/SIGINT: Fargate gives a short grace period before SIGKILL, so the announcement is
// bounded by announceTimeoutMs; the lock is released and the process exits either way.
export function createShutdownHandler(deps: {
  logger: Logger;
  finalize: FinalizeFn;
  exit: (code: number) => void;
  announceTimeoutMs: number;
}): (signal: string, code: number) => Promise<void> {
  let inFlight: Promise<void> | undefined;
  return (signal, code) => {
    inFlight ??= (async () => {
      deps.logger.error(`${signal} received - announcing failed run, releasing lock and exiting`);
      try {
        await deps.finalize('failed', { announceTimeoutMs: deps.announceTimeoutMs });
      } catch (err) {
        deps.logger.error('shutdown cleanup failed', { error: String((err as Error)?.message || err) });
      }
      deps.exit(code);
    })();
    return inFlight;
  };
}
