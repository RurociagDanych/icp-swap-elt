import { EventBridgeClient } from '@aws-sdk/client-eventbridge';
import type { Config } from './config.js';
import { publishRunFinished, type EventPublisher } from './events.js';
import type { Logger } from './logger.js';
import type { WarnFn } from './retry.js';
import type { StorageTarget } from './storageTarget.js';

// The event is the only trigger of the transformation stage, so it gets more attempts than
// ordinary calls (about 80s of backoff in total with the default delays).
export const ANNOUNCE_RETRIES = 8;

// Tells the transformation stage which S3 prefixes this run wrote files under, so it loads
// exactly that data. Also sent after a failed run: files committed before the failure are
// complete and the resume cursor has already moved past them.
export async function announceRun(
  cfg: Config,
  target: StorageTarget,
  logger: Logger,
  status: 'succeeded' | 'failed',
  warn: WarnFn,
  deps: { client?: EventPublisher; retries?: number } = {},
): Promise<void> {
  if (!cfg.s3Bucket) return;
  if (!cfg.eventBus) {
    logger.warn('no event bus configured (--no-event-bus): not announcing the run; the transformation stage will not be triggered');
    return;
  }
  const prefixes = target.writtenPrefixes();
  if (prefixes.length === 0) {
    logger.info('no files written; not announcing the run');
    return;
  }
  await publishRunFinished(
    deps.client ?? new EventBridgeClient({}),
    cfg.eventBus,
    { status, runId: cfg.runId, mode: cfg.mode, bucket: cfg.s3Bucket, prefixes },
    warn,
    deps.retries ?? ANNOUNCE_RETRIES
  );
  logger.info('announced run', { status, prefixes: prefixes.length });
}
