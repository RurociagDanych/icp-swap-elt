import type { RestTx } from '../idl/restTx.js';

// Canister `timestamp` fields are epoch SECONDS (e.g. 1695898747); the REST API's
// `txTime` (and therefore the backfill seam/floor) is epoch MILLISECONDS. Convert
// the newest canister timestamp to ms so the two feeds share one comparable unit.
export function canisterSeamMs(maxTimestampSeconds: bigint): number {
  return Number(maxTimestampSeconds) * 1000;
}

export function computeBackfillFloor(canisterMaxTxTime: number | undefined, overlapMs: number, explicitFloor?: number): number {
  if (explicitFloor !== undefined) return explicitFloor;
  if (canisterMaxTxTime !== undefined) return canisterMaxTxTime - overlapMs;
  throw new Error('Cannot determine backfill floor: no canisterMaxTxTime in state and no --backfill-floor provided');
}

const oldest = (content: RestTx[]): RestTx | undefined => content[content.length - 1];

export function reachedFloor(content: RestTx[], floor: number): boolean {
  const last = oldest(content);
  return last !== undefined && last.txTime < floor;
}

export function isShortPage(content: RestTx[], limit: number): boolean {
  return content.length < limit;
}

export function reachedWatermark(content: RestTx[], watermark: number, overlapMs: number): boolean {
  const last = oldest(content);
  return last !== undefined && last.txTime <= watermark - overlapMs;
}

export function maxTxTime(content: RestTx[], fallback: number): number {
  return content.reduce((m, t) => (t.txTime > m ? t.txTime : m), fallback);
}

export function newTxs(content: RestTx[], known: Set<string>): RestTx[] {
  return content.filter((t) => !known.has(t.txHash));
}
