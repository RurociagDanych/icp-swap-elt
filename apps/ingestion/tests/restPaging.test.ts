import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  computeBackfillFloor, reachedFloor, isShortPage,
  reachedWatermark, maxTxTime, newTxs, canisterSeamMs,
} from '../src/lib/restPaging.js';
import type { RestTx } from '../src/idl/restTx.js';

const tx = (txHash: string, txTime: number) => ({ txHash, txTime } as RestTx);

test('canisterSeamMs converts canister epoch-seconds to REST-comparable ms', () => {
  // Canister timestamps are seconds (e.g. 1695898747 = 2023-09-28); the REST feed
  // and the backfill seam are ms. Regression guard for the seconds-vs-ns seam bug
  // that produced canisterMaxTxTime=1780 (≈1970) instead of ~1.78e12.
  assert.equal(canisterSeamMs(1695898747n), 1695898747000);
  assert.equal(canisterSeamMs(0n), 0);
});

test('computeBackfillFloor prefers explicit floor', () => {
  assert.equal(computeBackfillFloor(1000, 50, 200), 200);
});
test('computeBackfillFloor derives from canister max minus overlap', () => {
  assert.equal(computeBackfillFloor(1000, 50), 950);
});
test('computeBackfillFloor throws when nothing is available', () => {
  assert.throws(() => computeBackfillFloor(undefined, 50), /backfill floor/);
});

test('reachedFloor true once oldest row dips below floor', () => {
  assert.equal(reachedFloor([tx('a', 300), tx('b', 100)], 200), true);
  assert.equal(reachedFloor([tx('a', 300), tx('b', 250)], 200), false);
});

test('isShortPage detects a partial page', () => {
  assert.equal(isShortPage([tx('a', 1)], 100), true);
  assert.equal(isShortPage(new Array(100).fill(tx('a', 1)), 100), false);
});

test('reachedWatermark accounts for overlap', () => {
  assert.equal(reachedWatermark([tx('a', 500), tx('b', 90)], 100, 20), false); // 90 <= 80? no
  assert.equal(reachedWatermark([tx('a', 500), tx('b', 70)], 100, 20), true);  // 70 <= 80 -> true
});

test('maxTxTime returns the largest txTime or fallback', () => {
  assert.equal(maxTxTime([tx('a', 5), tx('b', 9)], 0), 9);
  assert.equal(maxTxTime([], 42), 42);
});

test('newTxs filters known hashes', () => {
  const known = new Set(['a']);
  assert.deepEqual(newTxs([tx('a', 1), tx('b', 2)], known).map((t) => t.txHash), ['b']);
});
