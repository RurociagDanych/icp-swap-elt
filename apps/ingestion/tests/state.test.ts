import assert from 'node:assert/strict';
import { test } from 'node:test';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {
  RECENT_HASHES_LIMIT,
  defaultRestState,
  defaultState,
  loadState,
  parseState,
  pushBounded,
  saveState,
} from '../src/lib/state.js';

test('parseState parses the current schema', () => {
  const state = parseState(
    JSON.stringify({
      mode: 'incremental',
      lastRunAt: '2026-06-01T00:00:00.000Z',
      latestStorageId: 'aaaaa-aa',
      canisters: {
        'aaaaa-aa': { lastTotal: 42, recentHashes: ['h1', 'h2'], completed: false },
      },
    })
  );

  assert.equal(state.mode, 'incremental');
  assert.equal(state.latestStorageId, 'aaaaa-aa');
  assert.equal(state.canisters['aaaaa-aa'].lastTotal, 42);
  assert.deepEqual(state.canisters['aaaaa-aa'].recentHashes, ['h1', 'h2']);
});

test('parseState preserves rest + canister-seam fields', () => {
  const state = parseState(
    JSON.stringify({
      mode: 'incremental',
      canisters: {},
      canisterArchiveComplete: true,
      canisterMaxTxTime: 1749254400000,
      rest: {
        backfillComplete: false,
        backfillCursor: { endSnapshot: 1782127219000, nextPage: 137 },
        backfillFloor: 1749254400000,
        incrementalWatermark: 1782200000000,
        recentTxHashes: ['ttnzy-244431'],
      },
    })
  );

  assert.equal(state.canisterArchiveComplete, true);
  assert.equal(state.canisterMaxTxTime, 1749254400000);
  assert.equal(state.rest?.backfillCursor?.nextPage, 137);
  assert.deepEqual(state.rest?.recentTxHashes, ['ttnzy-244431']);
});

test('defaultRestState starts with an empty hash list', () => {
  assert.deepEqual(defaultRestState(), { recentTxHashes: [] });
});

test('parseState migrates the legacy single-canister schema', () => {
  const state = parseState(
    JSON.stringify({
      mode: 'incremental',
      lastRunAt: '2026-01-01T00:00:00.000Z',
      latestStorageId: 'bbbbb-bb',
      latestStorageTotal: 7,
      recentHashes: ['x'],
    })
  );

  assert.equal(state.canisters['bbbbb-bb'].lastTotal, 7);
  assert.deepEqual(state.canisters['bbbbb-bb'].recentHashes, ['x']);
  assert.equal(state.canisters['bbbbb-bb'].lastRun, '2026-01-01T00:00:00.000Z');
});

test('saveState/loadState round-trips through a file', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'icp-swap-elt-state-'));
  const file = path.join(dir, 'state.json');

  const state = defaultState();
  state.mode = 'full';
  state.canisters['ccccc-cc'] = { lastTotal: 100, recentHashes: ['a'], completed: true };

  saveState(file, state);
  const loaded = loadState(file);

  assert.deepEqual(loaded, state);
  fs.rmSync(dir, { recursive: true, force: true });
});

test('loadState returns defaultState for a missing file', () => {
  const loaded = loadState(path.join(os.tmpdir(), 'does-not-exist-icp-swap-elt', 'state.json'));
  assert.deepEqual(loaded, defaultState());
});

test('pushBounded keeps only the most recent entries', () => {
  const target = ['a', 'b'];
  pushBounded(target, ['c', 'd', 'e'], 3);
  assert.deepEqual(target, ['c', 'd', 'e']);
  pushBounded(target, ['f'], 3);
  assert.deepEqual(target, ['d', 'e', 'f']);
});

test('pushBounded leaves arrays under the limit untouched', () => {
  const target = ['a'];
  pushBounded(target, ['b'], 5);
  assert.deepEqual(target, ['a', 'b']);
});

test('RECENT_HASHES_LIMIT is exported for callers', () => {
  assert.equal(RECENT_HASHES_LIMIT, 5000);
});

test('saveState replaces the file atomically (temp file + rename, no leftovers)', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'icp-swap-elt-atomic-'));
  const file = path.join(dir, 'state.json');
  saveState(file, defaultState());
  const inoBefore = fs.statSync(file).ino;
  const next = defaultState();
  next.latestStorageId = 'x';
  saveState(file, next);
  assert.notEqual(fs.statSync(file).ino, inoBefore); // renamed over, not truncated in place
  assert.deepEqual(fs.readdirSync(dir), ['state.json']);
  assert.equal(loadState(file).latestStorageId, 'x');
});

test('parseState rejects malformed shapes', () => {
  const bad: unknown[] = [
    [],
    null,
    'x',
    { canisters: [] },
    { canisters: { a: { lastTotal: 'many', recentHashes: [] } } },
    { canisters: { a: { lastTotal: 1, recentHashes: 'h' } } },
    { canisters: { a: { lastTotal: 1, recentHashes: [1] } } },
    { mode: 'bogus', canisters: {} },
    { canisters: {}, canisterMaxTxTime: 'soon' },
    { canisters: {}, canisterArchiveComplete: 'yes' },
    { canisters: {}, rest: { recentTxHashes: 'h' } },
    { canisters: {}, rest: { recentTxHashes: [], incrementalWatermark: 'x' } },
    { canisters: {}, rest: { recentTxHashes: [], backfillCursor: { endSnapshot: 1 } } },
    { canisters: {}, rest: { recentTxHashes: [], pendingIncremental: { runId: 'r' } } },
  ];
  for (const doc of bad) {
    assert.throws(() => parseState(JSON.stringify(doc)), /Invalid state/, JSON.stringify(doc));
  }
});

test('parseState round-trips the persisted seam and pending-incremental fields', () => {
  const state = parseState(JSON.stringify({
    mode: 'full',
    canisters: { a: { lastTotal: 3, recentHashes: [], maxTimestampSec: 1700000000 } },
    rest: {
      recentTxHashes: [],
      pendingIncremental: { runId: 'r1', fileName: 'f.csv', hashes: ['h'], maxSeen: 9 },
    },
  }));
  assert.equal(state.canisters.a.maxTimestampSec, 1700000000);
  assert.deepEqual(state.rest?.pendingIncremental, { runId: 'r1', fileName: 'f.csv', hashes: ['h'], maxSeen: 9 });
});
