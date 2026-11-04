import assert from 'node:assert/strict';
import { test } from 'node:test';
import { runBackfill, runIncrementalRest, runSync, type RestDeps } from '../src/lib/restRuns.js';
import { defaultState, type IngestionState } from '../src/lib/state.js';
import type { RestFindData, RestTx } from '../src/idl/restTx.js';

const cfg: any = {
  runId: 'r1', mode: 'backfill', restBaseUrl: 'http://x', restPageSize: 2,
  backfillOverlapMs: 0, incrementalOverlapMs: 0, actionTypes: 'Swap',
  backfillPagesPerFile: 50,
};

type FakeOpts = { committed?: boolean; closeThrows?: boolean };

function fakeTarget(opts: FakeOpts = {}) {
  const sinks: { columns: readonly string[]; rows: any[]; closed: boolean; fileName: string; mode?: string }[] = [];
  const history: IngestionState[] = [];
  const manifests: any[] = [];
  let saved: IngestionState | undefined;
  const target: any = {
    supportsResumeSkip: false,
    async loadState() { return saved ?? defaultState(); },
    async saveState(s: IngestionState) { saved = JSON.parse(JSON.stringify(s)); history.push(saved!); },
    createSink(fileName: string, columns: readonly string[], mode?: string) {
      const sink = { columns, fileName, mode, rows: [] as any[], closed: false,
        async append(rs: any[]) { sink.rows.push(...rs); },
        async close() {
          if (opts.closeThrows) throw new Error('commit failed');
          sink.closed = true; return { location: 'l', rows: sink.rows.length, bytes: 1, sha256: 'x' };
        } };
      sinks.push(sink); return sink;
    },
    async hasCommittedFile() { return opts.committed === true; },
    async writeManifest(m: any) { manifests.push(m); },
  };
  return { target, sinks, history, manifests, getSaved: () => saved };
}

const tx = (h: string, t: number): RestTx => ({ txHash: h, txTime: t } as RestTx);
const logger: any = { info() {}, warn() {}, error() {} };

test('runBackfill pages newest->oldest and stops below floor', async () => {
  // canisterMaxTxTime=100, overlap=0 => floor=100
  const pages: Record<number, RestTx[]> = {
    1: [tx('a', 300), tx('b', 250)],
    2: [tx('c', 200), tx('d', 90)], // 90 < floor 100 -> stop after this page
  };
  const deps: RestDeps = {
    now: () => 1000,
    fetchData: async (p) => ({ totalElements: 4, page: p.page, limit: p.limit, content: pages[p.page] ?? [] } as RestFindData),
  };
  const state = defaultState();
  state.canisterMaxTxTime = 100;
  const { target, sinks, getSaved } = fakeTarget();
  await runBackfill(cfg, target, logger, state, deps);

  assert.equal(sinks[0].rows.length, 4);             // all 4 rows emitted
  assert.equal(sinks[0].columns[0], 'tx_hash');      // REST headers used
  assert.equal(getSaved()?.rest?.backfillComplete, true);
  assert.equal(getSaved()?.rest?.incrementalWatermark, 300); // seeded to newest captured txTime
  assert.equal(sinks.length, 1); // 2 pages fit in one file at pagesPerFile=50
});

test('runBackfill rotates files and advances the cursor only after each commit', async () => {
  const pages: Record<number, RestTx[]> = {
    1: [tx('a', 300), tx('b', 250)],
    2: [tx('c', 200), tx('d', 90)], // 90 < floor 100 -> stop after this page
  };
  const deps: RestDeps = {
    now: () => 1000,
    fetchData: async (p) => ({ totalElements: 4, page: p.page, limit: p.limit, content: pages[p.page] ?? [] } as RestFindData),
  };
  const state = defaultState();
  state.canisterMaxTxTime = 100;
  const { target, sinks, getSaved } = fakeTarget();
  await runBackfill({ ...cfg, backfillPagesPerFile: 1 }, target, logger, state, deps);

  assert.equal(sinks.length, 2);            // one file per page
  assert.equal(sinks[0].rows.length, 2);
  assert.equal(sinks[1].rows.length, 2);
  assert.equal(sinks[0].closed, true);      // each file committed
  assert.equal(getSaved()?.rest?.backfillComplete, true);
  assert.equal(getSaved()?.rest?.backfillCursor?.nextPage, 3); // cursor past both committed pages
});

test('runIncrementalRest emits only rows past the watermark and dedups', async () => {
  const pages: Record<number, RestTx[]> = {
    1: [tx('z', 500), tx('y', 400)],
    2: [tx('x', 300), tx('w', 50)], // 50 <= watermark(200)-0 -> stop
  };
  const deps: RestDeps = {
    now: () => 1000,
    fetchData: async (p) => ({ totalElements: 4, page: p.page, limit: p.limit, content: pages[p.page] ?? [] } as RestFindData),
  };
  const state = defaultState();
  state.rest = { backfillComplete: true, incrementalWatermark: 200, recentTxHashes: ['x'] };
  const { target, sinks, getSaved } = fakeTarget();
  await runIncrementalRest({ ...cfg, mode: 'incremental' }, target, logger, state, deps);

  // x is filtered (known hash); w is emitted as a new hash on the fetched page.
  assert.deepEqual(sinks[0].rows.map((r) => r.tx_hash), ['z', 'y', 'w']);
  assert.equal(getSaved()?.rest?.incrementalWatermark, 500);
});

const okFetch = (pages: Record<number, RestTx[]>): RestDeps => ({
  now: () => 1000,
  fetchData: async (p) => ({ totalElements: 9, page: p.page, limit: p.limit, content: pages[p.page] ?? [] } as RestFindData),
});

test('M1: an incremental run whose first page is empty creates no sink (nothing to announce) and keeps state', async () => {
  const state = defaultState();
  state.rest = { backfillComplete: true, incrementalWatermark: 200, recentTxHashes: [] };
  const { target, sinks, getSaved, manifests } = fakeTarget();
  await runIncrementalRest({ ...cfg, mode: 'incremental' }, target, logger, state, okFetch({}));

  assert.equal(sinks.length, 0);
  assert.equal(getSaved()?.rest?.incrementalWatermark, 200);
  assert.equal(manifests[0].totals.files, 0);
});

test('M1: an incremental run where every row is already known creates no sink either', async () => {
  const state = defaultState();
  state.rest = { backfillComplete: true, incrementalWatermark: 500, recentTxHashes: ['a', 'b'] };
  const { target, sinks } = fakeTarget();
  await runIncrementalRest({ ...cfg, mode: 'incremental' }, target, logger, state, okFetch({ 1: [tx('a', 500), tx('b', 400)] }));
  assert.equal(sinks.length, 0);
});

test('L4: incremental refuses to run before backfill has completed', async () => {
  const { target } = fakeTarget();
  await assert.rejects(
    () => runIncrementalRest({ ...cfg, mode: 'incremental' }, target, logger, defaultState(), okFetch({})),
    /backfill/i
  );
});

test('M2: pending incremental is recorded before the file commits and cleared afterwards', async () => {
  const state = defaultState();
  state.rest = { backfillComplete: true, incrementalWatermark: 100, recentTxHashes: [] };
  const { target, history } = fakeTarget();
  await runIncrementalRest({ ...cfg, mode: 'incremental' }, target, logger, state, okFetch({ 1: [tx('n1', 300), tx('n2', 200)] }));

  assert.equal(history.length, 2);
  assert.deepEqual(history[0].rest?.pendingIncremental, {
    runId: 'r1', fileName: 'transactions_incremental_r1.csv', hashes: ['n1', 'n2'], maxSeen: 300,
  });
  assert.equal(history[0].rest?.incrementalWatermark, 100); // cursor not advanced yet
  assert.equal(history[1].rest?.pendingIncremental, undefined);
  assert.equal(history[1].rest?.incrementalWatermark, 300);
});

test('M2: crash after file commit but before saveState does not duplicate rows on the next run', async () => {
  const state = defaultState();
  state.rest = { backfillComplete: true, incrementalWatermark: 100, recentTxHashes: [] };
  const crash = fakeTarget({ closeThrows: true });
  await assert.rejects(
    () => runIncrementalRest({ ...cfg, mode: 'incremental' }, crash.target, logger, state, okFetch({ 1: [tx('n1', 300), tx('n2', 200)] })),
    /commit failed/
  );
  const persisted = crash.getSaved()!; // what a restarted process would load
  const reload = (): IngestionState => JSON.parse(JSON.stringify(persisted));

  // The file did commit (e.g. the crash hit between commit and saveState): resume dedupes it.
  const resumed = fakeTarget({ committed: true });
  await runIncrementalRest({ ...cfg, runId: 'r2', mode: 'incremental' }, resumed.target, logger, reload(), okFetch({ 1: [tx('n1', 300), tx('n2', 200), tx('n3', 400)] }));
  assert.deepEqual(resumed.sinks[0].rows.map((r) => r.tx_hash), ['n3']);
  assert.equal(resumed.getSaved()?.rest?.pendingIncremental, undefined);

  // The file did NOT commit: nothing may be skipped.
  const lost = fakeTarget({ committed: false });
  await runIncrementalRest({ ...cfg, runId: 'r2', mode: 'incremental' }, lost.target, logger, reload(), okFetch({ 1: [tx('n1', 300), tx('n2', 200)] }));
  assert.deepEqual(lost.sinks[0].rows.map((r) => r.tx_hash), ['n1', 'n2']);
});

test('backfill failing mid-batch leaves the cursor at the last committed batch and never completes', async () => {
  const pages: Record<number, RestTx[]> = {
    1: [tx('a', 300), tx('b', 250)],
    2: [tx('c', 240), tx('d', 230)],
  };
  const deps: RestDeps = {
    now: () => 1000,
    fetchData: async (p) => {
      if (p.page === 3) throw new Error('rest down');
      return { totalElements: 9, page: p.page, limit: p.limit, content: pages[p.page] ?? [] } as RestFindData;
    },
  };
  const state = defaultState();
  state.canisterMaxTxTime = 100;
  const { target, sinks, getSaved } = fakeTarget();
  await assert.rejects(() => runBackfill({ ...cfg, backfillPagesPerFile: 1 }, target, logger, state, deps), /rest down/);

  assert.equal(sinks.length, 2);
  assert.equal(sinks.every((s) => s.closed), true);
  assert.equal(getSaved()?.rest?.backfillCursor?.nextPage, 3);
  assert.notEqual(getSaved()?.rest?.backfillComplete, true);
});

test('backfill failing inside a multi-page batch commits nothing and does not advance the cursor', async () => {
  const deps: RestDeps = {
    now: () => 1000,
    fetchData: async (p) => {
      if (p.page === 2) throw new Error('rest down');
      return { totalElements: 9, page: p.page, limit: p.limit, content: [tx('a', 300), tx('b', 250)] } as RestFindData;
    },
  };
  const state = defaultState();
  state.canisterMaxTxTime = 100;
  const { target, sinks, getSaved } = fakeTarget();
  await assert.rejects(() => runBackfill(cfg, target, logger, state, deps), /rest down/);
  assert.equal(sinks[0].closed, false);
  assert.equal(getSaved(), undefined);
});

test('M10: sinks are created with the effective mode (sync -> backfill then incremental)', async () => {
  const pages: Record<number, RestTx[]> = { 1: [tx('a', 300), tx('b', 90)] };
  const state = defaultState();
  state.canisterMaxTxTime = 100;
  const { target, sinks } = fakeTarget();
  await runSync({ ...cfg, mode: 'sync' }, target, logger, state, okFetch(pages));
  assert.deepEqual(sinks.map((s) => s.mode), ['backfill', 'incremental']);
});
