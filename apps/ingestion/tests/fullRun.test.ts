import assert from 'node:assert/strict';
import { test } from 'node:test';
import { fetchCanisterFull, runFull, type StorageReader } from '../src/lib/fullRun.js';
import { defaultState, type IngestionState } from '../src/lib/state.js';

process.env.RETRY_BASE_DELAY_MS = '1';

const logger: any = { info() {}, warn() {}, error() {} };
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

const makeTx = (hash: string, timestamp: bigint): any => ({
  to: 't', action: { swap: null }, token0Id: 'a', token1Id: 'b', liquidityTotal: 1n, from: 'f', hash, tick: 1n,
  token1Price: 1, recipient: 'r', token0ChangeAmount: 1, sender: 's', liquidityChange: 1n, token1Standard: 'x',
  token0Fee: 0, token1Fee: 0, timestamp, token1ChangeAmount: 1, token1Decimals: 8, token0Standard: 'x',
  amountUSD: 1, amountToken0: 1, amountToken1: 1, poolFee: 1n, token0Symbol: 'A', token0Decimals: 8,
  token0Price: 1, token1Symbol: 'B', poolId: 'p',
});

// A canister with `total` txs; `stallAt` makes it return an empty page from that offset on.
function reader(total: number, opts: { ts?: bigint; stallAt?: number; delayMs?: number; failWith?: string } = {}): StorageReader {
  return {
    async getBaseRecord(offset: bigint, limit: bigint) {
      if (opts.delayMs) await sleep(opts.delayMs);
      if (opts.failWith) throw new Error(opts.failWith);
      const o = Number(offset);
      if (opts.stallAt !== undefined && o >= opts.stallAt) return { totalElements: BigInt(total), content: [] } as any;
      const n = Math.max(0, Math.min(Number(limit), total - o));
      const content = Array.from({ length: n }, (_, i) => makeTx(`h${o + i}`, opts.ts ?? 1000n));
      return { totalElements: BigInt(total), content } as any;
    },
  };
}

function fakeTarget(resumeSkip = false) {
  const sinks: { name: string; rows: any[]; closed: boolean }[] = [];
  const manifests: any[] = [];
  let saves = 0;
  const target: any = {
    supportsResumeSkip: resumeSkip,
    async saveState() { saves++; },
    createSink(name: string) {
      const sink = { name, rows: [] as any[], closed: false,
        async append(rs: any[]) { sink.rows.push(...rs); },
        async close() { sink.closed = true; return { location: name, rows: sink.rows.length, bytes: 1, sha256: 'x' }; } };
      sinks.push(sink); return sink;
    },
    async writeManifest(m: any) { manifests.push(m); },
  };
  return { target, sinks, manifests, saves: () => saves };
}

const cfg: any = { pageSize: 2, concurrency: 2, runId: 'r1', mode: 'full' };

test('H1: fetchCanisterFull throws when paging ends below the canister total', async () => {
  const { target } = fakeTarget();
  const sink = target.createSink('a.csv');
  await assert.rejects(
    () => fetchCanisterFull(reader(10, { stallAt: 4 }), 'can1', 2, sink, true, logger),
    /can1.*4.*10|incomplete/i
  );
});

test('H1: a canister that stalls is never marked completed and the archive is never marked complete', async () => {
  const { target } = fakeTarget();
  const state = defaultState();
  const getStorage = async (id: string) => (id === 'old' ? reader(4) : reader(10, { stallAt: 4 }));
  await assert.rejects(() => runFull(cfg, getStorage, target, logger, state, ['old', 'new'], 'new'), /incomplete|below/i);
  assert.equal(state.canisters.old.completed, true);
  assert.equal(state.canisters.new, undefined);
  assert.notEqual(state.canisterArchiveComplete, true);
});

test('runFull writes every canister, marks all but the newest completed and records the seam from all canisters', async () => {
  const { target, sinks, manifests } = fakeTarget();
  const state = defaultState();
  const getStorage = async (id: string) => (id === 'old' ? reader(3, { ts: 1000n }) : reader(2, { ts: 2000n }));
  await runFull(cfg, getStorage, target, logger, state, ['old', 'new'], 'new');
  assert.equal(sinks.length, 2);
  assert.equal(state.canisters.old.completed, true);
  assert.equal(state.canisters.new.completed, false);
  assert.equal(state.canisterArchiveComplete, true);
  assert.equal(state.canisterMaxTxTime, 2000 * 1000);
  assert.equal(state.canisters.old.maxTimestampSec, 1000);
  assert.equal(manifests[0].totals.rows, 5);
});

test('M9: resume that skips a completed canister still uses its persisted timestamp for the seam', async () => {
  const { target, sinks } = fakeTarget(true);
  const state: IngestionState = defaultState();
  state.canisters.old = { lastTotal: 3, recentHashes: [], completed: true, maxTimestampSec: 5000 };
  await runFull(cfg, async () => reader(2, { ts: 2000n }), target, logger, state, ['old', 'new'], 'new');
  assert.equal(sinks.length, 1); // old skipped
  assert.equal(state.canisterMaxTxTime, 5000 * 1000);
  assert.equal(state.canisterArchiveComplete, true);
});

test('M9: when a skipped canister has no persisted seam, the archive is not marked complete', async () => {
  const { target } = fakeTarget(true);
  const state: IngestionState = defaultState();
  state.canisters.old = { lastTotal: 3, recentHashes: [], completed: true }; // legacy state
  await runFull(cfg, async () => reader(2, { ts: 2000n }), target, logger, state, ['old', 'new'], 'new');
  assert.notEqual(state.canisterArchiveComplete, true);
  assert.equal(state.canisterMaxTxTime, undefined);
});

test('H6: a failing canister stops new dispatch, and runFull rejects only after in-flight canisters finish', async () => {
  const { target, sinks, manifests } = fakeTarget();
  const state = defaultState();
  const started: string[] = [];
  const getStorage = async (id: string) => {
    started.push(id);
    if (id === 'bad') return reader(4, { failWith: 'canister rejected' });
    if (id === 'slow') return reader(4, { delayMs: 40 });
    return reader(2);
  };
  const c = { ...cfg, concurrency: 2 };
  await assert.rejects(
    () => runFull(c, getStorage, target, logger, state, ['bad', 'slow', 'c3', 'c4'], 'c4'),
    /canister rejected/
  );
  const slowSink = sinks.find((s) => s.name.includes('slow'))!;
  assert.equal(slowSink.closed, true, 'in-flight sink must be committed before runFull rejects');
  assert.equal(state.canisters.slow.completed, true);
  assert.deepEqual(started.sort(), ['bad', 'slow']);
  assert.equal(manifests.length, 0);
  assert.notEqual(state.canisterArchiveComplete, true);
});
