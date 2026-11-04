import assert from 'node:assert/strict';
import { test } from 'node:test';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import type { S3Client } from '@aws-sdk/client-s3';

// Retries are exercised below; keep their backoff negligible.
process.env.RETRY_BASE_DELAY_MS = '1';
import type { Config } from '../src/lib/config.js';
import { defaultState } from '../src/lib/state.js';
import { LOCK_TTL_MS, LocalTarget, S3Target, buildManifest, modePathSegment, type ManifestEntry } from '../src/lib/storageTarget.js';

test('modePathSegment separates sources', () => {
  assert.equal(modePathSegment('canister'), 'canister');
  assert.equal(modePathSegment('full'), 'canister');
  assert.equal(modePathSegment('backfill'), 'rest/backfill');
  assert.equal(modePathSegment('incremental'), 'rest/incremental');
});

function makeCfg(overrides: Partial<Config> = {}): Config {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'icp-swap-elt-target-'));
  return {
    host: 'https://ic0.app',
    baseStorageCanisterId: 'g54jq-hiaaa-aaaag-qck5q-cai',
    pageSize: 1000,
    mode: 'full',
    outDir: dir,
    stateFile: path.join(dir, 'state.json'),
    logFile: path.join(dir, 'ingestion.log'),
    overlap: 50,
    concurrency: 5,
    s3Bucket: undefined,
    s3Prefix: 'icp-swap-elt',
    runId: 'run-1',
    ...overrides,
  };
}

type SentCommand = { name: string; input: Record<string, unknown> };

function makeFakeS3(handlers: Record<string, (input: Record<string, unknown>) => unknown> = {}) {
  const sent: SentCommand[] = [];
  const client = {
    send: async (cmd: { constructor: { name: string }; input: Record<string, unknown> }) => {
      const name = cmd.constructor.name;
      sent.push({ name, input: cmd.input });
      const handler = handlers[name];
      if (handler) return handler(cmd.input);
      return {};
    },
  };
  return { sent, client: client as unknown as S3Client };
}

function sampleEntries(): ManifestEntry[] {
  return [
    { storageId: 'a', total: 10, written: 10, sink: { location: 'x', rows: 10, bytes: 100, sha256: 'h1' } },
    { storageId: 'b', total: 5, written: 4, sink: { location: 'y', rows: 4, bytes: 50, sha256: 'h2' } },
  ];
}

test('buildManifest sums totals and stamps schemaVersion', () => {
  const manifest = buildManifest({ runId: 'run-1', mode: 'full' }, sampleEntries());

  assert.equal(manifest.schemaVersion, 1);
  assert.equal(manifest.runId, 'run-1');
  assert.equal(manifest.mode, 'full');
  assert.equal(manifest.totals.files, 2);
  assert.equal(manifest.totals.rows, 14);
  assert.equal(manifest.totals.bytes, 150);
  assert.ok(manifest.generatedAt);
});

test('LocalTarget round-trips state and reports missing state as default', async () => {
  const target = new LocalTarget(makeCfg());

  assert.deepEqual(await target.loadState(), defaultState());

  const state = defaultState();
  state.canisters['c1'] = { lastTotal: 9, recentHashes: ['h'], completed: false };
  await target.saveState(state);

  assert.deepEqual(await target.loadState(), state);
});

test('LocalTarget writes the manifest into outDir', async () => {
  const cfg = makeCfg();
  const target = new LocalTarget(cfg);

  await target.writeManifest(buildManifest(cfg, sampleEntries()));

  const file = path.join(cfg.outDir, 'manifest_full_run-1.json');
  const manifest = JSON.parse(fs.readFileSync(file, 'utf8'));
  assert.equal(manifest.totals.rows, 14);
});

test('S3Target saves state under <prefix>/state/state.json', async () => {
  const { sent, client } = makeFakeS3();
  const target = new S3Target(client, makeCfg({ s3Bucket: 'bkt' }));

  const state = defaultState();
  await target.saveState(state);

  assert.equal(sent.length, 1);
  assert.equal(sent[0].name, 'PutObjectCommand');
  assert.equal(sent[0].input.Bucket, 'bkt');
  assert.equal(sent[0].input.Key, 'icp-swap-elt/state/state.json');
  assert.deepEqual(JSON.parse(String(sent[0].input.Body)), state);
});

test('S3Target reports the prefixes its sinks were created under', () => {
  const { client } = makeFakeS3();
  const target = new S3Target(client, makeCfg({ s3Bucket: 'bkt', mode: 'sync', runId: 'run-1' }));
  assert.deepEqual(target.writtenPrefixes(), []);

  target.createSink('a.csv', ['x']);
  target.createSink('b.csv', ['x']);

  assert.deepEqual(target.writtenPrefixes(), ['icp-swap-elt/rest/incremental/run-1/']);
});

test('S3Target returns defaultState when the state object is missing', async () => {
  const { client } = makeFakeS3({
    GetObjectCommand: () => {
      throw Object.assign(new Error('no such key'), { name: 'NoSuchKey' });
    },
  });
  const target = new S3Target(client, makeCfg({ s3Bucket: 'bkt' }));

  assert.deepEqual(await target.loadState(), defaultState());
});

test('S3Target parses state fetched from S3', async () => {
  const stored = defaultState();
  stored.latestStorageId = 'zzz';
  const { client } = makeFakeS3({
    GetObjectCommand: () => ({ Body: { transformToString: async () => JSON.stringify(stored) } }),
  });
  const target = new S3Target(client, makeCfg({ s3Bucket: 'bkt' }));

  const loaded = await target.loadState();
  assert.equal(loaded.latestStorageId, 'zzz');
});

test('S3Target writes the manifest under the runId prefix', async () => {
  const { sent, client } = makeFakeS3();
  const cfg = makeCfg({ s3Bucket: 'bkt', mode: 'incremental' });
  const target = new S3Target(client, cfg);

  await target.writeManifest(buildManifest(cfg, sampleEntries()));

  assert.equal(sent[0].name, 'PutObjectCommand');
  assert.equal(sent[0].input.Key, 'icp-swap-elt/rest/incremental/run-1/manifest_incremental.json');
});

test('S3Target puts each manifest of a sync run under its own mode segment', async () => {
  const { sent, client } = makeFakeS3();
  const cfg = makeCfg({ s3Bucket: 'bkt', mode: 'sync' });
  const target = new S3Target(client, cfg);

  await target.writeManifest(buildManifest({ ...cfg, mode: 'backfill' }, sampleEntries()));
  await target.writeManifest(buildManifest({ ...cfg, mode: 'incremental' }, sampleEntries()));

  assert.deepEqual(sent.map((c) => c.input.Key), [
    'icp-swap-elt/rest/backfill/run-1/manifest_backfill.json',
    'icp-swap-elt/rest/incremental/run-1/manifest_incremental.json',
  ]);
});

test('LocalTarget lock: acquire, conflict, release, reacquire', async () => {
  const cfg = makeCfg();
  const first = new LocalTarget(cfg);
  const second = new LocalTarget(cfg);

  assert.equal(await first.acquireRunLock(), true);
  assert.equal(await second.acquireRunLock(), false);

  await first.releaseRunLock();
  assert.equal(await second.acquireRunLock(), true);
});

test('LocalTarget lock: steals a stale lock past the TTL', async () => {
  const cfg = makeCfg();
  const lockFile = path.join(path.dirname(cfg.stateFile), 'run.lock');
  fs.writeFileSync(
    lockFile,
    JSON.stringify({ runId: 'old', startedAt: new Date(Date.now() - LOCK_TTL_MS - 60_000).toISOString() })
  );

  const target = new LocalTarget(cfg);
  assert.equal(await target.acquireRunLock(), true);
});

test('S3Target lock: acquire uses a conditional create', async () => {
  const { sent, client } = makeFakeS3();
  const target = new S3Target(client, makeCfg({ s3Bucket: 'bkt' }));

  assert.equal(await target.acquireRunLock(), true);
  assert.equal(sent[0].name, 'PutObjectCommand');
  assert.equal(sent[0].input.Key, 'icp-swap-elt/locks/run.lock');
  assert.equal(sent[0].input.IfNoneMatch, '*');
});

test('S3Target lock: fresh lock held by another run is respected', async () => {
  const freshLock = JSON.stringify({ runId: 'other', startedAt: new Date().toISOString() });
  const { client } = makeFakeS3({
    PutObjectCommand: (input) => {
      if (input.IfNoneMatch) throw Object.assign(new Error('held'), { name: 'PreconditionFailed' });
      return {};
    },
    GetObjectCommand: () => ({ Body: { transformToString: async () => freshLock } }),
  });
  const target = new S3Target(client, makeCfg({ s3Bucket: 'bkt' }));

  assert.equal(await target.acquireRunLock(), false);
});

const STALE = () => JSON.stringify({ runId: 'old', startedAt: new Date(Date.now() - LOCK_TTL_MS - 60_000).toISOString() });

test('H5: a stale lock is stolen with a conditional If-Match on the observed ETag', async () => {
  const { sent, client } = makeFakeS3({
    PutObjectCommand: (input) => {
      if (input.IfNoneMatch) throw Object.assign(new Error('held'), { name: 'PreconditionFailed' });
      return { ETag: '"new"' };
    },
    GetObjectCommand: () => ({ ETag: '"stale-etag"', Body: { transformToString: async () => STALE() } }),
  });
  const target = new S3Target(client, makeCfg({ s3Bucket: 'bkt' }));

  assert.equal(await target.acquireRunLock(), true);
  const lastPut = sent.filter((c) => c.name === 'PutObjectCommand').at(-1)!;
  assert.equal(lastPut.input.IfNoneMatch, undefined);
  assert.equal(lastPut.input.IfMatch, '"stale-etag"');
  await target.releaseRunLock();
});

test('H5: losing the steal race (If-Match fails) means the lock is not ours', async () => {
  const { client } = makeFakeS3({
    PutObjectCommand: () => { throw Object.assign(new Error('412'), { name: 'PreconditionFailed' }); },
    GetObjectCommand: () => ({ ETag: '"e"', Body: { transformToString: async () => STALE() } }),
  });
  const target = new S3Target(client, makeCfg({ s3Bucket: 'bkt' }));
  assert.equal(await target.acquireRunLock(), false);
});

// In-memory S3 with real conditional-write semantics, for race tests.
function makeStatefulS3() {
  const store = new Map<string, { body: string; etag: string }>();
  let version = 0;
  const log: SentCommand[] = [];
  const precondition = () => Object.assign(new Error('412'), { name: 'PreconditionFailed', $metadata: { httpStatusCode: 412 } });
  const client = {
    send: async (cmd: { constructor: { name: string }; input: Record<string, any> }) => {
      const name = cmd.constructor.name;
      const input = cmd.input;
      log.push({ name, input });
      await new Promise((r) => setImmediate(r)); // let concurrent callers interleave
      const cur = store.get(input.Key);
      if (name === 'PutObjectCommand') {
        if (input.IfNoneMatch === '*' && cur) throw precondition();
        if (input.IfMatch && cur?.etag !== input.IfMatch) throw precondition();
        const etag = `"v${++version}"`;
        store.set(input.Key, { body: String(input.Body), etag });
        return { ETag: etag };
      }
      if (name === 'GetObjectCommand') {
        if (!cur) throw Object.assign(new Error('nope'), { name: 'NoSuchKey' });
        return { ETag: cur.etag, Body: { transformToString: async () => cur.body } };
      }
      if (name === 'HeadObjectCommand') {
        if (!cur) throw Object.assign(new Error('nf'), { name: 'NotFound' });
        return { ETag: cur.etag };
      }
      if (name === 'DeleteObjectCommand') {
        if (input.IfMatch && cur?.etag !== input.IfMatch) throw precondition();
        store.delete(input.Key);
        return {};
      }
      return {};
    },
  };
  return { store, log, client: client as unknown as S3Client };
}

test('H5: two runs racing to steal a stale lock - exactly one wins', async () => {
  const fake = makeStatefulS3();
  fake.store.set('icp-swap-elt/locks/run.lock', { body: STALE(), etag: '"v0"' });
  const a = new S3Target(fake.client, makeCfg({ s3Bucket: 'bkt', runId: 'run-a' }));
  const b = new S3Target(fake.client, makeCfg({ s3Bucket: 'bkt', runId: 'run-b' }));

  const results = await Promise.all([a.acquireRunLock(), b.acquireRunLock()]);
  assert.deepEqual(results.filter(Boolean).length, 1);
  const owner = JSON.parse(fake.store.get('icp-swap-elt/locks/run.lock')!.body).runId;
  assert.equal(owner, results[0] ? 'run-a' : 'run-b');
  await a.releaseRunLock();
  await b.releaseRunLock();
});

test('H5: release never deletes a lock that belongs to another run', async () => {
  const fake = makeStatefulS3();
  const a = new S3Target(fake.client, makeCfg({ s3Bucket: 'bkt', runId: 'run-a' }));
  assert.equal(await a.acquireRunLock(), true);
  // our lock expires and another run steals it
  fake.store.set('icp-swap-elt/locks/run.lock', {
    body: JSON.stringify({ runId: 'run-b', startedAt: new Date().toISOString() }), etag: '"stolen"',
  });
  await a.releaseRunLock();
  assert.equal(JSON.parse(fake.store.get('icp-swap-elt/locks/run.lock')!.body).runId, 'run-b');
  assert.equal(fake.log.some((c) => c.name === 'DeleteObjectCommand'), false);
});

test('H5: refreshRunLock heartbeats startedAt with If-Match and detects a lost lock', async () => {
  const fake = makeStatefulS3();
  const a = new S3Target(fake.client, makeCfg({ s3Bucket: 'bkt', runId: 'run-a' }), () => {}, { heartbeatMs: 0 });
  assert.equal(await a.acquireRunLock(), true);
  const key = 'icp-swap-elt/locks/run.lock';
  const before = fake.store.get(key)!;
  await new Promise((r) => setTimeout(r, 5));
  await a.refreshRunLock();
  const after = fake.store.get(key)!;
  assert.notEqual(after.etag, before.etag);
  assert.ok(Date.parse(JSON.parse(after.body).startedAt) >= Date.parse(JSON.parse(before.body).startedAt));
  assert.equal(fake.log.at(-1)!.input.IfMatch, before.etag);

  fake.store.set(key, { body: JSON.stringify({ runId: 'run-b', startedAt: new Date().toISOString() }), etag: '"b"' });
  await assert.rejects(() => a.refreshRunLock(), /lock/i);
  // once the lock is lost, durable writes must stop
  await assert.rejects(() => a.saveState(defaultState()), /lock/i);
});

test('H5: a background heartbeat refreshes the lock until release', async () => {
  const fake = makeStatefulS3();
  const a = new S3Target(fake.client, makeCfg({ s3Bucket: 'bkt', runId: 'run-a' }), () => {}, { heartbeatMs: 10 });
  await a.acquireRunLock();
  await new Promise((r) => setTimeout(r, 80));
  const refreshes = fake.log.filter((c) => c.name === 'PutObjectCommand' && c.input.IfMatch).length;
  assert.ok(refreshes >= 2, `expected heartbeat puts, saw ${refreshes}`);
  await a.releaseRunLock();
  const afterRelease = fake.log.length;
  await new Promise((r) => setTimeout(r, 40));
  assert.equal(fake.log.length, afterRelease);
});

test('M5: writeManifest and state reads are retried on transient 5xx errors', async () => {
  let puts = 0;
  let gets = 0;
  const transient = () => Object.assign(new Error('slow down'), { name: 'ServiceUnavailable', $metadata: { httpStatusCode: 503 } });
  const { client } = makeFakeS3({
    PutObjectCommand: () => { if (++puts === 1) throw transient(); return {}; },
    GetObjectCommand: () => {
      if (++gets === 1) throw transient();
      return { Body: { transformToString: async () => JSON.stringify(defaultState()) } };
    },
  });
  const cfg = makeCfg({ s3Bucket: 'bkt' });
  const target = new S3Target(client, cfg, () => {});
  await target.writeManifest(buildManifest(cfg, sampleEntries()));
  assert.equal(puts, 2);
  assert.deepEqual(await target.loadState(), defaultState());
  assert.equal(gets, 2);
});

test('M6: an existing but empty state object is an error, not the default state', async () => {
  const { client } = makeFakeS3({
    GetObjectCommand: () => ({ Body: { transformToString: async () => '' } }),
  });
  const target = new S3Target(client, makeCfg({ s3Bucket: 'bkt' }));
  await assert.rejects(() => target.loadState(), /empty/i);
});

test('M10: createSink honors the effective mode for the path segment', () => {
  const { client } = makeFakeS3();
  const target = new S3Target(client, makeCfg({ s3Bucket: 'bkt', mode: 'sync', runId: 'run-1' }));
  target.createSink('a.csv', ['x'], 'backfill');
  target.createSink('b.csv', ['x'], 'incremental');
  assert.deepEqual(target.writtenPrefixes(), [
    'icp-swap-elt/rest/backfill/run-1/',
    'icp-swap-elt/rest/incremental/run-1/',
  ]);
});

test('hasCommittedFile checks the object under the given runId and mode', async () => {
  const fake = makeStatefulS3();
  fake.store.set('icp-swap-elt/rest/incremental/old-run/f.csv', { body: 'x', etag: '"1"' });
  const target = new S3Target(fake.client, makeCfg({ s3Bucket: 'bkt', mode: 'incremental' }));
  assert.equal(await target.hasCommittedFile('old-run', 'f.csv', 'incremental'), true);
  assert.equal(await target.hasCommittedFile('old-run', 'missing.csv', 'incremental'), false);
});

test('M7: local saveState is atomic and local loadState rejects a malformed state file', async () => {
  const cfg = makeCfg();
  const target = new LocalTarget(cfg);
  await target.saveState(defaultState());
  fs.writeFileSync(cfg.stateFile, '{"canisters": []}');
  await assert.rejects(() => target.loadState(), /Invalid state/);
});

test('M7: local release leaves a lock that belongs to another run', async () => {
  const cfg = makeCfg();
  const lockFile = path.join(path.dirname(cfg.stateFile), 'run.lock');
  const mine = new LocalTarget(cfg);
  assert.equal(await mine.acquireRunLock(), true);
  fs.writeFileSync(lockFile, JSON.stringify({ runId: 'someone-else', startedAt: new Date().toISOString() }));
  await mine.releaseRunLock();
  assert.equal(fs.existsSync(lockFile), true);
});

test('M7: concurrent stale-lock steals on the local target yield exactly one owner', async () => {
  const cfg = makeCfg();
  const lockFile = path.join(path.dirname(cfg.stateFile), 'run.lock');
  fs.writeFileSync(lockFile, STALE());
  const a = new LocalTarget({ ...cfg, runId: 'run-a' });
  const b = new LocalTarget({ ...cfg, runId: 'run-b' });
  const results = await Promise.all([a.acquireRunLock(), b.acquireRunLock()]);
  assert.equal(results.filter(Boolean).length, 1);
  assert.equal(JSON.parse(fs.readFileSync(lockFile, 'utf8')).runId, results[0] ? 'run-a' : 'run-b');
  assert.deepEqual(fs.readdirSync(path.dirname(lockFile)).filter((f) => f.includes('stale')), []);
});

test('M7: refreshRunLock on the local target rewrites startedAt only while the lock is ours', async () => {
  const cfg = makeCfg();
  const lockFile = path.join(path.dirname(cfg.stateFile), 'run.lock');
  const t = new LocalTarget(cfg, { heartbeatMs: 0 });
  await t.acquireRunLock();
  fs.writeFileSync(lockFile, JSON.stringify({ runId: 'run-1', startedAt: '2000-01-01T00:00:00.000Z' }));
  await t.refreshRunLock();
  assert.ok(Date.parse(JSON.parse(fs.readFileSync(lockFile, 'utf8')).startedAt) > Date.parse('2001-01-01'));
  fs.writeFileSync(lockFile, JSON.stringify({ runId: 'other', startedAt: new Date().toISOString() }));
  await assert.rejects(() => t.refreshRunLock(), /lock/i);
  await t.releaseRunLock();
});

test('hasCommittedFile on the local target checks the final file', async () => {
  const cfg = makeCfg();
  const t = new LocalTarget(cfg);
  fs.writeFileSync(path.join(cfg.outDir, 'f.csv'), 'x');
  assert.equal(await t.hasCommittedFile('whatever', 'f.csv', 'incremental'), true);
  assert.equal(await t.hasCommittedFile('whatever', 'nope.csv', 'incremental'), false);
});

test('S3Target lock: release deletes the lock object when it is ours', async () => {
  const { sent, client } = makeFakeS3({
    GetObjectCommand: () => ({
      ETag: '"mine"',
      Body: { transformToString: async () => JSON.stringify({ runId: 'run-1', startedAt: new Date().toISOString() }) },
    }),
  });
  const target = new S3Target(client, makeCfg({ s3Bucket: 'bkt' }));

  await target.acquireRunLock();
  await target.releaseRunLock();

  const last = sent.at(-1)!;
  assert.equal(last.name, 'DeleteObjectCommand');
  assert.equal(last.input.Key, 'icp-swap-elt/locks/run.lock');
  assert.equal(last.input.IfMatch, '"mine"');
});
