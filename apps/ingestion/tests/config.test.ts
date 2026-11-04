import assert from 'node:assert/strict';
import { test } from 'node:test';
import { parseArgs } from '../src/lib/config.js';

// Make sure ambient env vars don't leak into default-value assertions.
delete process.env.S3_BUCKET;
delete process.env.S3_PREFIX;

test('parseArgs applies defaults', () => {
  const cfg = parseArgs([]);

  assert.equal(cfg.mode, 'sync');
  assert.equal(cfg.host, 'https://ic0.app');
  assert.equal(cfg.pageSize, 1000);
  assert.equal(cfg.concurrency, 5);
  assert.equal(cfg.s3Bucket, undefined);
  assert.equal(cfg.s3Prefix, 'icp-swap-elt');
});

test('parseArgs reads CLI flags', () => {
  const cfg = parseArgs([
    '--mode', 'incremental',
    '--page-size', '500',
    '--concurrency', '2',
    '--s3-bucket', 'my-bucket',
    '--s3-prefix', 'custom',
    '--event-bus', 'bus',
  ]);

  assert.equal(cfg.mode, 'incremental');
  assert.equal(cfg.pageSize, 500);
  assert.equal(cfg.concurrency, 2);
  assert.equal(cfg.s3Bucket, 'my-bucket');
  assert.equal(cfg.s3Prefix, 'custom');
});

test('parseArgs reads the event bus from the flag or EVENT_BUS_NAME', () => {
  assert.equal(parseArgs([]).eventBus, undefined);
  assert.equal(parseArgs(['--event-bus', 'default']).eventBus, 'default');
  process.env.EVENT_BUS_NAME = 'from-env';
  try {
    assert.equal(parseArgs([]).eventBus, 'from-env');
  } finally {
    delete process.env.EVENT_BUS_NAME;
  }
});

test('parseArgs rejects unknown flags instead of ignoring them', () => {
  assert.throws(() => parseArgs(['--page-sise', '10']), /Unknown flag --page-sise/);
});

test('parseArgs rejects page-size outside 1..1000', () => {
  assert.throws(() => parseArgs(['--page-size', '0']), /page-size/);
  assert.throws(() => parseArgs(['--page-size', '1001']), /page-size/);
  assert.throws(() => parseArgs(['--page-size', 'abc']), /page-size/);
});

test('parseArgs rejects concurrency outside 1..20', () => {
  assert.throws(() => parseArgs(['--concurrency', '0']), /concurrency/);
  assert.throws(() => parseArgs(['--concurrency', '21']), /concurrency/);
});

test('--overlap is a deprecated no-op: still validated, ignored, and reported as a warning', () => {
  assert.throws(() => parseArgs(['--overlap', '-1']), /overlap/);
  const cfg = parseArgs(['--overlap', '10']);
  assert.equal('overlap' in cfg, false);
  assert.ok(cfg.warnings.some((w) => /--overlap.*(deprecated|ignored)/i.test(w)));
});

test('parseArgs reads REST flags and defaults mode to sync', () => {
  const cfg = parseArgs([]);
  assert.equal(cfg.mode, 'sync');
  assert.equal(cfg.restBaseUrl, 'https://api.icpswap.com/info');
  assert.equal(cfg.restPageSize, 100);
  assert.equal(cfg.backfillOverlapMs, 3600000);
  assert.equal(cfg.incrementalOverlapMs, 300000);
  assert.equal(cfg.actionTypes, 'Swap,AddLiquidity,DecreaseLiquidity,Claim');
  assert.equal(cfg.backfillFloor, undefined);
  assert.equal(cfg.backfillPagesPerFile, 50);
});

test('parseArgs rejects backfill-pages-per-file below 1', () => {
  assert.throws(() => parseArgs(['--backfill-pages-per-file', '0']), /backfill-pages-per-file/);
});

test('parseArgs parses explicit REST flags', () => {
  const cfg = parseArgs([
    '--mode', 'backfill',
    '--rest-page-size', '50',
    '--backfill-overlap-ms', '60000',
    '--backfill-floor', '1749254400000',
  ]);
  assert.equal(cfg.mode, 'backfill');
  assert.equal(cfg.restPageSize, 50);
  assert.equal(cfg.backfillOverlapMs, 60000);
  assert.equal(cfg.backfillFloor, 1749254400000);
});

test('parseArgs rejects rest-page-size above 100', () => {
  assert.throws(() => parseArgs(['--rest-page-size', '101']), /rest-page-size/);
});

test('parseArgs rejects an unknown or misspelled mode instead of falling through to sync', () => {
  assert.throws(() => parseArgs(['--mode', 'increment']), /Invalid --mode=increment/);
  for (const mode of ['sync', 'canister', 'full', 'backfill', 'incremental']) {
    assert.equal(parseArgs(['--mode', mode]).mode, mode);
  }
});

test('parseArgs rejects a flag with a missing value (trailing flag or value that is another flag)', () => {
  assert.throws(() => parseArgs(['--mode', 'sync', '--s3-bucket']), /Missing value for --s3-bucket/);
  assert.throws(() => parseArgs(['--s3-bucket', '--s3-prefix', 'p']), /Missing value for --s3-bucket/);
  assert.throws(() => parseArgs(['--page-size']), /Missing value for --page-size/);
  // negative numbers are values, not flags
  assert.equal(parseArgs(['--backfill-floor', '5']).backfillFloor, 5);
});

test('parseArgs fails fast when an S3 bucket has no event bus unless explicitly opted out', () => {
  assert.throws(() => parseArgs(['--s3-bucket', 'b']), /event bus/i);
  const cfg = parseArgs(['--s3-bucket', 'b', '--no-event-bus']);
  assert.equal(cfg.eventBus, undefined);
  assert.ok(cfg.warnings.some((w) => /will NOT announce/.test(w)));
  assert.equal(parseArgs(['--s3-bucket', 'b', '--event-bus', 'bus']).warnings.length, 0);
  assert.throws(() => parseArgs(['--s3-bucket', 'b', '--event-bus', 'bus', '--no-event-bus']), /--no-event-bus/);
});

test('parseArgs normalizes the S3 prefix and rejects empty or malformed ones', () => {
  const ok = (prefix: string) => parseArgs(['--s3-prefix', prefix]).s3Prefix;
  assert.equal(ok('custom/'), 'custom');
  assert.equal(ok('/a/b//'), 'a/b');
  assert.throws(() => parseArgs(['--s3-prefix', '/']), /s3-prefix/);
  assert.throws(() => parseArgs(['--s3-prefix', 'a//b']), /s3-prefix/);
});
