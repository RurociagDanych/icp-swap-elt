import assert from 'node:assert/strict';
import { test } from 'node:test';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import type { PassThrough } from 'node:stream';
import { LocalCsvSink, StreamingCsvSink, escapeCsvValue } from '../src/lib/csvSink.js';

const tick = (ms = 30) => new Promise((r) => setTimeout(r, ms));

test('escapeCsvValue quotes commas, quotes, LF and lone CR', () => {
  assert.equal(escapeCsvValue('a,b'), '"a,b"');
  assert.equal(escapeCsvValue('a"b'), '"a""b"');
  assert.equal(escapeCsvValue('a\nb'), '"a\nb"');
  assert.equal(escapeCsvValue('a\rb'), '"a\rb"');
  assert.equal(escapeCsvValue('plain'), 'plain');
});

test('streaming sink: an upload that fails with no pending write does not raise an unhandled rejection', async () => {
  const unhandled: unknown[] = [];
  const onUnhandled = (e: unknown) => unhandled.push(e);
  process.on('unhandledRejection', onUnhandled);
  try {
    new StreamingCsvSink('s3://x', ['a'], async () => { throw new Error('upload exploded'); });
    await tick(50);
    assert.deepEqual(unhandled, []);
  } finally {
    process.off('unhandledRejection', onUnhandled);
  }
});

test('streaming sink: append rejects instead of hanging on drain when the upload already failed', { timeout: 5000 }, async () => {
  const sink = new StreamingCsvSink('s3://x', ['a'], async (stream: PassThrough) => {
    // never reads the stream (backpressure builds), then fails
    void stream;
    await tick(20);
    throw new Error('multipart failed');
  });
  const big = 'x'.repeat(200_000);
  await assert.rejects(async () => {
    for (let i = 0; i < 50; i++) await sink.append([{ a: big }]);
  }, /multipart failed/);
});

test('streaming sink: close rejects with the upload error and the stream is destroyed', { timeout: 5000 }, async () => {
  let captured: PassThrough | undefined;
  const sink = new StreamingCsvSink('s3://x', ['a'], async (stream: PassThrough) => {
    captured = stream;
    await tick(10);
    throw new Error('boom');
  });
  await tick(40);
  await assert.rejects(() => sink.append([{ a: 'x' }]), /boom/);
  await assert.rejects(() => sink.close(), /boom/);
  assert.equal(captured?.destroyed, true);
});

test('streaming sink: closing an empty sink yields zero rows and an empty sha256', async () => {
  const sink = new StreamingCsvSink('s3://x', ['a'], async (stream: PassThrough) => {
    stream.resume();
    await new Promise((r) => stream.on('end', r));
  });
  const stats = await sink.close();
  assert.equal(stats.rows, 0);
  assert.equal(stats.bytes, 0);
  assert.equal(stats.sha256, 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855');
});

test('local sink only exposes the final file once closed (atomic commit)', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'icp-sink-'));
  const file = path.join(dir, 'a.csv');
  const sink = new LocalCsvSink(file, ['a']);
  await sink.append([{ a: '1' }], true);
  assert.equal(fs.existsSync(file), false);
  await sink.close();
  assert.equal(fs.readFileSync(file, 'utf8'), 'a\n1\n');
  assert.deepEqual(fs.readdirSync(dir), ['a.csv']);
});
