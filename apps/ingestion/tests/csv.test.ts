import assert from 'node:assert/strict';
import { test } from 'node:test';
import { createHash } from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import type { SwapTx } from '../src/idl/poolInfo.js';
import { headers, rowFromTx } from '../src/lib/csv.js';
import { LocalCsvSink } from '../src/lib/csvSink.js';

function makeTx(overrides: Partial<SwapTx> = {}): SwapTx {
  return {
    to: 'to-principal',
    action: { swap: null },
    token0Id: 'tok0',
    token1Id: 'tok1',
    liquidityTotal: 123456789n,
    from: 'from-principal',
    hash: 'abc123',
    tick: -42n,
    token1Price: 1.5,
    recipient: 'recipient-principal',
    token0ChangeAmount: 10.25,
    sender: 'sender-principal',
    liquidityChange: 999n,
    token1Standard: 'ICRC2',
    token0Fee: 0.003,
    token1Fee: 0.003,
    timestamp: 1718000000n,
    token1ChangeAmount: -4.5,
    token1Decimals: 8,
    token0Standard: 'ICRC1',
    amountUSD: 100.5,
    amountToken0: 10.25,
    amountToken1: 4.5,
    poolFee: 3000n,
    token0Symbol: 'ICP',
    token0Decimals: 8,
    token0Price: 9.8,
    token1Symbol: 'ckBTC',
    poolId: 'pool-1',
    ...overrides,
  };
}

test('rowFromTx maps a swap transaction to a flat CSV row', () => {
  const row = rowFromTx('storage-1', makeTx());

  assert.equal(row.storage_canister, 'storage-1');
  assert.equal(row.hash, 'abc123');
  assert.equal(row.action, 'swap');
  assert.equal(row.timestamp, '1718000000');
  assert.equal(row.tick, '-42');
  assert.equal(row.pool_fee, '3000');
  assert.equal(row.liquidity_total, '123456789');
  assert.equal(row.liquidity_change, '999');
  assert.equal(row.token0_change, 10.25);
  assert.equal(row.amount_usd, 100.5);
});

test('rowFromTx falls back to "unknown" for an empty action variant', () => {
  const row = rowFromTx('storage-1', makeTx({ action: {} }));
  assert.equal(row.action, 'unknown');
});

test('LocalCsvSink writes header, escapes special characters, and reports stats', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'icp-swap-elt-csv-'));
  const file = path.join(dir, 'out.csv');

  const plain = rowFromTx('storage-1', makeTx());
  const tricky = rowFromTx(
    'storage-1',
    makeTx({ hash: 'def456', sender: 'evil,"sender"\nline2', token0Symbol: 'A,B' })
  );

  const sink = new LocalCsvSink(file, headers);
  await sink.append([plain], true);
  await sink.append([tricky], false);
  const stats = await sink.close();

  const content = fs.readFileSync(file, 'utf8');
  const lines = content.split('\n');

  assert.equal(lines[0], headers.join(','));
  assert.ok(content.includes('"evil,""sender""\nline2"'), 'comma/quote/newline value must be quoted and doubled');
  assert.equal(stats.rows, 2);
  assert.equal(stats.bytes, Buffer.byteLength(content, 'utf8'));
  assert.equal(stats.sha256, createHash('sha256').update(content, 'utf8').digest('hex'));
  assert.equal(stats.location, file);

  fs.rmSync(dir, { recursive: true, force: true });
});

test('LocalCsvSink rejects writes after close', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'icp-swap-elt-csv-'));
  const file = path.join(dir, 'out.csv');

  const sink = new LocalCsvSink(file, headers);
  await sink.append([rowFromTx('s', makeTx())], true);
  await sink.close();

  await assert.rejects(() => sink.append([], false), /already closed/);
  fs.rmSync(dir, { recursive: true, force: true });
});

test('LocalCsvSink writes arbitrary columns with header + checksum', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'icp-swap-elt-sink-'));
  const file = path.join(dir, 'out.csv');
  const sink = new LocalCsvSink(file, ['a', 'b']);
  await sink.append([{ a: 'x', b: 1 }], true);
  const stats = await sink.close();

  assert.equal(fs.readFileSync(file, 'utf8'), 'a,b\nx,1\n');
  assert.equal(stats.rows, 1);
  assert.ok(stats.sha256.length === 64);
  fs.rmSync(dir, { recursive: true, force: true });
});
