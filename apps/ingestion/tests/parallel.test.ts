import assert from 'node:assert/strict';
import { test } from 'node:test';
import { parallelLimit } from '../src/lib/parallel.js';

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

test('parallelLimit returns results in task order and honors the limit', async () => {
  let running = 0;
  let peak = 0;
  const tasks = [1, 2, 3, 4, 5].map((n) => async () => {
    running++;
    peak = Math.max(peak, running);
    await sleep(5);
    running--;
    return n * 2;
  });
  assert.deepEqual(await parallelLimit(tasks, 2), [2, 4, 6, 8, 10]);
  assert.equal(peak, 2);
});

test('H6: after the first failure no new task starts, and rejection waits for in-flight tasks', async () => {
  const started: number[] = [];
  let slowSettled = false;
  const tasks = [
    async () => { started.push(0); await sleep(5); throw new Error('canister 0 failed'); },
    async () => { started.push(1); await sleep(60); slowSettled = true; },
    async () => { started.push(2); },
    async () => { started.push(3); },
  ];
  await assert.rejects(() => parallelLimit(tasks, 2), /canister 0 failed/);
  assert.equal(slowSettled, true, 'must not reject while another worker is still running');
  assert.deepEqual(started, [0, 1]);
});

test('H6: the first error is thrown and later errors do not escape as unhandled rejections', async () => {
  const unhandled: unknown[] = [];
  const onUnhandled = (e: unknown) => unhandled.push(e);
  process.on('unhandledRejection', onUnhandled);
  try {
    const tasks = [
      async () => { await sleep(5); throw new Error('first'); },
      async () => { await sleep(20); throw new Error('second'); },
    ];
    await assert.rejects(() => parallelLimit(tasks, 2), /first/);
    await sleep(30);
    assert.deepEqual(unhandled, []);
  } finally {
    process.off('unhandledRejection', onUnhandled);
  }
});
