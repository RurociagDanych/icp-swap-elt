import assert from 'node:assert/strict';
import { test } from 'node:test';
import { createFinalizer, createShutdownHandler } from '../src/lib/shutdown.js';

const logger: any = { info() {}, warn() {}, error() {} };
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

test('finalize announces before releasing the lock, and only once', async () => {
  const order: string[] = [];
  const finalize = createFinalizer({
    logger,
    announce: async (status) => { order.push(`announce:${status}`); },
    release: async () => { order.push('release'); },
  });
  await Promise.all([finalize('failed'), finalize('succeeded')]);
  await finalize('failed');
  assert.deepEqual(order, ['announce:failed', 'release']);
});

test('finalize still releases the lock when the announcement fails; failed runs swallow, successful runs throw', async () => {
  let released = 0;
  const failing = (status: 'succeeded' | 'failed') => createFinalizer({
    logger,
    announce: async () => { throw new Error('bus down'); },
    release: async () => { released++; },
  })(status);
  await failing('failed');
  await assert.rejects(() => failing('succeeded'), /bus down/);
  assert.equal(released, 2);
});

test('H3: SIGTERM announces a failed run, then releases the lock, then exits with the signal code', async () => {
  const order: string[] = [];
  const exits: number[] = [];
  const finalize = createFinalizer({
    logger,
    announce: async (status) => { order.push(`announce:${status}`); },
    release: async () => { order.push('release'); },
  });
  const handler = createShutdownHandler({ logger, finalize, exit: (c) => { order.push('exit'); exits.push(c); }, announceTimeoutMs: 1000 });
  await handler('SIGTERM', 143);
  assert.deepEqual(order, ['announce:failed', 'release', 'exit']);
  assert.deepEqual(exits, [143]);
});

test('H3: a hanging announcement is bounded by the timeout; the lock is still released and the process exits', async () => {
  const order: string[] = [];
  const finalize = createFinalizer({
    logger,
    announce: () => new Promise<void>(() => {}),
    release: async () => { order.push('release'); },
  });
  const handler = createShutdownHandler({ logger, finalize, exit: () => { order.push('exit'); }, announceTimeoutMs: 30 });
  await handler('SIGTERM', 143);
  assert.deepEqual(order, ['release', 'exit']);
});

test('a second signal while shutting down does not exit twice or announce again', async () => {
  let announces = 0;
  let exits = 0;
  const finalize = createFinalizer({
    logger,
    announce: async () => { announces++; await sleep(20); },
    release: async () => {},
  });
  const handler = createShutdownHandler({ logger, finalize, exit: () => { exits++; }, announceTimeoutMs: 1000 });
  await Promise.all([handler('SIGTERM', 143), handler('SIGINT', 130)]);
  assert.equal(announces, 1);
  assert.equal(exits, 1);
});
