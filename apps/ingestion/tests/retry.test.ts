import assert from 'node:assert/strict';
import { test } from 'node:test';
import { HttpError, isRetryable, parseRetryAfter, withRetry } from '../src/lib/retry.js';

const noWarn = () => {};

test('withRetry does not retry 4xx errors other than 429', async () => {
  let calls = 0;
  await assert.rejects(
    () => withRetry(async () => { calls++; throw new HttpError('nope', 404); }, 'x', noWarn, 5, { sleep: async () => {} }),
    /nope/
  );
  assert.equal(calls, 1);
});

test('withRetry does not retry S3 conditional-write failures (by name or $metadata status)', async () => {
  let calls = 0;
  const err = Object.assign(new Error('held'), { name: 'PreconditionFailed' });
  await assert.rejects(() => withRetry(async () => { calls++; throw err; }, 'x', noWarn, 5, { sleep: async () => {} }));
  assert.equal(calls, 1);
  assert.equal(isRetryable(Object.assign(new Error('x'), { $metadata: { httpStatusCode: 403 } })), false);
  assert.equal(isRetryable(Object.assign(new Error('x'), { $metadata: { httpStatusCode: 503 } })), true);
});

test('withRetry retries 429, 5xx and plain network errors', async () => {
  assert.equal(isRetryable(new HttpError('x', 429)), true);
  assert.equal(isRetryable(new HttpError('x', 500)), true);
  assert.equal(isRetryable(new Error('ECONNRESET')), true);
});

test('withRetry honors Retry-After when it exceeds the computed backoff', async () => {
  const sleeps: number[] = [];
  let calls = 0;
  const result = await withRetry(
    async () => {
      calls++;
      if (calls === 1) throw new HttpError('slow down', 429, 7000);
      return 'ok';
    },
    'x', noWarn, 5,
    { baseDelayMs: 100, random: () => 0, sleep: async (ms) => { sleeps.push(ms); } }
  );
  assert.equal(result, 'ok');
  assert.deepEqual(sleeps, [7000]);
});

test('withRetry applies jitter within [cap/2, cap] and respects maxDelayMs', async () => {
  const run = async (random: number) => {
    const sleeps: number[] = [];
    await assert.rejects(() => withRetry(
      async () => { throw new Error('boom'); }, 'x', noWarn, 6,
      { baseDelayMs: 1000, maxDelayMs: 4000, random: () => random, sleep: async (ms) => { sleeps.push(ms); } }
    ));
    return sleeps;
  };
  assert.deepEqual(await run(0), [500, 1000, 2000, 2000, 2000]);
  const high = await run(0.999999);
  assert.equal(high.length, 5);
  for (const [i, s] of high.entries()) assert.ok(s <= Math.min(1000 * 2 ** i, 4000) && s > Math.min(1000 * 2 ** i, 4000) * 0.99);
});

test('withRetry attempts and base delay are configurable via env defaults', async () => {
  process.env.RETRY_ATTEMPTS = '2';
  process.env.RETRY_BASE_DELAY_MS = '1';
  try {
    let calls = 0;
    await assert.rejects(() => withRetry(async () => { calls++; throw new Error('boom'); }, 'x', noWarn));
    assert.equal(calls, 2);
  } finally {
    delete process.env.RETRY_ATTEMPTS;
    delete process.env.RETRY_BASE_DELAY_MS;
  }
});

test('withRetry default is 5 attempts', async () => {
  let calls = 0;
  await assert.rejects(() => withRetry(async () => { calls++; throw new Error('boom'); }, 'x', noWarn, undefined, { sleep: async () => {} }));
  assert.equal(calls, 5);
});

test('parseRetryAfter handles seconds, HTTP dates and garbage', () => {
  assert.equal(parseRetryAfter('3'), 3000);
  assert.equal(parseRetryAfter('Wed, 21 Oct 2015 07:28:10 GMT', Date.parse('Wed, 21 Oct 2015 07:28:00 GMT')), 10000);
  assert.equal(parseRetryAfter('soon'), undefined);
  assert.equal(parseRetryAfter(null), undefined);
});
