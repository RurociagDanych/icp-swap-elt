import assert from 'node:assert/strict';
import { test } from 'node:test';
import { DEFAULT_REST_TIMEOUT_MS, buildFindUrl, fetchTransactions } from '../src/lib/restClient.js';

// Keep retry backoff negligible for the whole file.
process.env.RETRY_BASE_DELAY_MS = '1';

const base = { baseUrl: 'https://api.icpswap.com/info', page: 1, limit: 100, actionTypes: 'Swap' };

test('buildFindUrl includes required params and omits absent optionals', () => {
  const url = buildFindUrl(base);
  assert.match(url, /\/transaction\/find\?/);
  assert.match(url, /page=1/);
  assert.match(url, /limit=100/);
  assert.match(url, /actionTypes=Swap/);
  assert.doesNotMatch(url, /begin=/);
});

test('buildFindUrl includes begin/end when provided', () => {
  const url = buildFindUrl({ ...base, begin: 10, end: 20 });
  assert.match(url, /begin=10/);
  assert.match(url, /end=20/);
});

test('fetchTransactions returns data on code 200', async () => {
  const data = { totalElements: 1, content: [], page: 1, limit: 100 };
  const fake = async () => ({ ok: true, status: 200, json: async () => ({ code: 200, message: null, data }) });
  const result = await fetchTransactions(base, () => {}, fake);
  assert.deepEqual(result, data);
});

test('fetchTransactions retries then throws on non-200 wrapper code', async () => {
  let calls = 0;
  const fake = async () => { calls++; return { ok: true, status: 200, json: async () => ({ code: 500, message: 'boom', data: null }) }; };
  await assert.rejects(() => fetchTransactions(base, () => {}, fake), /boom/);
  assert.equal(calls, 5); // withRetry default attempts
});

test('fetchTransactions aborts a hung request after the per-attempt timeout', { timeout: 5000 }, async () => {
  let calls = 0;
  const hang = (_url: string, init?: { signal?: AbortSignal }) => {
    calls++;
    return new Promise<never>((_, reject) => {
      init?.signal?.addEventListener('abort', () => reject(init.signal?.reason ?? new Error('aborted')));
    });
  };
  await assert.rejects(() => fetchTransactions(base, () => {}, hang, { timeoutMs: 20, attempts: 2 }), /time|abort/i);
  assert.equal(calls, 2); // a timeout is retryable, one signal per attempt
});

test('default REST timeout is 30 seconds', () => {
  assert.equal(DEFAULT_REST_TIMEOUT_MS, 30_000);
});

test('fetchTransactions does not retry a 404 but retries 429 honoring Retry-After', async () => {
  let calls = 0;
  const notFound = async () => { calls++; return { ok: false, status: 404, json: async () => ({}) }; };
  await assert.rejects(() => fetchTransactions(base, () => {}, notFound), /HTTP 404/);
  assert.equal(calls, 1);

  const sleeps: number[] = [];
  let n = 0;
  const limited = async () => {
    n++;
    if (n === 1) return { ok: false, status: 429, headers: { get: () => '2' }, json: async () => ({}) };
    return { ok: true, status: 200, json: async () => ({ code: 200, data: { totalElements: 0, content: [], page: 1, limit: 100 } }) };
  };
  const data = await fetchTransactions(base, () => {}, limited, { sleep: async (ms) => { sleeps.push(ms); } });
  assert.equal(data.totalElements, 0);
  assert.deepEqual(sleeps, [2000]);
});
