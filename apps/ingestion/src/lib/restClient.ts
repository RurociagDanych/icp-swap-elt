import type { RestFindData, RestResponse } from '../idl/restTx.js';
import { HttpError, parseRetryAfter, withRetry, type RetryOptions, type WarnFn } from './retry.js';

export type FindParams = {
  baseUrl: string;
  page: number;
  limit: number;
  actionTypes: string;
  begin?: number;
  end?: number;
  poolId?: string;
  tokenId?: string;
  principal?: string;
};

export type FetchLike = (
  url: string,
  init?: { signal?: AbortSignal }
) => Promise<{
  ok: boolean;
  status: number;
  headers?: { get(name: string): string | null };
  json: () => Promise<unknown>;
}>;

export const DEFAULT_REST_TIMEOUT_MS = 30_000;

export type FetchOptions = RetryOptions & {
  // Per-attempt timeout (env REST_TIMEOUT_MS, default 30s).
  timeoutMs?: number;
  // Total attempts (env RETRY_ATTEMPTS, default 5).
  attempts?: number;
};

function defaultTimeoutMs(): number {
  const n = Number(process.env.REST_TIMEOUT_MS);
  return Number.isFinite(n) && n > 0 ? n : DEFAULT_REST_TIMEOUT_MS;
}

export function buildFindUrl(p: FindParams): string {
  const qs = new URLSearchParams({
    page: String(p.page),
    limit: String(p.limit),
    actionTypes: p.actionTypes,
  });
  if (p.begin !== undefined) qs.set('begin', String(p.begin));
  if (p.end !== undefined) qs.set('end', String(p.end));
  if (p.poolId) qs.set('poolId', p.poolId);
  if (p.tokenId) qs.set('tokenId', p.tokenId);
  if (p.principal) qs.set('principal', p.principal);
  return `${p.baseUrl.replace(/\/$/, '')}/transaction/find?${qs.toString()}`;
}

export async function fetchTransactions(
  p: FindParams,
  warn: WarnFn,
  fetchImpl: FetchLike = (url, init) => fetch(url, init),
  options: FetchOptions = {},
): Promise<RestFindData> {
  const url = buildFindUrl(p);
  const timeoutMs = options.timeoutMs ?? defaultTimeoutMs();
  return withRetry(async () => {
    // A fresh controller per attempt: its signal bounds both the response headers and the body read.
    // A ref'd setTimeout is used instead of AbortSignal.timeout(): that timer is unref'd, so a hung
    // request alone would let the event loop drain (Node 22 test runner cancels the test).
    const controller = new AbortController();
    const timer = setTimeout(() => {
      controller.abort(new Error(`REST request timed out after ${timeoutMs}ms`));
    }, timeoutMs);
    try {
      const res = await fetchImpl(url, { signal: controller.signal });
      if (!res.ok) {
        throw new HttpError(`HTTP ${res.status} for ${url}`, res.status, parseRetryAfter(res.headers?.get('retry-after')));
      }
      const body = (await res.json()) as RestResponse;
      if (body.code !== 200 || !body.data) {
        throw new Error(`API code ${body.code}: ${body.message ?? 'unknown'} for page ${p.page}`);
      }
      return body.data;
    } finally {
      clearTimeout(timer);
    }
  }, `find page=${p.page}`, warn, options.attempts, options);
}
