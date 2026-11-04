export type WarnFn = (msg: string) => void;

export type RetryOptions = {
  baseDelayMs?: number;
  maxDelayMs?: number;
  // Upper bound for a server-supplied Retry-After.
  maxRetryAfterMs?: number;
  sleep?: (ms: number) => Promise<void>;
  random?: () => number;
};

export const DEFAULT_RETRY_ATTEMPTS = 5;
export const DEFAULT_RETRY_BASE_DELAY_MS = 1000;
export const DEFAULT_RETRY_MAX_DELAY_MS = 16000;
export const DEFAULT_MAX_RETRY_AFTER_MS = 60000;

// An error that carries the HTTP status (and optional Retry-After) of a failed response.
export class HttpError extends Error {
  constructor(message: string, readonly status: number, readonly retryAfterMs?: number) {
    super(message);
    this.name = 'HttpError';
  }
}

// S3 conditional-write outcomes are decisions, not transient faults.
const NON_RETRYABLE_NAMES = new Set([
  'PreconditionFailed', 'ConditionalRequestConflict', 'NoSuchKey', 'NotFound', 'AccessDenied',
]);

function statusOf(err: unknown): number | undefined {
  const e = err as { status?: unknown; $metadata?: { httpStatusCode?: unknown } } | undefined;
  if (typeof e?.status === 'number') return e.status;
  if (typeof e?.$metadata?.httpStatusCode === 'number') return e.$metadata.httpStatusCode;
  return undefined;
}

// 4xx responses (except 429 Too Many Requests) will not succeed on a retry.
export function isRetryable(err: unknown): boolean {
  if (NON_RETRYABLE_NAMES.has((err as { name?: string } | undefined)?.name ?? '')) return false;
  const status = statusOf(err);
  if (status === undefined) return true;
  if (status === 429) return true;
  return !(status >= 400 && status < 500);
}

// Retry-After is either delta-seconds or an HTTP date.
export function parseRetryAfter(value: string | null | undefined, now: number = Date.now()): number | undefined {
  if (!value) return undefined;
  const trimmed = value.trim();
  if (/^\d+$/.test(trimmed)) return Number(trimmed) * 1000;
  const at = Date.parse(trimmed);
  return Number.isFinite(at) ? Math.max(0, at - now) : undefined;
}

function envNumber(name: string, fallback: number, min: number): number {
  const raw = process.env[name];
  if (raw === undefined || raw === '') return fallback;
  const n = Number(raw);
  return Number.isFinite(n) && n >= min ? n : fallback;
}

// `retries` is the total number of attempts. Defaults come from RETRY_ATTEMPTS /
// RETRY_BASE_DELAY_MS / RETRY_MAX_DELAY_MS, read per call, falling back to 5 / 1s / 16s.
export async function withRetry<T>(
  fn: () => Promise<T>,
  label: string,
  warn: WarnFn = console.warn,
  retries?: number,
  options: RetryOptions = {}
): Promise<T> {
  const attempts = Math.max(1, Math.floor(retries ?? envNumber('RETRY_ATTEMPTS', DEFAULT_RETRY_ATTEMPTS, 1)));
  const base = options.baseDelayMs ?? envNumber('RETRY_BASE_DELAY_MS', DEFAULT_RETRY_BASE_DELAY_MS, 0);
  const maxDelay = options.maxDelayMs ?? envNumber('RETRY_MAX_DELAY_MS', DEFAULT_RETRY_MAX_DELAY_MS, 0);
  const maxRetryAfter = options.maxRetryAfterMs ?? DEFAULT_MAX_RETRY_AFTER_MS;
  const sleep = options.sleep ?? ((ms: number) => new Promise<void>((r) => setTimeout(r, ms)));
  const random = options.random ?? Math.random;

  let lastErr: unknown;
  for (let i = 0; i < attempts; i++) {
    try {
      return await fn();
    } catch (err) {
      lastErr = err;
      if (!isRetryable(err) || i === attempts - 1) break;
      const cap = Math.min(base * 2 ** i, maxDelay);
      let delay = Math.round(cap / 2 + random() * (cap / 2));
      const retryAfter = (err as { retryAfterMs?: unknown })?.retryAfterMs;
      if (typeof retryAfter === 'number' && retryAfter > delay) delay = Math.min(retryAfter, maxRetryAfter);
      warn(`[retry ${i + 1}/${attempts}] ${label}: ${String((err as Error)?.message || err)}; waiting ${delay}ms`);
      await sleep(delay);
    }
  }
  throw lastErr;
}
