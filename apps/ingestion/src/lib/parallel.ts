// Runs `tasks` with at most `limit` in flight. On the first failure no further task is
// dispatched; the call then waits for every in-flight task to settle and only then rejects
// with that first error. Callers can therefore announce/release safely afterwards: nothing
// is still writing. Later errors are reported via `onError`, never as unhandled rejections.
export async function parallelLimit<T>(
  tasks: (() => Promise<T>)[],
  limit: number,
  onError?: (err: unknown, index: number) => void,
): Promise<T[]> {
  const results: T[] = new Array(tasks.length);
  let nextIdx = 0;
  let failed = false;
  let firstError: unknown;

  async function worker(): Promise<void> {
    while (!failed && nextIdx < tasks.length) {
      const idx = nextIdx++;
      try {
        results[idx] = await tasks[idx]();
      } catch (err) {
        onError?.(err, idx);
        if (!failed) {
          failed = true;
          firstError = err;
        }
        return;
      }
    }
  }

  const workers = Array.from({ length: Math.min(limit, tasks.length) }, () => worker());
  await Promise.all(workers); // workers never reject
  if (failed) throw firstError;
  return results;
}
