import assert from 'node:assert/strict';
import { test } from 'node:test';
import type { Config } from '../src/lib/config.js';
import { announceRun } from '../src/lib/announce.js';

process.env.RETRY_BASE_DELAY_MS = '1';

type Logged = { level: string; msg: string };
function makeLogger() {
  const lines: Logged[] = [];
  const logger = {
    info: (msg: string) => lines.push({ level: 'info', msg }),
    warn: (msg: string) => lines.push({ level: 'warn', msg }),
    error: (msg: string) => lines.push({ level: 'error', msg }),
  };
  return { logger, lines };
}
const cfg = (o: Partial<Config> = {}): Config => ({
  runId: 'r1', mode: 'sync', s3Bucket: 'bkt', eventBus: 'bus', ...o,
}) as Config;
const target = (prefixes: string[]) => ({ writtenPrefixes: () => prefixes }) as any;

test('announceRun publishes status, runId, mode, bucket and the written prefixes', async () => {
  const sent: any[] = [];
  const client = { send: async (cmd: any) => { sent.push(cmd.input); return {}; } } as any;
  const { logger } = makeLogger();
  await announceRun(cfg(), target(['p/rest/incremental/r1/']), logger, 'failed', () => {}, { client });
  const entry = sent[0].Entries[0];
  assert.equal(entry.EventBusName, 'bus');
  assert.deepEqual(JSON.parse(entry.Detail), {
    status: 'failed', runId: 'r1', mode: 'sync', bucket: 'bkt', prefixes: ['p/rest/incremental/r1/'],
  });
});

test('announceRun does nothing for local runs or when no prefix was written', async () => {
  const client = { send: async () => { throw new Error('must not be called'); } } as any;
  const { logger, lines } = makeLogger();
  await announceRun(cfg({ s3Bucket: undefined }), target(['x/']), logger, 'succeeded', () => {}, { client });
  await announceRun(cfg(), target([]), logger, 'succeeded', () => {}, { client });
  assert.ok(lines.some((l) => /no files written/.test(l.msg)));
});

test('M8: an S3 run without an event bus logs a WARNING (not info) that nothing was announced', async () => {
  const { logger, lines } = makeLogger();
  await announceRun(cfg({ eventBus: undefined }), target(['x/']), logger, 'succeeded', () => {});
  assert.ok(lines.some((l) => l.level === 'warn' && /not announc/i.test(l.msg)));
});

test('H3: announcement retries harder than the default 5 attempts', async () => {
  let calls = 0;
  const client = { send: async () => { calls++; if (calls <= 6) throw new Error('throttled'); return {}; } } as any;
  const { logger } = makeLogger();
  await announceRun(cfg(), target(['x/']), logger, 'succeeded', () => {}, { client });
  assert.equal(calls, 7);
});

test('announceRun surfaces a persistent failure', async () => {
  const client = { send: async () => { throw new Error('bus gone'); } } as any;
  const { logger } = makeLogger();
  await assert.rejects(() => announceRun(cfg(), target(['x/']), logger, 'succeeded', () => {}, { client, retries: 2 }), /bus gone/);
});
