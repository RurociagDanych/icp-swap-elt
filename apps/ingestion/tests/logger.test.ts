import assert from 'node:assert/strict';
import { test } from 'node:test';
import { createLogger } from '../src/lib/logger.js';

test('logger emits JSON lines with level, message, and base fields', () => {
  const lines: string[] = [];
  const logger = createLogger({ base: { runId: 'r1', mode: 'full' }, write: (l) => lines.push(l) });

  logger.info('hello', { rows: 3 });
  logger.warn('careful');
  logger.error('boom', { reason: 'x' });

  const events = lines.map((l) => JSON.parse(l));
  assert.equal(events.length, 3);
  assert.equal(events[0].level, 'info');
  assert.equal(events[0].msg, 'hello');
  assert.equal(events[0].runId, 'r1');
  assert.equal(events[0].mode, 'full');
  assert.equal(events[0].rows, 3);
  assert.ok(events[0].ts);
  assert.equal(events[1].level, 'warn');
  assert.equal(events[2].level, 'error');
  assert.equal(events[2].reason, 'x');
});

test('logger stringifies bigint fields instead of throwing', () => {
  const lines: string[] = [];
  const logger = createLogger({ write: (l) => lines.push(l) });
  logger.info('big', { total: 123n });
  assert.equal(JSON.parse(lines[0]).total, '123');
});
