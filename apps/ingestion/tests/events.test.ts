import assert from 'node:assert/strict';
import { test } from 'node:test';
import { EVENT_DETAIL_TYPE, EVENT_SOURCE, publishRunFinished, type EventPublisher } from '../src/lib/events.js';

const detail = {
  status: 'succeeded' as const,
  runId: '2026-06-23T11-49-42-924Z',
  mode: 'sync',
  bucket: 'bkt',
  prefixes: ['icp-swap-elt/rest/incremental/2026-06-23T11-49-42-924Z/'],
};

function fakePublisher(responses: Array<{ FailedEntryCount?: number; Entries?: { ErrorMessage?: string }[] }>) {
  const sent: any[] = [];
  const client = {
    send: async (cmd: any) => {
      sent.push(cmd.input);
      return responses.shift() ?? {};
    },
  } as unknown as EventPublisher;
  return { sent, client };
}

test('publishRunFinished sends one entry with the documented source, detail-type and prefixes', async () => {
  const { sent, client } = fakePublisher([{ FailedEntryCount: 0 }]);
  await publishRunFinished(client, 'default', detail, () => {});

  assert.equal(sent.length, 1);
  const entry = sent[0].Entries[0];
  assert.equal(entry.EventBusName, 'default');
  assert.equal(entry.Source, EVENT_SOURCE);
  assert.equal(entry.DetailType, EVENT_DETAIL_TYPE);
  assert.deepEqual(JSON.parse(entry.Detail), detail);
});

test('publishRunFinished fails when EventBridge rejects the entry', async () => {
  const rejected = { FailedEntryCount: 1, Entries: [{ ErrorMessage: 'bus not found' }] };
  const { client } = fakePublisher([rejected]);
  await assert.rejects(() => publishRunFinished(client, 'nope', detail, () => {}, 1), /bus not found/);
});
