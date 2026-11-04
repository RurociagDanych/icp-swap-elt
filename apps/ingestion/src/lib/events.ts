import { PutEventsCommand, type EventBridgeClient } from '@aws-sdk/client-eventbridge';
import { withRetry, type WarnFn } from './retry.js';

// The contract between the two stages. The transformation stack subscribes to exactly this
// source / detail-type (see infra/transformation/compute); keep both sides in sync.
export const EVENT_SOURCE = 'icp-swap-elt.ingestion';
export const EVENT_DETAIL_TYPE = 'IngestionRunFinished';

export type RunFinishedDetail = {
  status: 'succeeded' | 'failed';
  runId: string;
  mode: string;
  bucket: string;
  // Full S3 key prefixes ("<prefix>/<segment>/<runId>/") that this run wrote files under.
  prefixes: string[];
};

export type EventPublisher = Pick<EventBridgeClient, 'send'>;

export async function publishRunFinished(
  client: EventPublisher,
  eventBusName: string,
  detail: RunFinishedDetail,
  warn: WarnFn = console.warn,
  retries = 5
): Promise<void> {
  const command = new PutEventsCommand({
    Entries: [
      {
        EventBusName: eventBusName,
        Source: EVENT_SOURCE,
        DetailType: EVENT_DETAIL_TYPE,
        Detail: JSON.stringify(detail),
      },
    ],
  });
  await withRetry(
    async () => {
      const res = await client.send(command);
      if (res.FailedEntryCount) {
        const reason = res.Entries?.find((e) => e.ErrorMessage)?.ErrorMessage ?? 'unknown error';
        throw new Error(`PutEvents rejected the entry: ${reason}`);
      }
    },
    'publishRunFinished',
    warn,
    retries
  );
}
