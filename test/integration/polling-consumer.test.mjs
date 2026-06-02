import { afterEach, describe, expect, it } from 'vitest';

import { collectRecords, createClient, distinctIds, uniqueStreamName } from './helpers/client.mjs';

describe('polling consumer (GetRecords)', () => {
  let client;

  afterEach(() => {
    if (client) client.stopConsumer();
    client = undefined;
  });

  it('consumes every record written to the stream', async () => {
    const streamName = uniqueStreamName('polling');
    const total = 25;

    client = createClient({ streamName, useEnhancedFanOut: false });
    await client.startConsumer();

    const sent = Array.from({ length: total }, (_, id) => ({
      data: { id },
      partitionKey: `key-${id}`
    }));
    await client.putRecords({ records: sent });

    const received = await collectRecords(client, (records) => distinctIds(records) >= total);
    const ids = [...new Set(received.map((record) => record.data.id))].sort((a, b) => a - b);

    expect(ids).toEqual(sent.map((_, id) => id));
  });
});
