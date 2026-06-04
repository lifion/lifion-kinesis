import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { NodeHttpHandler } from '@smithy/node-http-handler';

import { collectRecords, createClient, distinctIds, uniqueStreamName } from './helpers/client.js';
import { startFanOutProxy } from './helpers/fan-out-proxy.js';

// Enhanced fan-out exercises RegisterStreamConsumer + the raw SubscribeToShard
// HTTP path (SigV4 + got streaming + event-stream parsing). LocalStack community
// can't deliver SubscribeToShard (localstack/localstack#10864), so the client's
// Kinesis endpoint points at a proxy that forwards everything to LocalStack and
// synthesizes only the SubscribeToShard event stream from the real records.
describe('enhanced fan-out consumer (SubscribeToShard)', () => {
  let client;
  let proxy;

  beforeEach(async () => {
    proxy = await startFanOutProxy();
  });

  afterEach(async () => {
    if (client) client.stopConsumer();
    client = undefined;
    if (proxy) await proxy.close();
    proxy = undefined;
  });

  it('consumes every record written to the stream', async () => {
    const streamName = uniqueStreamName('fan-out');
    const total = 25;

    // Kinesis (and the fan-out subscribe) go through the proxy; DynamoDB state
    // talks to LocalStack directly via the helper's default dynamoDb.endpoint.
    client = createClient({
      endpoint: proxy.url,
      maxEnhancedConsumers: 1,
      requestHandler: new NodeHttpHandler(),
      streamName,
      useEnhancedFanOut: true
    });
    await client.startConsumer();

    const sent = Array.from({ length: total }, (_, id) => ({
      data: { id },
      partitionKey: `key-${id}`
    }));
    await client.putRecords({ records: sent });

    const received = await collectRecords(client, (records) => distinctIds(records) >= total);
    const ids = Array.from(new Set(received.map((record) => record.data.id))).sort((a, b) => a - b);

    expect(ids).toEqual(sent.map((_, id) => id));
  });
});
