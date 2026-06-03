import { createServer, request as httpRequest } from 'node:http';
import { createRequire } from 'node:module';
import { setTimeout as delay } from 'node:timers/promises';
import { EventStreamCodec } from '@smithy/eventstream-codec';
import { fromUtf8, toUtf8 } from '@smithy/util-utf8';

const require = createRequire(import.meta.url);
const { Kinesis } = require('aws-sdk');

const SUBSCRIBE_TARGET = 'Kinesis_20131202.SubscribeToShard';
const EVENT_STREAM_CONTENT_TYPE = 'application/vnd.amazon.eventstream';
const POLL_INTERVAL_MS = 400;

// LocalStack's community kinesis-mock answers RegisterStreamConsumer and marks
// the consumer ACTIVE, but its SubscribeToShard never delivers records (see
// localstack/localstack#10864). This proxy sits in front of LocalStack: it
// forwards every Kinesis call through untouched, except SubscribeToShard, which
// it fulfils itself by reading the freshly produced records via GetRecords and
// re-framing them as real application/vnd.amazon.eventstream SubscribeToShardEvents.
// That lets the library's actual fan-out pipeline (SigV4 signing, got streaming,
// lifion-aws-event-stream parsing, checkpointing) run end to end.
export async function startFanOutProxy({
  localstackEndpoint = 'http://localhost:4566',
  region = 'us-east-1'
} = {}) {
  const codec = new EventStreamCodec(toUtf8, fromUtf8);
  const upstream = new URL(localstackEndpoint);
  const kinesis = new Kinesis({
    accessKeyId: 'test',
    endpoint: localstackEndpoint,
    region,
    secretAccessKey: 'test'
  });

  const debug =
    process.env.PROXY_DEBUG === 'true' ? (...a) => console.error('[proxy]', ...a) : () => {};

  const server = createServer((clientReq, clientRes) => {
    const chunks = [];
    clientReq.on('data', (chunk) => chunks.push(chunk));
    clientReq.on('end', () => {
      const body = Buffer.concat(chunks);
      const target = clientReq.headers['x-amz-target'];
      debug(clientReq.method, clientReq.url, target || '(no target)');
      if (target === SUBSCRIBE_TARGET) {
        streamSubscription({ body, clientRes, codec, debug, kinesis }).catch((err) => {
          debug('subscribe error:', err && err.message);
          if (!clientRes.headersSent) clientRes.writeHead(500);
          clientRes.end();
        });
      } else {
        forward({ body, clientReq, clientRes, upstream });
      }
    });
  });

  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address();
  return {
    close: () => new Promise((resolve) => server.close(resolve)),
    url: `http://127.0.0.1:${port}`
  };
}

function forward({ body, clientReq, clientRes, upstream }) {
  const proxyReq = httpRequest(
    {
      headers: { ...clientReq.headers, host: upstream.host },
      hostname: upstream.hostname,
      method: clientReq.method,
      path: clientReq.url,
      port: upstream.port
    },
    (proxyRes) => {
      clientRes.writeHead(proxyRes.statusCode, proxyRes.headers);
      proxyRes.pipe(clientRes);
    }
  );
  proxyReq.on('error', () => {
    if (!clientRes.headersSent) clientRes.writeHead(502);
    clientRes.end();
  });
  proxyReq.end(body);
}

async function streamSubscription({ body, clientRes, codec, debug = () => {}, kinesis }) {
  const { ConsumerARN, ShardId, StartingPosition = {} } = JSON.parse(body.toString('utf8'));
  // The stream name is embedded in the consumer ARN: .../stream/<name>/consumer/<n>:<ts>
  const streamName = ConsumerARN.split(':stream/')[1].split('/consumer/')[0];
  debug('subscribe', { ShardId, StartingPosition, streamName });

  const iteratorType =
    StartingPosition.Type === 'AFTER_SEQUENCE_NUMBER'
      ? 'AFTER_SEQUENCE_NUMBER'
      : StartingPosition.Type === 'LATEST'
        ? 'LATEST'
        : 'TRIM_HORIZON';

  const { ShardIterator } = await kinesis
    .getShardIterator({
      ShardId,
      ShardIteratorType: iteratorType,
      StreamName: streamName,
      ...(iteratorType === 'AFTER_SEQUENCE_NUMBER' && {
        StartingSequenceNumber: StartingPosition.SequenceNumber
      })
    })
    .promise();

  clientRes.writeHead(200, { 'content-type': EVENT_STREAM_CONTENT_TYPE });

  let open = true;
  clientRes.on('close', () => {
    open = false;
  });

  let iterator = ShardIterator;
  while (open && iterator) {
    const { NextShardIterator, Records } = await kinesis
      .getRecords({ Limit: 1000, ShardIterator: iterator })
      .promise();

    if (Records.length > 0) {
      debug('got records:', Records.length);
      clientRes.write(Buffer.from(codec.encode(subscribeEvent(Records))));
    }
    iterator = NextShardIterator;
    await delay(POLL_INTERVAL_MS);
  }
  clientRes.end();
}

function subscribeEvent(records) {
  const payload = {
    ContinuationSequenceNumber: records[records.length - 1].SequenceNumber,
    MillisBehindLatest: 0,
    Records: records.map((record) => ({
      ApproximateArrivalTimestamp: record.ApproximateArrivalTimestamp
        ? Math.floor(new Date(record.ApproximateArrivalTimestamp).getTime() / 1000)
        : 0,
      Data: Buffer.from(record.Data).toString('base64'),
      PartitionKey: record.PartitionKey,
      SequenceNumber: record.SequenceNumber
    }))
  };
  return {
    body: fromUtf8(JSON.stringify(payload)),
    headers: {
      ':content-type': { type: 'string', value: 'application/json' },
      ':event-type': { type: 'string', value: 'SubscribeToShardEvent' },
      ':message-type': { type: 'string', value: 'event' }
    }
  };
}
