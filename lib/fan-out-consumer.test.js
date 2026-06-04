import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest';
import got from 'got';
import mockStream from 'node:stream';

import FanOutConsumer from './fan-out-consumer';
import * as records from './records';
import * as stats from './stats';
import deaggregate from './deaggregate';

vi.mock('./deaggregate');

vi.mock('util', async () => {
  const { promisify, ...otherUtils } = await vi.importActual('util');
  return {
    ...otherUtils,
    promisify: (...args) => {
      const [func] = args;
      if (func.name === 'setTimeout') {
        return () => new Promise((resolve) => setImmediate(resolve));
      }
      return promisify(...args);
    }
  };
});

vi.mock('node:util', async () => {
  const { promisify, ...otherUtils } = await vi.importActual('node:util');
  return {
    ...otherUtils,
    promisify: (...args) => {
      const [func] = args;
      if (func.name === 'setTimeout') {
        return () => new Promise((resolve) => setImmediate(resolve));
      }
      return promisify(...args);
    }
  };
});

vi.mock('./records', () => {
  const RecordsDecoder = vi.fn(function () {
    return new mockStream.Transform({
      objectMode: true,
      transform(chunk, encoding, callback) {
        const transformedRecords = Object.keys(chunk).reduce(
          (agg, key) => ({ ...agg, [key[0].toLowerCase() + key.slice(1)]: chunk[key] }),
          {}
        );
        this.push(transformedRecords);
        callback();
      }
    });
  });
  return { RecordsDecoder };
});

vi.mock('./stream', () => ({
  getStreamShards: () => ({})
}));

vi.mock('./stats');

vi.useFakeTimers({ toFake: ['clearInterval', 'clearTimeout', 'setInterval', 'setTimeout'] });
vi.spyOn(globalThis, 'setTimeout');
vi.spyOn(globalThis, 'clearTimeout');

function nextTickWait() {
  return new Promise((resolve) => setImmediate(resolve));
}

describe('lib/fan-out-consumer', () => {
  const debug = vi.fn();
  const error = vi.fn();
  const warn = vi.fn();
  const logger = { debug, error, warn };

  const getShardsData = vi.fn(() =>
    Promise.resolve({ shardsPath: '#a', shardsPathNames: { '#a': 'a' } })
  );
  const markShardAsDepleted = vi.fn();
  const storeShardCheckpoint = vi.fn();
  const stateStore = { getShardsData, markShardAsDepleted, storeShardCheckpoint };

  const pushToStream = vi.fn();
  const stopConsumer = vi.fn();
  const credentials = {
    accessKeyId: 'resolved-access-key-id',
    secretAccessKey: 'resolved-secret-access-key',
    sessionToken: 'resolved-session-token'
  };
  const options = {
    awsOptions: { region: 'us-east-1' },
    checkpoint: null,
    client: { getCredentials: () => Promise.resolve(credentials) },
    compression: 'LZ-UTF8',
    consumerArn: 'arn:enhanced-consumer',
    initialPositionInStream: 'LATEST',
    leaseExpiration: new Date(Date.now() + 5 * 60 * 1000).toISOString(),
    logger,
    pushToStream,
    shardId: 'shard-0001',
    stateStore,
    stopConsumer,
    streamName: 'test-stream'
  };

  beforeEach(() => {
    deaggregate.mockImplementation(async (x) => x);
  });

  afterEach(() => {
    deaggregate.mockClear();
    debug.mockClear();
    error.mockClear();
    getShardsData.mockClear();
    got.mockClear();
    markShardAsDepleted.mockClear();
    pushToStream.mockClear();
    stopConsumer.mockClear();
    storeShardCheckpoint.mockClear();
    warn.mockClear();
    stats.reportError.mockClear();
    stats.reportResponse.mockClear();
    records.RecordsDecoder.mockClear();
    vi.clearAllTimers();
    vi.useFakeTimers({ toFake: ['clearInterval', 'clearTimeout', 'setInterval', 'setTimeout'] });
    vi.spyOn(globalThis, 'setTimeout');
    vi.spyOn(globalThis, 'clearTimeout');
  });

  test('the module exports the expected', () => {
    expect(FanOutConsumer).toEqual(expect.any(Function));
    expect(FanOutConsumer).toThrow('Class constructor');
  });

  test('the constructor adds a hook to sign requests with resolved credentials', async () => {
    const consumer = new FanOutConsumer(options);
    expect(consumer).toBeInstanceOf(FanOutConsumer);
    const { extend } = got.getMocks();
    expect(extend).toHaveBeenCalledWith({
      headers: { 'Content-Type': 'application/x-amz-json-1.1' },
      hooks: { beforeRequest: [expect.any(Function)] },
      method: 'POST',
      prefixUrl: 'https://kinesis.us-east-1.amazonaws.com',
      throwHttpErrors: false
    });
    const { beforeRequest } = got.getHooks();
    const [signRequest] = beforeRequest;
    expect(signRequest).toBeInstanceOf(Function);
    const requestOptions = {
      headers: {},
      method: 'POST',
      url: 'https://kinesis.us-east-1.amazonaws.com/'
    };
    await signRequest(requestOptions);
    expect(requestOptions.headers).toEqual({
      Authorization: expect.stringMatching(/^AWS4-HMAC-SHA256 Credential=resolved-access-key-id/),
      Host: 'kinesis.us-east-1.amazonaws.com',
      'X-Amz-Date': expect.stringMatching(/^\d{8}T\d{6}Z$/),
      'X-Amz-Security-Token': 'resolved-session-token'
    });
  });

  test('the signing hook honors a custom endpoint instead of forcing real AWS', async () => {
    const consumer = new FanOutConsumer({
      awsOptions: { endpoint: 'http://localhost:4566', region: 'us-east-1' },
      client: {
        getCredentials: () => Promise.resolve({ accessKeyId: 'foo', secretAccessKey: 'bar' })
      }
    });
    expect(consumer).toBeInstanceOf(FanOutConsumer);
    const { extend } = got.getMocks();
    expect(extend).toHaveBeenCalledWith(
      expect.objectContaining({ prefixUrl: 'http://localhost:4566' })
    );
    const { beforeRequest } = got.getHooks();
    const [signRequest] = beforeRequest;
    const requestOptions = { headers: {}, method: 'POST', url: 'http://localhost:4566/' };
    await signRequest(requestOptions);
    expect(requestOptions.headers).toEqual({
      Authorization: expect.stringMatching(/^AWS4-HMAC-SHA256 Credential=foo/),
      Host: 'localhost:4566',
      'X-Amz-Date': expect.stringMatching(/^\d{8}T\d{6}Z$/)
    });
  });

  test('the signing hook defaults to the real Kinesis endpoint with no region or endpoint', () => {
    const consumer = new FanOutConsumer({
      awsOptions: {},
      client: { getCredentials: () => Promise.resolve(credentials) }
    });
    expect(consumer).toBeInstanceOf(FanOutConsumer);
    const { extend } = got.getMocks();
    expect(extend).toHaveBeenCalledWith(
      expect.objectContaining({ prefixUrl: 'https://kinesis.us-east-1.amazonaws.com' })
    );
  });

  test('starting the consumer creates a streaming pipeline that pushes records', async () => {
    const consumer = new FanOutConsumer(options);
    const start = consumer.start();
    await nextTickWait();
    const { response, stream } = got.getMocks();
    response.emit('response', {
      headers: { 'content-type': 'application/vnd.amazon.eventstream' },
      statusCode: 200
    });
    response.push({
      continuationSequenceNumber: '2',
      millisBehindLatest: 0,
      records: [{ foo: 'bar' }]
    });

    await nextTickWait();
    response.emit('error', Object.assign(new Error('foo'), { code: 'ValidationException' }));
    await start;

    expect(stream).toHaveBeenCalledWith({
      body: JSON.stringify({
        ConsumerARN: 'arn:enhanced-consumer',
        ShardId: 'shard-0001',
        StartingPosition: { Type: 'LATEST' }
      }),
      headers: { 'X-Amz-Target': 'Kinesis_20131202.SubscribeToShard' }
    });
    expect(records.RecordsDecoder).toHaveBeenCalledWith({ compression: 'LZ-UTF8', logger });

    expect(clearTimeout).toHaveBeenCalledTimes(3);
    expect(clearTimeout).toHaveBeenNthCalledWith(1, null);
    expect(clearTimeout).toHaveBeenNthCalledWith(2, null);
    expect(clearTimeout).toHaveBeenNthCalledWith(3, expect.any(Object));

    expect(setTimeout).toHaveBeenCalledTimes(2);
    expect(setTimeout).toHaveBeenNthCalledWith(1, expect.any(Function), expect.any(Number));
    expect(setTimeout).toHaveBeenNthCalledWith(2, expect.any(Function), 10000);

    expect(storeShardCheckpoint).toHaveBeenCalledWith('shard-0001', '2', '#a', { '#a': 'a' });

    expect(debug).toHaveBeenCalledTimes(3);
    expect(debug).toHaveBeenNthCalledWith(
      1,
      'Starting an enhanced fan-out subscriber for shard "shard-0001"…'
    );
    expect(debug).toHaveBeenNthCalledWith(2, 'Subscription to shard is successful.');
    expect(debug).toHaveBeenNthCalledWith(3, 'Got 1 record(s) from "shard-0001" (0ms behind)');

    expect(pushToStream).toHaveBeenNthCalledWith(1, null, {
      continuationSequenceNumber: '2',
      millisBehindLatest: 0,
      records: [{ foo: 'bar' }],
      shardId: 'shard-0001'
    });
    expect(pushToStream).toHaveBeenCalledTimes(2);

    expect(stats.reportResponse).toHaveBeenCalledWith('kinesis', 'test-stream');
    expect(stats.reportError).not.toHaveBeenCalled();
  });

  test('passing a TRIM_HORIZON initialPositionInStream starts a TRIM_HORIZON stream', async () => {
    const trimOptions = { ...options, initialPositionInStream: 'TRIM_HORIZON' };
    const consumer = new FanOutConsumer(trimOptions);
    const start = consumer.start();
    await nextTickWait();
    const { response, stream } = got.getMocks();
    response.emit('response', {
      headers: { 'content-type': 'application/vnd.amazon.eventstream' },
      statusCode: 200
    });
    response.push({
      continuationSequenceNumber: '2',
      millisBehindLatest: 0,
      records: [{ foo: 'bar' }]
    });

    await nextTickWait();
    response.emit('error', Object.assign(new Error('foo'), { code: 'ValidationException' }));
    await start;

    expect(stream).toHaveBeenCalledWith({
      body: JSON.stringify({
        ConsumerARN: 'arn:enhanced-consumer',
        ShardId: 'shard-0001',
        StartingPosition: { Type: 'TRIM_HORIZON' }
      }),
      headers: { 'X-Amz-Target': 'Kinesis_20131202.SubscribeToShard' }
    });
  });

  test('the shard checkpoint is used as the starting point if available', async () => {
    const consumer = new FanOutConsumer({ ...options, checkpoint: '1' });
    const start = consumer.start();
    await nextTickWait();
    const { response, stream } = got.getMocks();
    response.emit('response', {
      headers: { 'content-type': 'application/vnd.amazon.eventstream' },
      statusCode: 200
    });
    response.push({
      continuationSequenceNumber: '2',
      millisBehindLatest: 0,
      records: [{ foo: 'bar' }]
    });
    await nextTickWait();
    response.emit('error', Object.assign(new Error('foo'), { code: 'ValidationException' }));
    await start;
    expect(stream).toHaveBeenCalledWith({
      body: JSON.stringify({
        ConsumerARN: 'arn:enhanced-consumer',
        ShardId: 'shard-0001',
        StartingPosition: { SequenceNumber: '1', Type: 'AFTER_SEQUENCE_NUMBER' }
      }),
      headers: { 'X-Amz-Target': 'Kinesis_20131202.SubscribeToShard' }
    });
  });

  test('non event stream responses are reported as errors', async () => {
    const consumer = new FanOutConsumer(options);
    const start = consumer.start();
    await nextTickWait();
    const { response } = got.getMocks();
    response.emit('response', { headers: { 'content-type': 'application/json' }, statusCode: 500 });
    response.push(JSON.stringify({ __type: 'UnknownOperationException' }));
    await start;

    expect(clearTimeout).toHaveBeenCalledTimes(2);
    expect(clearTimeout).toHaveBeenNthCalledWith(1, null);
    expect(clearTimeout).toHaveBeenNthCalledWith(2, null);

    expect(setTimeout).toHaveBeenCalledTimes(1);
    expect(setTimeout).toHaveBeenNthCalledWith(1, expect.any(Function), expect.any(Number));

    expect(storeShardCheckpoint).not.toHaveBeenCalled();
    expect(debug).toHaveBeenCalledTimes(1);
    expect(error).toHaveBeenNthCalledWith(
      1,
      'Pipeline closed with error: [UnknownOperationException] Failed to subscribe to shard.'
    );
    expect(error).toHaveBeenCalledTimes(1);
    expect(pushToStream).toHaveBeenNthCalledWith(
      1,
      expect.objectContaining({
        code: 'UnknownOperationException',
        isRetryable: true,
        message: 'Failed to subscribe to shard.'
      })
    );
    expect(pushToStream).toHaveBeenCalledTimes(1);
    expect(stats.reportResponse).not.toHaveBeenCalled();
    expect(stats.reportError).toHaveBeenCalledWith('kinesis', { statusCode: 500 }, 'test-stream');
  });

  test("the consumer is stopped when the shards state can't be resolved on start", async () => {
    const consumer = new FanOutConsumer(options);
    getShardsData.mockImplementationOnce(() => {
      throw new Error('foo');
    });
    await consumer.start();
    expect(warn).toHaveBeenCalledTimes(1);
    expect(warn).toHaveBeenNthCalledWith(
      1,
      "Can't start the consumer as the state can't be resolved:",
      expect.objectContaining({ message: 'foo' })
    );
    expect(stopConsumer).toHaveBeenCalledWith('shard-0001');
  });

  test('an empty array of records in the stream is not pushed outside the pipeline', async () => {
    const consumer = new FanOutConsumer({ ...options, shouldDeaggregate: true });
    const start = consumer.start();
    await nextTickWait();
    const { response } = got.getMocks();
    response.emit('response', {
      headers: { 'content-type': 'application/vnd.amazon.eventstream' },
      statusCode: 200
    });
    response.push({
      continuationSequenceNumber: '2',
      millisBehindLatest: 0,
      records: []
    });
    await nextTickWait();
    response.emit('error', Object.assign(new Error('foo'), { code: 'ValidationException' }));
    await start;
    expect(storeShardCheckpoint).toHaveBeenCalledWith('shard-0001', '2', '#a', { '#a': 'a' });
    expect(debug).toHaveBeenNthCalledWith(2, 'Subscription to shard is successful.');
    expect(debug).toHaveBeenCalledTimes(2);
    expect(pushToStream).toHaveBeenCalledTimes(1);
    expect(stats.reportResponse).toHaveBeenCalledWith('kinesis', 'test-stream');
    expect(stats.reportError).not.toHaveBeenCalled();
  });

  test("a shard is marked as depleted if there's no continuation sequence number", async () => {
    const consumer = new FanOutConsumer(options);
    const start = consumer.start();
    await nextTickWait();
    const { response } = got.getMocks();
    response.emit('response', {
      headers: { 'content-type': 'application/vnd.amazon.eventstream' },
      statusCode: 200
    });
    response.push({ millisBehindLatest: 0, records: [] });
    await nextTickWait();
    response.emit('error', Object.assign(new Error('foo'), { code: 'ValidationException' }));
    await start;
    expect(markShardAsDepleted).toHaveBeenCalledWith({}, 'shard-0001');
    expect(storeShardCheckpoint).not.toHaveBeenCalled();
    expect(debug).toHaveBeenNthCalledWith(3, 'The parent shard "shard-0001" has been depleted.');
    expect(debug).toHaveBeenCalledTimes(3);
  });

  test('a stream should be recreated on a timeout to received event data', async () => {
    setTimeout.mockImplementationOnce(() => {});
    setTimeout.mockImplementationOnce(() => {});
    setTimeout.mockImplementationOnce(() => {});
    setTimeout.mockImplementationOnce((func, delay, ...args) => {
      expect(delay).toBe(10000);
      func(...args);
    });
    const consumer = new FanOutConsumer(options);
    const start = consumer.start();
    await nextTickWait();
    const { abort, response } = got.getMocks();
    response.emit('response', {
      headers: { 'content-type': 'application/vnd.amazon.eventstream' },
      statusCode: 200
    });
    response.push({
      millisBehindLatest: 0,
      records: []
    });
    await nextTickWait();
    response.emit('error', Object.assign(new Error('foo'), { code: 'ValidationException' }));
    await start;
    expect(abort).toHaveBeenCalled();
  });

  test('a consumer can be stopped twice', async () => {
    const consumer = new FanOutConsumer(options);
    consumer.start();
    await nextTickWait();
    consumer.stop();
    await nextTickWait();
    consumer.stop();
    expect(setTimeout).toHaveBeenCalledTimes(1);
    expect(clearTimeout).toHaveBeenCalledTimes(4);
  });

  test('a consumer can be stopped without starting it', async () => {
    const consumer = new FanOutConsumer(options);
    consumer.stop();
    expect(setTimeout).toHaveBeenCalledTimes(0);
    expect(clearTimeout).toHaveBeenCalledTimes(1);
  });

  test("updating the lease to an expired timestamp doesn't schedule timeouts", async () => {
    const consumer = new FanOutConsumer(options);
    consumer.updateLeaseExpiration(0);
    expect(setTimeout).toHaveBeenCalledTimes(0);
    expect(clearTimeout).toHaveBeenCalledTimes(1);
  });

  test('updating the lease to a future timestamp schedules a timeout', async () => {
    const consumer = new FanOutConsumer(options);
    consumer.updateLeaseExpiration(new Date(Date.now() + 5 * 60 * 1000));
    expect(setTimeout).toHaveBeenCalledTimes(1);
    expect(clearTimeout).toHaveBeenCalledTimes(1);
  });

  test('the consumer should be stopped once the lease expires', async () => {
    setTimeout.mockImplementationOnce((func, delay, ...args) => func(...args));
    const consumer = new FanOutConsumer(options);
    consumer.updateLeaseExpiration(new Date(Date.now() + 5 * 60 * 1000));
    expect(stopConsumer).toHaveBeenCalledWith('shard-0001');
    expect(debug).toHaveBeenNthCalledWith(1, 'The lease for "shard-0001" has expired.');
    expect(debug).toHaveBeenCalledTimes(1);
  });

  test('the consumer will recreate the pipeline on retryable errors', async () => {
    const consumer = new FanOutConsumer({ ...options, checkpoint: '1' });
    const start = consumer.start();
    await nextTickWait();
    let { response } = got.getMocks();
    response.emit('response', {
      headers: { 'content-type': 'application/json' },
      statusCode: 500
    });
    response.push(
      JSON.stringify({
        __type: 'InternalServerError',
        message: 'Unexpected Server Error'
      })
    );
    await nextTickWait();
    await nextTickWait();
    const gotMocks = got.getMocks();
    ({ response } = gotMocks);
    const { stream } = gotMocks;
    response.emit('error', Object.assign(new Error('foo'), { code: 'UnknownOperationException' }));
    await start;
    expect(stream).toHaveBeenCalledTimes(2);
    expect(warn).toHaveBeenNthCalledWith(1, 'Subscription unsuccessful: 500');
    expect(warn).toHaveBeenNthCalledWith(
      2,
      [
        'Trying to recover from AWS.Kinesis error…',
        '- Message: Unexpected Server Error',
        '- Request ID: undefined',
        '- Code: InternalServerError (500)',
        '- Stream: test-stream'
      ].join('\n\t')
    );
    expect(warn).toHaveBeenNthCalledWith(3, 'Waiting before retrying the pipeline…');
    expect(warn).toHaveBeenCalledTimes(3);
  });

  test('the consumer will recreate the pipeline on ARN is in use errors', async () => {
    const consumer = new FanOutConsumer({ ...options, checkpoint: '1' });
    const start = consumer.start();
    await nextTickWait();
    let { response } = got.getMocks();
    response.emit('response', {
      headers: { 'content-type': 'application/json' },
      statusCode: 500
    });
    response.push(
      JSON.stringify({
        __type: 'ResourceInUseException',
        message: 'Another active subscription exists for consumer "foo"'
      })
    );
    await nextTickWait();
    await nextTickWait();
    const gotMocks = got.getMocks();
    ({ response } = gotMocks);
    const { stream } = gotMocks;
    response.emit('error', Object.assign(new Error('foo'), { code: 'UnknownOperationException' }));
    await start;
    expect(stream).toHaveBeenCalledTimes(2);
    expect(warn).toHaveBeenNthCalledWith(1, 'Subscription unsuccessful: 500');
    expect(warn).toHaveBeenNthCalledWith(
      2,
      [
        'Trying to recover from AWS.Kinesis error…',
        '- Message: Another active subscription exists for consumer "foo"',
        '- Request ID: undefined',
        '- Code: ResourceInUseException (500)',
        '- Stream: test-stream'
      ].join('\n\t')
    );
    expect(warn).toHaveBeenNthCalledWith(3, 'Waiting before retrying the pipeline…');
    expect(warn).toHaveBeenCalledTimes(3);
  });

  test('data is deaggregated properly', async () => {
    const consumer = new FanOutConsumer({ ...options, checkpoint: '1', shouldDeaggregate: true });
    const start = consumer.start();
    await nextTickWait();
    const { response, stream } = got.getMocks();
    response.emit('response', {
      headers: { 'content-type': 'application/vnd.amazon.eventstream' },
      statusCode: 200
    });
    response.push({
      continuationSequenceNumber: '2',
      millisBehindLatest: 0,
      payload: { Records: [{}] },
      records: [{ foo: 'bar' }]
    });
    await nextTickWait();
    response.emit('error', Object.assign(new Error('foo'), { code: 'ValidationException' }));
    await start;
    expect(stream).toHaveBeenCalledWith({
      body: JSON.stringify({
        ConsumerARN: 'arn:enhanced-consumer',
        ShardId: 'shard-0001',
        StartingPosition: { SequenceNumber: '1', Type: 'AFTER_SEQUENCE_NUMBER' }
      }),
      headers: { 'X-Amz-Target': 'Kinesis_20131202.SubscribeToShard' }
    });
  });

  test('errors thrown by the deaggregation method are caught', async () => {
    deaggregate.mockImplementation(() => {
      throw new Error('deaggregate error');
    });

    const consumer = new FanOutConsumer({ ...options, shouldDeaggregate: true });
    const start = consumer.start();
    await nextTickWait();
    const { response } = got.getMocks();

    response.emit('response', {
      headers: { 'content-type': 'application/vnd.amazon.eventstream' },
      statusCode: 200
    });
    response.push({
      continuationSequenceNumber: '2',
      millisBehindLatest: 0,
      payload: { Records: [{ foo: 'bar' }] },
      records: []
    });

    await nextTickWait();
    response.emit('error', Object.assign(new Error('foo'), { code: 'UnknownOperationException' }));
    await start;
    await nextTickWait();
    expect(storeShardCheckpoint).not.toHaveBeenCalled();
    expect(debug).toHaveBeenNthCalledWith(2, 'Subscription to shard is successful.');
    expect(debug).toHaveBeenCalledTimes(2);
    expect(pushToStream).toHaveBeenCalledTimes(1);
    expect(stats.reportResponse).toHaveBeenCalledWith('kinesis', 'test-stream');
    expect(stats.reportError).not.toHaveBeenCalled();
  });

  test('the consumer will recreate the pipeline on a chunk that cannot be parsed as a JSON', async () => {
    const consumer = new FanOutConsumer({ ...options, checkpoint: '1' });
    const start = consumer.start();
    await nextTickWait();
    let { response } = got.getMocks();
    response.emit('response', {
      headers: { 'content-type': 'application/json' },
      statusCode: 500
    });
    response.push('<ServiceUnavailableException/>');
    await nextTickWait();
    await nextTickWait();
    const gotMocks = got.getMocks();
    ({ response } = gotMocks);
    const { stream } = gotMocks;
    response.emit('error', Object.assign(new Error('foo'), { code: 'UnknownOperationException' }));
    await start;
    expect(stream).toHaveBeenCalledTimes(2);
    expect(warn).toHaveBeenNthCalledWith(1, 'Subscription unsuccessful: 500');
    expect(warn).toHaveBeenNthCalledWith(
      2,
      [
        'Trying to recover from AWS.Kinesis error…',
        '- Message: <ServiceUnavailableException/>',
        '- Request ID: undefined',
        '- Code: undefined (500)',
        '- Stream: test-stream'
      ].join('\n\t')
    );
    expect(warn).toHaveBeenNthCalledWith(3, 'Waiting before retrying the pipeline…');
    expect(warn).toHaveBeenCalledTimes(3);
  });
});
