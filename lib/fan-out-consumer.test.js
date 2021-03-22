'use strict';

const aws = require('aws-sdk');
const got = require('got');
const mockStream = require('stream');

const FanOutConsumer = require('./fan-out-consumer');
const records = require('./records');
const stats = require('./stats');
const deaggregate = require('./deaggregate');

jest.mock('./deaggregate');

jest.mock('util', () => {
  const { promisify, ...otherUtils } = jest.requireActual('util');
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

jest.mock('./records', () => {
  const RecordsDecoder = jest.fn(
    () =>
      new mockStream.Transform({
        objectMode: true,
        transform(chunk, encoding, callback) {
          const transformedRecords = Object.keys(chunk).reduce(
            (agg, key) => ({ ...agg, [key[0].toLowerCase() + key.slice(1)]: chunk[key] }),
            {}
          );
          this.push(transformedRecords);
          callback();
        }
      })
  );
  return { RecordsDecoder };
});

jest.mock('./stream', () => ({
  getStreamShards: () => ({})
}));

jest.mock('./stats');

jest.useFakeTimers();

function nextTickWait() {
  return new Promise((resolve) => setImmediate(resolve));
}

describe('lib/fan-out-consumer', () => {
  const debug = jest.fn();
  const error = jest.fn();
  const warn = jest.fn();
  const logger = { debug, error, warn };

  const getShardsData = jest.fn(() =>
    Promise.resolve({ shardsPath: '#a', shardsPathNames: { '#a': 'a' } })
  );
  const markShardAsDepleted = jest.fn();
  const storeShardCheckpoint = jest.fn();
  const stateStore = { getShardsData, markShardAsDepleted, storeShardCheckpoint };

  const pushToStream = jest.fn();
  const stopConsumer = jest.fn();
  const options = {
    awsOptions: { region: 'us-east-1' },
    checkpoint: null,
    client: {},
    compression: 'LZ-UTF8',
    consumerArn: 'arn:enhanced-consumer',
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
    aws.mockClear();
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
    clearTimeout.mockReset();
    setTimeout.mockReset();
    jest.clearAllTimers();
    jest.useFakeTimers();
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
      region: 'us-east-1',
      throwHttpErrors: false
    });
    const { beforeRequest } = got.getHooks();
    const [signRequest] = beforeRequest;
    expect(signRequest).toBeInstanceOf(Function);
    const requestOptions = {};
    await signRequest(requestOptions);
    expect(requestOptions).toEqual({
      headers: {
        Authorization: expect.stringMatching(/^AWS4-HMAC-SHA256 Credential=resolved-access-key-id/),
        Host: '.us-east-1.amazonaws.com',
        'X-Amz-Date': expect.stringMatching(/^\d{8}T\d{6}Z$/),
        'X-Amz-Security-Token': 'resolved-session-token'
      },
      hostname: '.us-east-1.amazonaws.com',
      path: '/'
    });
  });

  test('the constructor adds a hook to sign requests with the passed credentials', async () => {
    const consumer = new FanOutConsumer({
      awsOptions: { accessKeyId: 'foo', secretAccessKey: 'bar', sessionToken: 'baz' }
    });
    expect(consumer).toBeInstanceOf(FanOutConsumer);
    const { beforeRequest } = got.getHooks();
    const [signRequest] = beforeRequest;
    expect(signRequest).toBeInstanceOf(Function);
    const requestOptions = {};
    await signRequest(requestOptions);
    expect(requestOptions).toEqual({
      headers: {
        Authorization: expect.stringMatching(/^AWS4-HMAC-SHA256 Credential=foo/),
        Host: '.us-east-1.amazonaws.com',
        'X-Amz-Date': expect.stringMatching(/^\d{8}T\d{6}Z$/),
        'X-Amz-Security-Token': 'baz'
      },
      hostname: '.us-east-1.amazonaws.com',
      path: '/'
    });
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

    expect(stream).toHaveBeenCalledWith('/', {
      body: JSON.stringify({
        ConsumerARN: 'arn:enhanced-consumer',
        ShardId: 'shard-0001',
        StartingPosition: { Type: 'LATEST' }
      }),
      headers: { 'X-Amz-Target': 'Kinesis_20131202.SubscribeToShard' },
      service: 'kinesis'
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
    expect(stream).toHaveBeenCalledWith('/', {
      body: JSON.stringify({
        ConsumerARN: 'arn:enhanced-consumer',
        ShardId: 'shard-0001',
        StartingPosition: { SequenceNumber: '1', Type: 'AFTER_SEQUENCE_NUMBER' }
      }),
      headers: { 'X-Amz-Target': 'Kinesis_20131202.SubscribeToShard' },
      service: 'kinesis'
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
    expect(stream).toHaveBeenCalledWith('/', {
      body: JSON.stringify({
        ConsumerARN: 'arn:enhanced-consumer',
        ShardId: 'shard-0001',
        StartingPosition: { SequenceNumber: '1', Type: 'AFTER_SEQUENCE_NUMBER' }
      }),
      headers: { 'X-Amz-Target': 'Kinesis_20131202.SubscribeToShard' },
      service: 'kinesis'
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
