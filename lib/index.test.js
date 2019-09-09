'use strict';

const Chance = require('chance');
const { resetMockCounter } = require('short-uuid');

const ConsumersManager = require('./consumers-manager');
const HeartbeatManager = require('./heartbeat-manager');
const KinesisClient = require('./kinesis-client');
const LeaseManager = require('./lease-manager');
const RecordsModule = require('./records');
const StateStore = require('./state-store');
const stats = require('./stats');
const stream = require('./stream');
const S3Client = require('./s3-client');
// const bucket = require('./bucket');

const Kinesis = require('.');

const chance = new Chance();

jest.mock('./stats');

jest.mock('./consumers-manager', () => {
  const reconcile = jest.fn();
  const stop = jest.fn();
  return jest.fn(() => ({ reconcile, stop }));
});

jest.mock('./heartbeat-manager', () => {
  const start = jest.fn();
  const stop = jest.fn();
  return jest.fn(() => ({ start, stop }));
});

jest.mock('./kinesis-client', () => {
  const putRecord = jest.fn(() =>
    Promise.resolve({
      EncryptionType: 'foo',
      SequenceNumber: '0',
      ShardId: 'baz'
    })
  );
  const putRecords = jest.fn(({ Records }) =>
    Promise.resolve({
      EncryptionType: 'foo',
      Records: Records.map((item, index) => ({
        SequenceNumber: index.toString(),
        ShardId: 'baz'
      }))
    })
  );
  return jest.fn(() => ({ isEndpointLocal: () => true, putRecord, putRecords }));
});
jest.mock('./s3-client', () => {
  const putObject = jest.fn().mockResolvedValue({
    ETag: ''
  });
  const createBucket = jest.fn().mockResolvedValue({});
  const getBucketTagging = jest.fn().mockResolvedValue({});
  const getBucketLifecycleConfiguration = jest.fn().mockResolvedValue({});
  const putBucketLifecycleConfiguration = jest.fn().mockResolvedValue({});
  return jest.fn(() => ({
    createBucket,
    getBucketLifecycleConfiguration,
    getBucketTagging,
    putBucketLifecycleConfiguration,
    putObject
  }));
});
jest.mock('./bucket');

jest.mock('./lease-manager', () => {
  const start = jest.fn();
  const stop = jest.fn();
  return jest.fn(() => ({ start, stop }));
});

jest.mock('./state-store', () => {
  let enhancedConsumers = {};
  const clearMockData = () => {
    enhancedConsumers = {};
  };
  const start = jest.fn();
  const getEnhancedConsumers = jest.fn(() => Promise.resolve(enhancedConsumers));
  const registerEnhancedConsumer = jest.fn((consumerName, arn) => {
    enhancedConsumers[consumerName] = { arn };
    return Promise.resolve();
  });
  const deregisterEnhancedConsumer = jest.fn(consumerName => {
    delete enhancedConsumers[consumerName];
    return Promise.resolve();
  });
  return jest.fn(() => ({
    clearMockData,
    deregisterEnhancedConsumer,
    getEnhancedConsumers,
    registerEnhancedConsumer,
    start
  }));
});

jest.mock('./stream', () => {
  let enhancedConsumers = {};
  return {
    clearMockData: () => {
      enhancedConsumers = {};
    },
    confirmStreamTags: jest.fn(),
    ensureStreamEncription: jest.fn(),
    ensureStreamExists: jest.fn(() =>
      Promise.resolve({
        streamArn: 'arn:test-stream',
        streamCreatedOn: new Date('2019-01-01').toISOString()
      })
    ),
    getEnhancedConsumers: jest.fn(() => Promise.resolve(enhancedConsumers)),
    registerEnhancedConsumer: jest.fn(({ consumerName }) => {
      enhancedConsumers[consumerName] = {
        arn: `arn:consumer-${Object.keys(enhancedConsumers).length}`
      };
      return Promise.resolve();
    })
  };
});

jest.useFakeTimers();

describe('lib/index', () => {
  const options = { compression: 'LZ-UTF8', streamName: 'test-stream' };
  const largeDoc = chance.paragraph({ sentences: 20000 });

  beforeAll(() => {
    stats.getStats.mockImplementation(streamName => ({
      stats: {},
      ...(streamName && { [streamName]: { stats: {} } })
    }));
  });

  afterEach(() => {
    const consumersManager = new ConsumersManager();
    consumersManager.reconcile.mockClear();
    consumersManager.stop.mockClear();

    const hearbeatManager = new HeartbeatManager();
    hearbeatManager.start.mockClear();
    hearbeatManager.stop.mockClear();

    const kinesisClient = new KinesisClient();
    kinesisClient.putRecord.mockClear();
    kinesisClient.putRecords.mockClear();

    const leaseManager = new LeaseManager();
    leaseManager.start.mockClear();
    leaseManager.stop.mockClear();

    const stateStore = new StateStore();
    stateStore.start.mockClear();
    stateStore.getEnhancedConsumers.mockClear();
    stateStore.registerEnhancedConsumer.mockClear();
    stateStore.deregisterEnhancedConsumer.mockClear();
    stateStore.clearMockData();

    ConsumersManager.mockClear();
    HeartbeatManager.mockClear();
    KinesisClient.mockClear();
    LeaseManager.mockClear();
    StateStore.mockClear();

    stats.getStats.mockClear();
    stats.reportRecordConsumed.mockClear();

    stream.confirmStreamTags.mockClear();
    stream.ensureStreamEncription.mockClear();
    stream.ensureStreamExists.mockClear();
    stream.getEnhancedConsumers.mockClear();
    stream.registerEnhancedConsumer.mockClear();
    stream.clearMockData();

    setTimeout.mockClear();
    resetMockCounter();
  });

  test('the module exports the expected', () => {
    expect(Kinesis).toEqual(expect.any(Function));
    expect(Kinesis).toThrow('Class constructor');
  });

  test('the constructor should throw if not provided with a stream name', () => {
    expect(() => new Kinesis()).toThrow('The "streamName" option is required.');
  });

  test('starting a consumer will make sure the stream exists', async () => {
    const kinesis = new Kinesis({
      ...options,
      encryption: { keyId: 'foo', type: 'bar' },
      tags: { baz: 'qux' }
    });
    await expect(kinesis.startConsumer()).resolves.toBeUndefined();
    expect(stream.ensureStreamExists).toHaveBeenCalledWith(
      expect.objectContaining({ streamName: 'test-stream' })
    );
    expect(stream.ensureStreamEncription).toHaveBeenCalledWith(
      expect.objectContaining({ encryption: { keyId: 'foo', type: 'bar' } })
    );
    expect(stream.confirmStreamTags).toHaveBeenCalledWith(
      expect.objectContaining({ tags: { baz: 'qux' } })
    );
    kinesis.stopConsumer();
  });

  test('defaults s3 bucketName to streamName when starting a consumer with useS3ForLargeItems set to true', async () => {
    const mockLogger = { debug: jest.fn(), error: jest.fn(), warn: jest.fn() };
    const getRecordsEncoderSpy = jest.spyOn(RecordsModule, 'getRecordsEncoder');
    const kinesis = new Kinesis({
      ...options,
      logger: mockLogger,
      s3: { largeItemThreshold: 900, nonS3Keys: [] },
      useS3ForLargeItems: true
    });

    await expect(kinesis.startConsumer()).resolves.toBeUndefined();

    expect(getRecordsEncoderSpy).toHaveBeenCalledWith({
      compression: 'LZ-UTF8',
      outputEncoding: 'Buffer',
      s3: { bucketName: 'test-stream', largeItemThreshold: 900, nonS3Keys: [] },
      s3Client: expect.any(Object),
      streamName: 'test-stream',
      useS3ForLargeItems: true
    });
    kinesis.stopConsumer();
  });

  test('starting a consumer will make the internal managers start', async () => {
    const mockLogger = { debug: jest.fn(), error: jest.fn(), warn: jest.fn() };
    const kinesis = new Kinesis({ ...options, logger: mockLogger, foo: 'bar' });
    const awsOptions = { foo: 'bar' };
    const logger = {
      debug: expect.any(Function),
      error: expect.any(Function),
      warn: expect.any(Function)
    };
    const streamName = 'test-stream';
    expect(KinesisClient).toHaveBeenCalledWith({ awsOptions, logger, streamName });
    await expect(kinesis.startConsumer()).resolves.toBeUndefined();
    const useEnhancedFanOut = false;
    const consumerId = '0000';
    const useAutoShardAssignment = true;
    expect(StateStore).toHaveBeenCalledWith(
      expect.objectContaining({
        consumerGroup: 'lifion-kinesis',
        consumerId,
        dynamoDb: {},
        logger,
        streamCreatedOn: '2019-01-01T00:00:00.000Z',
        streamName,
        useAutoShardAssignment,
        useEnhancedFanOut
      })
    );
    const stateStore = new StateStore();
    expect(stateStore.start).toHaveBeenCalled();
    expect(HeartbeatManager).toHaveBeenCalledWith(expect.objectContaining({ logger, stateStore }));
    expect(new HeartbeatManager().start).toHaveBeenCalled();
    const client = expect.any(Object);
    expect(ConsumersManager).toHaveBeenCalledWith(
      expect.objectContaining({
        awsOptions,
        client,
        compression: 'LZ-UTF8',
        limit: 10000,
        logger,
        noRecordsPollDelay: 1000,
        pollDelay: 250,
        pushToStream: expect.any(Function),
        stateStore,
        streamName,
        useAutoCheckpoints: true,
        useEnhancedFanOut,
        usePausedPolling: false
      })
    );
    const consumersManager = new ConsumersManager();
    expect(consumersManager.reconcile).toHaveBeenCalled();
    expect(LeaseManager).toHaveBeenCalledWith(
      expect.objectContaining({
        client,
        consumerId,
        consumersManager,
        logger,
        stateStore,
        streamName,
        useAutoShardAssignment,
        useEnhancedFanOut
      })
    );
    expect(new LeaseManager().start).toHaveBeenCalled();
    expect(setTimeout).toHaveBeenCalledWith(expect.any(Function), 30000);
    expect(mockLogger.debug.mock.calls).toEqual([
      ['Trying to start the consumer…'],
      ['The consumer is now ready.']
    ]);
    expect(mockLogger.error).not.toHaveBeenCalled();
    expect(mockLogger.warn).not.toHaveBeenCalled();
    kinesis.stopConsumer();
  });

  test('invalid options in the constructor should be defaulted', async () => {
    const kinesis = new Kinesis({
      ...options,
      limit: -100,
      noRecordsPollDelay: 0,
      pollDelay: -100,
      shardCount: 0,
      statsInterval: 100
    });
    await expect(kinesis.startConsumer()).resolves.toBeUndefined();
    expect(StateStore).toHaveBeenCalledWith(
      expect.objectContaining({
        limit: 10000,
        noRecordsPollDelay: 250,
        pollDelay: 250,
        shardCount: 1,
        statsInterval: 30000
      })
    );
    kinesis.stopConsumer();
  });

  test('enhanced consumers should be set up if the option is on', async () => {
    await stream.registerEnhancedConsumer({ consumerName: 'foo-1' });
    await stream.registerEnhancedConsumer({ consumerName: 'foo-2' });
    stream.registerEnhancedConsumer.mockClear();

    const stateStore = new StateStore();
    await stateStore.registerEnhancedConsumer('bar-1', 'arn:bar-1');
    await stateStore.registerEnhancedConsumer('bar-2', 'arn:bar-2');
    stateStore.registerEnhancedConsumer.mockClear();

    const debug = jest.fn();
    const kinesis = new Kinesis({ ...options, logger: { debug }, useEnhancedFanOut: true });
    await expect(kinesis.startConsumer()).resolves.toBeUndefined();
    expect(stream.registerEnhancedConsumer.mock.calls).toEqual([
      [expect.objectContaining({ consumerName: 'lifion-kinesis-0001' })],
      [expect.objectContaining({ consumerName: 'lifion-kinesis-0002' })],
      [expect.objectContaining({ consumerName: 'lifion-kinesis-0003' })]
    ]);
    expect(stateStore.registerEnhancedConsumer.mock.calls).toEqual([
      ['foo-1', 'arn:consumer-0'],
      ['foo-2', 'arn:consumer-1'],
      ['lifion-kinesis-0001', 'arn:consumer-2'],
      ['lifion-kinesis-0002', 'arn:consumer-3'],
      ['lifion-kinesis-0003', 'arn:consumer-4']
    ]);
    expect(stateStore.deregisterEnhancedConsumer.mock.calls).toEqual([['bar-1'], ['bar-2']]);
    const enhancedConsumers = {
      'foo-1': { arn: 'arn:consumer-0' },
      'foo-2': { arn: 'arn:consumer-1' },
      'lifion-kinesis-0001': { arn: 'arn:consumer-2' },
      'lifion-kinesis-0002': { arn: 'arn:consumer-3' },
      'lifion-kinesis-0003': { arn: 'arn:consumer-4' }
    };
    expect(await stream.getEnhancedConsumers()).toEqual(enhancedConsumers);
    expect(await stateStore.getEnhancedConsumers()).toEqual(enhancedConsumers);
    expect(debug.mock.calls).toEqual([
      ['Trying to start the consumer…'],
      ['Cleaning up enhanced consumers for "test-stream"…'],
      ['The consumer is now ready.']
    ]);
    kinesis.stopConsumer();
  });

  test('the stream should be able to pass data', async done => {
    const kinesis = new Kinesis(options);
    kinesis.on('data', data => {
      try {
        expect(data).toEqual({ foo: 'bar' });
        done();
      } catch (err) {
        done.fail(err);
      }
    });
    await kinesis.startConsumer();
    const [[{ pushToStream }]] = ConsumersManager.mock.calls;
    pushToStream(null, { foo: 'bar' });
    try {
      expect(stats.reportRecordConsumed).toHaveBeenCalledWith('test-stream');
    } catch (err) {
      done.fail(err);
    }
    kinesis.stopConsumer();
  });

  test('the stream should be able to pass errors', async done => {
    const kinesis = new Kinesis(options);
    kinesis.on('error', err => {
      try {
        expect(err.message).toBe('foo');
        done();
      } catch (err_) {
        done.fail(err_);
      }
    });
    await kinesis.startConsumer();
    const [[{ pushToStream }]] = ConsumersManager.mock.calls;
    pushToStream(new Error('foo'));
    try {
      expect(stats.reportRecordConsumed).not.toHaveBeenCalled();
    } catch (err) {
      done.fail(err);
    }
    kinesis.stopConsumer();
  });

  test('the stream should be able emit stats', async done => {
    const kinesis = new Kinesis(options);
    kinesis.on('stats', data => {
      try {
        expect(data).toEqual({
          stats: {},
          'test-stream': {
            stats: {}
          }
        });
        done();
      } catch (err) {
        done.fail(err);
      }
    });
    await kinesis.startConsumer();
    jest.runOnlyPendingTimers();
    kinesis.stopConsumer();
  });

  test('stats can be retrieved for instances of the module', async () => {
    const kinesis = new Kinesis(options);
    await kinesis.startConsumer();
    expect(kinesis.getStats()).toEqual({
      stats: {},
      'test-stream': {
        stats: {}
      }
    });
  });

  test('stats can be retrieved for the module', () => {
    expect(Kinesis.getStats()).toEqual({ stats: {} });
  });

  describe('putRecord', () => {
    test('a record can be writen to the Kinesis stream', async () => {
      const kinesis = new Kinesis(options);
      await kinesis.startConsumer();
      const promise = kinesis.putRecord({ data: 'foo' });
      jest.runOnlyPendingTimers();
      await expect(promise).resolves.toEqual({
        encryptionType: 'foo',
        sequenceNumber: '0',
        shardId: 'baz'
      });
    });

    test('a call with no parameters to put record should throw', async () => {
      const kinesis = new Kinesis(options);
      await kinesis.startConsumer();
      const promise = kinesis.putRecord();
      jest.runOnlyPendingTimers();
      await expect(promise).rejects.toThrow('The "data" property is required.');
    });

    test('trying to put a record on a non-existing stream should create it', async () => {
      new KinesisClient().putRecord.mockRejectedValueOnce(
        Object.assign(new Error('foo'), { code: 'ResourceNotFoundException' })
      );
      const kinesis = new Kinesis({
        ...options,
        encryption: { keyId: 'foo', type: 'bar' },
        tags: { baz: 'qux' }
      });
      await kinesis.startConsumer();

      const promise = kinesis.putRecord({ data: 'foo', streamName: 'test-stream-2' });
      jest.runOnlyPendingTimers();
      await expect(promise).resolves.toEqual(expect.any(Object));
      expect(stream.ensureStreamExists).toHaveBeenCalledWith(
        expect.objectContaining({ streamName: 'test-stream-2' })
      );
      expect(stream.ensureStreamEncription).toHaveBeenCalledWith(
        expect.objectContaining({ encryption: { keyId: 'foo', type: 'bar' } })
      );
      expect(stream.confirmStreamTags).toHaveBeenCalledWith(
        expect.objectContaining({ tags: { baz: 'qux' } })
      );
    });

    test('trying to put a record on a non-existing local stream should create it', async () => {
      new KinesisClient().putRecord.mockRejectedValueOnce(
        Object.assign(new Error('foo'), { code: 'UnknownError' })
      );
      const kinesis = new Kinesis(options);
      await kinesis.startConsumer();
      const promise = kinesis.putRecord({ data: 'foo', streamName: 'test-stream-2' });
      jest.runOnlyPendingTimers();
      await expect(promise).resolves.toEqual(expect.any(Object));
      expect(stream.ensureStreamExists).toHaveBeenCalledWith(
        expect.objectContaining({ streamName: 'test-stream-2' })
      );
    });

    test('trying to put a large record through S3 should initialize the stream if there is no records encoder', async () => {
      new S3Client().putObject.mockResolvedValue({
        ETag: ''
      });
      stream.ensureStreamExists.mockResolvedValue({
        streamArn: 'streamArn',
        streamCreatedOn: 'streamCreatedOn'
      });
      const kinesis = new Kinesis({ streamName: 'test-stream', useS3ForLargeItems: true });
      const promise = kinesis.putRecord({ data: largeDoc, streamName: 'test-stream' });

      jest.runOnlyPendingTimers();
      await expect(promise).resolves.toEqual(expect.any(Object));
      expect(stream.ensureStreamExists).toHaveBeenCalledWith(
        expect.objectContaining({ streamName: 'test-stream' })
      );
    });

    test('the call to put a record should throw errors from the internal call', async () => {
      const error = new Error('foo');
      new KinesisClient().putRecord.mockRejectedValueOnce(error);
      const kinesis = new Kinesis(options);
      await kinesis.startConsumer();
      const promise = kinesis.putRecord({ data: 'foo' });
      jest.runOnlyPendingTimers();
      await expect(promise).rejects.toThrow('foo');
    });
  });

  describe('putRecords', () => {
    test('records can be writen to the Kinesis stream', async () => {
      const kinesis = new Kinesis(options);
      await kinesis.startConsumer();
      const promise = kinesis.putRecords({ records: [{ data: 'foo' }, { data: 'bar' }] });
      jest.runOnlyPendingTimers();
      await expect(promise).resolves.toEqual({
        encryptionType: 'foo',
        records: [{ sequenceNumber: '0', shardId: 'baz' }, { sequenceNumber: '1', shardId: 'baz' }]
      });
    });

    test('a call with no parameters to put records should throw', async () => {
      const kinesis = new Kinesis(options);
      await kinesis.startConsumer();
      let promise = kinesis.putRecords();
      jest.runOnlyPendingTimers();
      await expect(promise).rejects.toThrow('The "records" property is required.');
      promise = kinesis.putRecords({ records: [{}] });
      jest.runOnlyPendingTimers();
      await expect(promise).rejects.toThrow('The "data" property is required.');
    });

    test('trying to put records on a non-existing stream should create it', async () => {
      new KinesisClient().putRecords.mockRejectedValueOnce(
        Object.assign(new Error('foo'), { code: 'ResourceNotFoundException' })
      );
      const kinesis = new Kinesis({
        ...options,
        encryption: { keyId: 'foo', type: 'bar' },
        tags: { baz: 'qux' }
      });
      await kinesis.startConsumer();
      const promise = kinesis.putRecords({
        records: [{ data: 'foo' }],
        streamName: 'test-stream-2'
      });
      jest.runOnlyPendingTimers();
      await expect(promise).resolves.toEqual(expect.any(Object));
      expect(stream.ensureStreamExists).toHaveBeenCalledWith(
        expect.objectContaining({ streamName: 'test-stream-2' })
      );
      expect(stream.ensureStreamEncription).toHaveBeenCalledWith(
        expect.objectContaining({ encryption: { keyId: 'foo', type: 'bar' } })
      );
      expect(stream.confirmStreamTags).toHaveBeenCalledWith(
        expect.objectContaining({ tags: { baz: 'qux' } })
      );
    });

    test('trying to put large records through S3 should initialize the stream if there is no records encoder', async () => {
      new S3Client().putObject.mockResolvedValue({
        ETag: ''
      });
      stream.ensureStreamExists.mockResolvedValue({
        streamArn: 'streamArn',
        streamCreatedOn: 'streamCreatedOn'
      });
      const kinesis = new Kinesis({ streamName: 'test-stream', useS3ForLargeItems: true });
      const promise = kinesis.putRecords({
        records: [{ data: largeDoc }],
        streamName: 'test-stream'
      });

      jest.runOnlyPendingTimers();
      await expect(promise).resolves.toEqual(expect.any(Object));
      expect(stream.ensureStreamExists).toHaveBeenCalledWith(
        expect.objectContaining({ streamName: 'test-stream' })
      );
    });

    test('trying to put records on a non-existing local stream should create it', async () => {
      new KinesisClient().putRecords.mockRejectedValueOnce(
        Object.assign(new Error('foo'), { code: 'UnknownError' })
      );
      const kinesis = new Kinesis(options);
      await kinesis.startConsumer();
      const promise = kinesis.putRecords({
        records: [{ data: 'foo' }],
        streamName: 'test-stream-2'
      });
      jest.runOnlyPendingTimers();
      await expect(promise).resolves.toEqual(expect.any(Object));
      expect(stream.ensureStreamExists).toHaveBeenCalledWith(
        expect.objectContaining({ streamName: 'test-stream-2' })
      );
    });

    test('the call to put records should throw errors from the internal call', async () => {
      const error = new Error('foo');
      new KinesisClient().putRecords.mockRejectedValueOnce(error);
      const kinesis = new Kinesis(options);
      await kinesis.startConsumer();
      const promise = kinesis.putRecords({ records: [{ data: 'foo' }] });
      jest.runOnlyPendingTimers();
      await expect(promise).rejects.toThrow('foo');
    });
  });
});
