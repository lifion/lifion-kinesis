'use strict';

const PollingConsumer = require('./polling-consumer');

jest.mock('./stream', () => ({
  getStreamShards: () => Promise.resolve({ 'shard-0000': {} })
}));

jest.useFakeTimers();

describe('lib/polling-consumer', () => {
  const getRecords = jest.fn(() =>
    Promise.resolve({
      MillisBehindLatest: 10,
      NextShardIterator: 'next-iterator',
      Records: [{ Data: 'foo', SequenceNumber: 1 }]
    })
  );
  const getShardIterator = jest.fn(() => Promise.resolve({ ShardIterator: 'iterator' }));
  const client = { getRecords, getShardIterator };

  const debug = jest.fn();
  const errorMock = jest.fn();
  const warn = jest.fn();
  const logger = { debug, error: errorMock, warn };

  const getShardsData = jest.fn(() => Promise.resolve({ shardsPath: '', shardsPathNames: {} }));
  const markShardAsDepleted = jest.fn();
  const storeShardCheckpoint = jest.fn();
  const stateStore = { getShardsData, markShardAsDepleted, storeShardCheckpoint };

  const pushToStream = jest.fn();
  const stopConsumer = jest.fn();
  const options = {
    client,
    leaseExpiration: new Date(Date.now() + 1000 * 60 * 5),
    logger,
    noRecordsPollDelay: 1000,
    pollDelay: 250,
    pushToStream,
    shardId: 'shardId-0000',
    stateStore,
    stopConsumer,
    streamName: 'stream',
    useAutoCheckpoints: true,
    usePausedPolling: false
  };

  afterEach(() => {
    debug.mockClear();
    errorMock.mockClear();
    getRecords.mockClear();
    getShardIterator.mockClear();
    getShardsData.mockClear();
    markShardAsDepleted.mockClear();
    pushToStream.mockClear();
    setTimeout.mockClear();
    stopConsumer.mockClear();
    storeShardCheckpoint.mockClear();
    warn.mockClear();
  });

  test('the module exports the expected', () => {
    expect(PollingConsumer).toEqual(expect.any(Function));
    expect(PollingConsumer).toThrow('Class constructor');
  });

  test('the consumer keeps polling for records if they were previously available', done => {
    const consumer = new PollingConsumer(options);
    setTimeout.mockImplementationOnce(() => {
      try {
        expect(getShardIterator).toHaveBeenCalledWith({
          ShardId: 'shardId-0000',
          ShardIteratorType: 'LATEST',
          StreamName: 'stream'
        });
        expect(getRecords).toHaveBeenCalledWith({ ShardIterator: 'iterator' });
        expect(storeShardCheckpoint).toHaveBeenCalledWith('shardId-0000', 1, '', {});
        expect(pushToStream).toHaveBeenCalledWith(null, {
          millisBehindLatest: 10,
          records: [{ data: 'foo', sequenceNumber: 1 }],
          shardId: 'shardId-0000',
          streamName: 'stream'
        });
        expect(debug.mock.calls).toEqual([
          ['Starting to read shard "shardId-0000" from the latest record.'],
          ['Got 1 record(s) from "shardId-0000" (10ms behind)']
        ]);
        consumer.stop();
        done();
      } catch (err) {
        done.fail(err);
      }
    });
    consumer.start();
  });

  test('the consumer uses the next shard iterator for the subsequent get records call', done => {
    const consumer = new PollingConsumer(options);
    setTimeout.mockImplementationOnce((callback, delay, ...args) => {
      getShardIterator.mockClear();
      setImmediate(() => callback(...args));
    });
    setTimeout.mockImplementationOnce(() => {
      try {
        expect(getShardIterator).not.toHaveBeenCalled();
        expect(getRecords.mock.calls).toEqual([
          [{ ShardIterator: 'iterator' }],
          [{ ShardIterator: 'next-iterator' }]
        ]);
        expect(debug.mock.calls).toEqual([
          ['Starting to read shard "shardId-0000" from the latest record.'],
          ['Got 1 record(s) from "shardId-0000" (10ms behind)'],
          ['Got 1 record(s) from "shardId-0000" (10ms behind)']
        ]);
        consumer.stop();
        done();
      } catch (err) {
        done.fail(err);
      }
    });
    consumer.start();
  });

  test('data will only be pushed when calling continue polling on paused polling mode', done => {
    const consumer = new PollingConsumer({ ...options, usePausedPolling: true });
    pushToStream.mockImplementationOnce((err, { continuePolling }) => {
      if (!err) {
        try {
          expect(continuePolling).toBeInstanceOf(Function);
          expect(storeShardCheckpoint).not.toHaveBeenCalled();
          setImmediate(continuePolling);
        } catch (err2) {
          done.fail(err2);
        }
      } else {
        done.fail(err);
      }
    });
    pushToStream.mockImplementationOnce(() => {
      try {
        expect(storeShardCheckpoint).toHaveBeenCalledWith('shardId-0000', 1, '', {});
        expect(setTimeout).not.toHaveBeenCalled();
        expect(debug.mock.calls).toEqual([
          ['Starting to read shard "shardId-0000" from the latest record.'],
          ['Got 1 record(s) from "shardId-0000" (10ms behind)'],
          ['Got 1 record(s) from "shardId-0000" (10ms behind)']
        ]);
        consumer.stop();
        done();
      } catch (err) {
        done.fail(err);
      }
    });
    consumer.start();
  });

  test('the consumer uses the checkpoint when retrieving an iterator if available', done => {
    const consumer = new PollingConsumer({ ...options, checkpoint: '123' });
    setTimeout.mockImplementationOnce(() => {
      try {
        expect(getShardIterator).toHaveBeenCalledWith({
          ShardId: 'shardId-0000',
          ShardIteratorType: 'AFTER_SEQUENCE_NUMBER',
          StartingSequenceNumber: '123',
          StreamName: 'stream'
        });
        expect(debug.mock.calls).toEqual([
          ['Starting to read shard "shardId-0000" from a known checkpoint.'],
          ['Got 1 record(s) from "shardId-0000" (10ms behind)']
        ]);
        consumer.stop();
        done();
      } catch (err) {
        done.fail(err);
      }
    });
    consumer.start();
  });

  test('the consumer is able to push errors from internal SDK calls', async done => {
    const error = new Error('foo');
    getShardIterator.mockRejectedValueOnce(error);
    const consumer = new PollingConsumer(options);
    pushToStream.mockImplementationOnce(err => {
      try {
        expect(err).toBe(error);
        expect(debug.mock.calls).toEqual([
          ['Starting to read shard "shardId-0000" from the latest record.']
        ]);
        expect(errorMock.mock.calls).toEqual([[error]]);
        consumer.stop();
        done();
      } catch (err2) {
        done.fail(err2);
      }
    });
    await expect(consumer.start()).resolves.toBeUndefined();
  });

  test('an iterator for the latest records replaces an invalid checkpoint', done => {
    const error = Object.assign(new Error('foo'), { code: 'InvalidArgumentException' });
    getShardIterator.mockRejectedValueOnce(error);
    const consumer = new PollingConsumer({ ...options, checkpoint: '123' });
    pushToStream.mockImplementationOnce(() => {
      try {
        expect(getShardIterator).toHaveBeenNthCalledWith(1, {
          ShardId: 'shardId-0000',
          ShardIteratorType: 'AFTER_SEQUENCE_NUMBER',
          StartingSequenceNumber: '123',
          StreamName: 'stream'
        });
        expect(getShardIterator).toHaveBeenNthCalledWith(2, {
          ShardId: 'shardId-0000',
          ShardIteratorType: 'LATEST',
          StreamName: 'stream'
        });
        expect(warn.mock.calls).toEqual([
          ['The stored checkpoint for "stream/shardId-0000" is invalid. Ignoring it.']
        ]);
        expect(debug.mock.calls).toEqual([
          ['Starting to read shard "shardId-0000" from a known checkpoint.'],
          ['Got 1 record(s) from "shardId-0000" (10ms behind)']
        ]);
        expect(errorMock).not.toHaveBeenCalled();
        consumer.stop();
        done();
      } catch (err) {
        done.fail(err);
      }
    });
    consumer.start();
  });

  test('starting a consumer with an expired lease will make it stop immediately', done => {
    const leaseExpiration = new Date(Date.now() - 1000);
    const consumer = new PollingConsumer({ ...options, leaseExpiration });
    stopConsumer.mockImplementationOnce(shardId => {
      try {
        expect(shardId).toBe('shardId-0000');
        expect(getRecords).not.toHaveBeenCalled();
        expect(setTimeout).not.toHaveBeenCalled();
        expect(debug.mock.calls).toEqual([
          ['Unable to read from shard "shardId-0000" anymore, the lease expired.']
        ]);
        consumer.stop();
        done();
      } catch (err) {
        done.fail(err);
      }
    });
    consumer.start();
  });

  test('the consumer should stop if the lease expiration is updated to the past', done => {
    const consumer = new PollingConsumer(options);
    setTimeout.mockImplementationOnce((callback, delay, ...args) => {
      consumer.updateLeaseExpiration(new Date(Date.now() - 1000));
      setImmediate(() => callback(...args));
    });
    stopConsumer.mockImplementationOnce(() => {
      try {
        expect(getShardIterator).toHaveBeenCalledTimes(1);
        expect(getRecords).toHaveBeenCalledTimes(1);
        expect(debug.mock.calls).toEqual([
          ['Starting to read shard "shardId-0000" from the latest record.'],
          ['Got 1 record(s) from "shardId-0000" (10ms behind)'],
          ['Unable to read from shard "shardId-0000" anymore, the lease expired.']
        ]);
        consumer.stop();
        done();
      } catch (err) {
        done.fail(err);
      }
    });
    consumer.start();
  });

  test('if no records are retrieved continue after the no records poll delay', done => {
    const consumer = new PollingConsumer(options);
    getRecords.mockResolvedValueOnce({
      MillisBehindLatest: 0,
      NextShardIterator: 'next-iterator',
      Records: []
    });
    setTimeout.mockImplementationOnce((callback, delay) => {
      try {
        expect(delay).toBe(1000);
        expect(debug.mock.calls).toEqual([
          ['Starting to read shard "shardId-0000" from the latest record.']
        ]);
        consumer.stop();
        done();
      } catch (err) {
        done.fail(err);
      }
    });
    consumer.start();
  });

  test('fast-forward if no records are retrieved and the iterator is behind', done => {
    const consumer = new PollingConsumer(options);
    getRecords.mockResolvedValueOnce({
      MillisBehindLatest: 1000,
      NextShardIterator: 'next-iterator',
      Records: []
    });
    setTimeout.mockImplementationOnce((callback, delay) => {
      try {
        expect(delay).toBe(0);
        expect(debug.mock.calls).toEqual([
          ['Starting to read shard "shardId-0000" from the latest record.'],
          ['Fast-forwarding "shardId-0000"… (1000ms behind)']
        ]);
        consumer.stop();
        done();
      } catch (err) {
        done.fail(err);
      }
    });
    consumer.start();
  });

  test('mark the shard as depleted if there is no next iterator after a get records call', done => {
    const consumer = new PollingConsumer(options);
    getRecords.mockResolvedValueOnce({
      MillisBehindLatest: 0,
      Records: []
    });
    stopConsumer.mockImplementationOnce(() => {
      try {
        expect(markShardAsDepleted).toHaveBeenCalledWith({ 'shard-0000': {} }, 'shardId-0000');
        expect(setTimeout).not.toHaveBeenCalled();
        expect(debug.mock.calls).toEqual([
          ['Starting to read shard "shardId-0000" from the latest record.'],
          ['The parent shard "shardId-0000" has been depleted.']
        ]);
        consumer.stop();
        done();
      } catch (err) {
        done.fail(err);
      }
    });
    consumer.start();
  });

  test('checkpoints can be set manually if the use auto checkpoints option is disabled', done => {
    const consumer = new PollingConsumer({ ...options, useAutoCheckpoints: false });
    pushToStream.mockImplementationOnce(async (err, { setCheckpoint }) => {
      if (!err) {
        try {
          expect(setCheckpoint).toBeInstanceOf(Function);
          expect(storeShardCheckpoint).not.toHaveBeenCalled();
          await setCheckpoint('123');
          expect(storeShardCheckpoint).toHaveBeenCalledWith('shardId-0000', '123', '', {});
          expect(debug.mock.calls).toEqual([
            ['Starting to read shard "shardId-0000" from the latest record.'],
            ['Got 1 record(s) from "shardId-0000" (10ms behind)']
          ]);
          consumer.stop();
          done();
        } catch (err2) {
          done.fail(err2);
        }
      } else {
        done.fail(err);
      }
    });
    consumer.start();
  });
});
