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
    limit: 1000,
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

  test('the consumer keeps polling for records if they were previously available', () => {
    return new Promise((resolve, reject) => {
      const consumer = new PollingConsumer(options);
      setTimeout.mockImplementationOnce(() => {
        try {
          expect(getShardIterator).toHaveBeenCalledWith({
            ShardId: 'shardId-0000',
            ShardIteratorType: 'LATEST',
            StreamName: 'stream'
          });
          expect(getRecords).toHaveBeenCalledWith({ Limit: 1000, ShardIterator: 'iterator' });
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
          resolve();
        } catch (err) {
          reject(err);
        } finally {
          consumer.stop();
        }
      });
      consumer.start();
    });
  });

  test('the consumer uses the next shard iterator for the subsequent get records call', () => {
    return new Promise((resolve, reject) => {
      const consumer = new PollingConsumer(options);
      setTimeout.mockImplementationOnce((callback, _, ...args) => {
        getShardIterator.mockClear();
        setImmediate(() => callback(...args));
      });
      setTimeout.mockImplementationOnce(() => {
        try {
          expect(getShardIterator).not.toHaveBeenCalled();
          expect(getRecords.mock.calls).toEqual([
            [{ Limit: 1000, ShardIterator: 'iterator' }],
            [{ Limit: 1000, ShardIterator: 'next-iterator' }]
          ]);
          expect(debug.mock.calls).toEqual([
            ['Starting to read shard "shardId-0000" from the latest record.'],
            ['Got 1 record(s) from "shardId-0000" (10ms behind)'],
            ['Got 1 record(s) from "shardId-0000" (10ms behind)']
          ]);
          resolve();
        } catch (err) {
          reject(err);
        } finally {
          consumer.stop();
        }
      });
      consumer.start();
    });
  });

  test('data will only be pushed when calling continue polling on paused polling mode', () => {
    const consumer = new PollingConsumer({ ...options, usePausedPolling: true });
    return new Promise((resolve, reject) => {
      pushToStream.mockImplementationOnce((err, { continuePolling }) => {
        if (!err) {
          try {
            expect(continuePolling).toBeInstanceOf(Function);
            expect(storeShardCheckpoint).not.toHaveBeenCalled();
            setImmediate(continuePolling);
          } catch (err_) {
            reject(err);
          }
        } else {
          reject(err);
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
          resolve();
        } catch (err) {
          reject(err);
        }
      });
      consumer.start();
    }).finally(() => {
      consumer.stop();
    });
  });

  test('the consumer uses the checkpoint when retrieving an iterator if available', () => {
    return new Promise((resolve, reject) => {
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
          resolve();
        } catch (err) {
          reject();
        } finally {
          consumer.stop();
        }
      });
      consumer.start();
    });
  });

  test('the consumer is able to push errors from internal SDK calls', () => {
    const error = new Error('foo');
    getShardIterator.mockRejectedValueOnce(error);
    const consumer = new PollingConsumer(options);
    return new Promise((resolve) => {
      pushToStream.mockImplementationOnce((err) => {
        expect(err).toBe(error);
        expect(debug.mock.calls).toEqual([
          ['Starting to read shard "shardId-0000" from the latest record.']
        ]);
        expect(errorMock.mock.calls).toEqual([[error]]);
        resolve();
      });
      consumer.start();
    }).finally(() => {
      consumer.stop();
    });
  });

  test('the consumer stops when failing to resolve the shard state on start', async () => {
    getShardsData.mockImplementationOnce(() => {
      throw new Error('foo');
    });
    const consumer = new PollingConsumer(options);
    await consumer.start();
    expect(warn).toHaveBeenCalledWith(
      "Can't start the consumer as the state can't be resolved:",
      expect.objectContaining({ message: 'foo' })
    );
    expect(stopConsumer).toHaveBeenCalledWith('shardId-0000');
  });

  test('an iterator for the latest records replaces an invalid checkpoint', () => {
    return new Promise((resolve, reject) => {
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
          resolve();
        } catch (err) {
          reject(err);
        } finally {
          consumer.stop();
        }
      });
      consumer.start();
    });
  });

  test('starting a consumer with an expired lease will make it stop immediately', () => {
    return new Promise((resolve, reject) => {
      const leaseExpiration = new Date(Date.now() - 1000);
      const consumer = new PollingConsumer({ ...options, leaseExpiration });
      stopConsumer.mockImplementationOnce((shardId) => {
        try {
          expect(shardId).toBe('shardId-0000');
          expect(getRecords).not.toHaveBeenCalled();
          expect(setTimeout).not.toHaveBeenCalled();
          expect(debug.mock.calls).toEqual([
            ['Unable to read from shard "shardId-0000" anymore, the lease expired.']
          ]);
          resolve();
        } catch (err) {
          reject(err);
        } finally {
          consumer.stop();
        }
      });
      consumer.start();
    });
  });

  test('the consumer should stop if the lease expiration is updated to the past', () => {
    return new Promise((resolve, reject) => {
      const consumer = new PollingConsumer(options);
      setTimeout.mockImplementationOnce((callback, _, ...args) => {
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
          resolve();
        } catch (err) {
          reject(err);
        } finally {
          consumer.stop();
        }
      });
      consumer.start();
    });
  });

  test('if no records are retrieved continue after the no records poll delay', () => {
    return new Promise((resolve, reject) => {
      const consumer = new PollingConsumer(options);
      getRecords.mockResolvedValueOnce({
        MillisBehindLatest: 0,
        NextShardIterator: 'next-iterator',
        Records: []
      });
      setTimeout.mockImplementationOnce((_, delay) => {
        try {
          expect(delay).toBe(1000);
          expect(debug.mock.calls).toEqual([
            ['Starting to read shard "shardId-0000" from the latest record.']
          ]);
          resolve();
        } catch (err) {
          reject(err);
        } finally {
          consumer.stop();
        }
      });
      consumer.start();
    });
  });

  test('fast-forward if no records are retrieved and the iterator is behind', () => {
    return new Promise((resolve, reject) => {
      const consumer = new PollingConsumer(options);
      getRecords.mockResolvedValueOnce({
        MillisBehindLatest: 1000,
        NextShardIterator: 'next-iterator',
        Records: []
      });
      setTimeout.mockImplementationOnce((_, delay) => {
        try {
          expect(delay).toBe(0);
          expect(debug.mock.calls).toEqual([
            ['Starting to read shard "shardId-0000" from the latest record.'],
            ['Fast-forwarding "shardId-0000"… (1000ms behind)']
          ]);
          resolve();
        } catch (err) {
          reject(err);
        } finally {
          consumer.stop();
        }
      });
      consumer.start();
    });
  });

  test('mark the shard as depleted if there is no next iterator after a get records call', () => {
    return new Promise((resolve, reject) => {
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
          resolve();
        } catch (err) {
          reject(err);
        } finally {
          consumer.stop();
        }
      });
      consumer.start();
    });
  });

  test('checkpoints can be set manually if the use auto checkpoints option is disabled', () => {
    return new Promise((resolve, reject) => {
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
            resolve();
          } catch (err_) {
            reject(err_);
          } finally {
            consumer.stop();
          }
        } else {
          reject(err);
        }
      });
      consumer.start();
    });
  });

  test('the consumer keeps polling with a re-created iterator if the previous one expires', () => {
    return new Promise((resolve, reject) => {
      getShardIterator.mockResolvedValueOnce({ ShardIterator: 'iterator-1' });
      getShardIterator.mockResolvedValueOnce({ ShardIterator: 'iterator-3' });
      getRecords.mockResolvedValueOnce({
        MillisBehindLatest: 10,
        NextShardIterator: 'iterator-2',
        Records: [{ Data: 'foo', SequenceNumber: 1 }]
      });
      getRecords.mockRejectedValueOnce(
        Object.assign(new Error('The iterator has expired'), { code: 'ExpiredIteratorException' })
      );
      const consumer = new PollingConsumer(options);
      setTimeout.mockImplementationOnce((callback, _, ...args) => {
        setImmediate(() => callback(...args));
      });
      setTimeout.mockImplementationOnce(() => {
        try {
          expect(getShardIterator).toHaveBeenNthCalledWith(1, {
            ShardId: 'shardId-0000',
            ShardIteratorType: 'LATEST',
            StreamName: 'stream'
          });
          expect(getShardIterator).toHaveBeenNthCalledWith(2, {
            ShardId: 'shardId-0000',
            ShardIteratorType: 'AFTER_SEQUENCE_NUMBER',
            StartingSequenceNumber: 1,
            StreamName: 'stream'
          });
          expect(getShardIterator).toHaveBeenCalledTimes(2);
          expect(getRecords).toHaveBeenNthCalledWith(1, {
            Limit: 1000,
            ShardIterator: 'iterator-1'
          });
          expect(getRecords).toHaveBeenNthCalledWith(2, {
            Limit: 1000,
            ShardIterator: 'iterator-2'
          });
          expect(getRecords).toHaveBeenNthCalledWith(3, {
            Limit: 1000,
            ShardIterator: 'iterator-3'
          });
          expect(getRecords).toHaveBeenCalledTimes(3);
          expect(warn).toHaveBeenNthCalledWith(1, 'Previous shard iterator expired, recreating…');
          expect(warn).toHaveBeenCalledTimes(1);
          expect(debug).toHaveBeenNthCalledWith(
            1,
            'Starting to read shard "shardId-0000" from the latest record.'
          );
          expect(debug).toHaveBeenNthCalledWith(
            2,
            'Got 1 record(s) from "shardId-0000" (10ms behind)'
          );
          expect(debug).toHaveBeenNthCalledWith(
            3,
            'Starting to read shard "shardId-0000" from a known checkpoint.'
          );
          expect(debug).toHaveBeenNthCalledWith(
            4,
            'Got 1 record(s) from "shardId-0000" (10ms behind)'
          );
          expect(debug).toHaveBeenCalledTimes(4);
          resolve();
        } catch (err) {
          reject(err);
        } finally {
          consumer.stop();
        }
      });
      consumer.start();
    });
  });
});
