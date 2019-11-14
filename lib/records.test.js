'use strict';

const Chance = require('chance');
const shortUuid = require('short-uuid');
const records = require('./records');

const chance = new Chance();

describe('lib/records', () => {
  const largeDoc = chance.paragraph({ sentences: 6000 });
  const mockLogger = { debug: jest.fn(), error: jest.fn(), warn: jest.fn() };
  const { RecordsDecoder, getRecordsDecoder, getRecordsEncoder } = records;

  const mockS3Client = {
    createBucket: jest.fn(),
    getBucketTagging: jest.fn(),
    getObject: jest.fn(),
    headBucket: jest.fn(),
    putBucketLifecycleConfiguration: jest.fn(),
    putBucketTagging: jest.fn(),
    putObject: jest.fn()
  };

  afterEach(() => {
    mockLogger.debug.mockClear();
    mockLogger.error.mockClear();
    mockLogger.warn.mockClear();
    mockS3Client.createBucket.mockClear();
    mockS3Client.getBucketTagging.mockClear();
    mockS3Client.getObject.mockClear();
    mockS3Client.headBucket.mockClear();
    mockS3Client.putBucketLifecycleConfiguration.mockClear();
    mockS3Client.putBucketTagging.mockClear();
    mockS3Client.putObject.mockClear();
  });

  test('the module exports the expected', () => {
    expect(records).toEqual({
      RecordsDecoder: expect.any(Function),
      getRecordsDecoder: expect.any(Function),
      getRecordsEncoder: expect.any(Function)
    });
    expect(RecordsDecoder).toThrow('Class constructor');
  });

  describe('RecordsDecoder', () => {
    const compression = 'LZ-UTF8';

    test('is able to transform SubscribeToShardEvent messages', () => {
      const decoder = new RecordsDecoder({
        compression,
        logger: mockLogger,
        shouldParseJson: true
      });
      return new Promise((resolve, reject) => {
        decoder.on('error', reject);
        decoder.on('data', data => {
          expect(data).toEqual({
            continuationSequenceNumber: 'foo',
            millisBehindLatest: 'bar',
            records: [
              {
                approximateArrivalTimestamp: 'baz',
                data: { foo: 'bar' },
                encryptionType: 'qux',
                partitionKey: 'quux',
                sequenceNumber: 'quuz'
              }
            ]
          });
          resolve();
        });
        decoder.write({
          headers: {
            ':event-type': 'SubscribeToShardEvent',
            ':message-type': 'event'
          },
          payload: {
            ContinuationSequenceNumber: 'foo',
            MillisBehindLatest: 'bar',
            Records: [
              {
                ApproximateArrivalTimestamp: 'baz',
                Data: 'eyJmb28iOiJiYXIifQ==',
                EncryptionType: 'qux',
                PartitionKey: 'quux',
                SequenceNumber: 'quuz'
              }
            ]
          }
        });
      }).finally(() => {
        decoder.destroy();
      });
    });

    test('emits event message types that are not SubscribeToShardEvent', () => {
      const decoder = new RecordsDecoder({ compression, logger: mockLogger });
      return new Promise((resolve, reject) => {
        decoder.on('error', reject);
        decoder.on('data', reject);
        decoder.on('foo', data => {
          expect(data).toEqual({ bar: 'baz' });
          resolve();
        });
        decoder.write({
          headers: {
            ':event-type': 'foo',
            ':message-type': 'event'
          },
          payload: { bar: 'baz' }
        });
      }).finally(() => {
        decoder.destroy();
      });
    });

    test('emits exception messages as errors', () => {
      const decoder = new RecordsDecoder({ compression, logger: mockLogger });
      return new Promise((resolve, reject) => {
        decoder.on('error', err => {
          const { code, message } = err;
          expect(code).toBe('foo');
          expect(message).toBe('bar');
          resolve();
        });
        decoder.on('data', reject);
        decoder.write({
          headers: {
            ':exception-type': 'foo',
            ':message-type': 'exception'
          },
          payload: { message: 'bar' }
        });
      }).finally(() => {
        decoder.destroy();
      });
    });

    test('emits errors if the message type is not an event', () => {
      const decoder = new RecordsDecoder({ compression, logger: mockLogger });
      return new Promise((resolve, reject) => {
        decoder.on('error', err => {
          expect(err.message).toBe('Unknown event stream message type "foo".');
          resolve();
        });
        decoder.on('data', reject);
        decoder.write({
          headers: {
            ':message-type': 'foo'
          }
        });
      }).finally(() => {
        decoder.destroy();
      });
    });

    test('emits errors when failing to parse SubscribeToShardEvent', () => {
      const decoder = new RecordsDecoder({
        compression,
        logger: mockLogger,
        shouldParseJson: 'auto'
      });
      return new Promise((resolve, reject) => {
        decoder.on('error', reject);
        decoder.on('data', data => {
          expect(data).toMatchObject({
            records: [{ data: '{"fn"bar"}' }]
          });
          expect(mockLogger.warn).toHaveBeenCalled();
          resolve();
        });
        decoder.write({
          headers: {
            ':event-type': 'SubscribeToShardEvent',
            ':message-type': 'event'
          },
          payload: {
            Records: [{ Data: 'eyJmbiJiYXIifQ==' }]
          }
        });
      }).finally(() => {
        decoder.destroy();
      });
    });
  });

  describe('getRecordsDecoder', () => {
    test('returns a function', () => {
      const decoder = getRecordsDecoder();
      expect(decoder).toEqual(expect.any(Function));
    });

    test('returns a function that decodes compressed base64-encoded data', async () => {
      const decoder = getRecordsDecoder({ compression: 'LZ-UTF8', inputEncoding: 'Base64' });
      await expect(decoder({ Data: 'Zm9vLCBiYXIsIMgK' })).resolves.toEqual({
        data: 'foo, bar, foo, bar'
      });
    });

    test('returns a function that decodes base-64 encoded data', async () => {
      const decoder = getRecordsDecoder({ inputEncoding: 'Base64' });
      await expect(decoder({ Data: 'eyJmb28iOiJiYXIifQ==' })).resolves.toEqual({
        data: { foo: 'bar' }
      });
    });

    test('returns a function that parses JSON', async () => {
      const decoder = getRecordsDecoder({ shouldParseJson: true });
      await expect(decoder({ Data: '{"foo":"bar"}' })).resolves.toEqual({ data: { foo: 'bar' } });
    });

    test('returns a function that parses if data is JSON-like', async () => {
      const decoder = getRecordsDecoder({ shouldParseJson: 'auto' });
      await expect(decoder({ Data: '{"foo":"bar"}' })).resolves.toEqual({ data: { foo: 'bar' } });
    });

    test('returns a function that will not parse JSON if the parsing flag is off', async () => {
      const decoder = getRecordsDecoder({ shouldParseJson: false });
      await expect(decoder({ Data: '{"foo":"bar"}' })).resolves.toEqual({ data: '{"foo":"bar"}' });
    });

    test('returns a function that will not try to parse if data is not JSON-like', async () => {
      const decoder = getRecordsDecoder({ shouldParseJson: 'auto' });
      await expect(decoder({ Data: 'foo' })).resolves.toEqual({ data: 'foo' });
      expect(mockLogger.warn).not.toHaveBeenCalled();
    });

    test('returns a function that returns the original data when parsing JSON fails', async () => {
      const decoder = getRecordsDecoder({ logger: mockLogger, shouldParseJson: true });
      await expect(decoder({ Data: 'foo' })).resolves.toEqual({ data: 'foo' });
      expect(mockLogger.warn).toHaveBeenCalledTimes(1);
      expect(mockLogger.warn).toHaveBeenCalledWith('Could not decode record:', expect.any(Error));
    });

    test('returns a function that will not throw when parsing JSON fails', async () => {
      const decoder = getRecordsDecoder({ shouldParseJson: true });
      await expect(decoder({ Data: 'foo' })).resolves.toEqual({ data: 'foo' });
    });

    test('returns a function that decodes a message passed through s3', async () => {
      mockS3Client.getObject.mockResolvedValue({
        Body: Buffer.from(`{"data":"${largeDoc}"}`),
        ContentType: 'application/json'
      });
      const decoder = getRecordsDecoder({
        compression: null,
        inputEncoding: null,
        s3Client: mockS3Client,
        shouldParseJson: true,
        useS3ForLargeItems: true
      });
      expect(decoder).toEqual(expect.any(Function));
      await expect(
        decoder({
          ApproximateArrivalTimestamp: 'baz',
          Data: '{"@S3Item":{"bucket":"test bucket name","Body":"test body"}}',
          EncryptionType: 'qux',
          PartitionKey: 'quux',
          SequenceNumber: 'quuz'
        })
      ).resolves.toEqual({
        approximateArrivalTimestamp: 'baz',
        data: { data: largeDoc },
        encryptionType: 'qux',
        partitionKey: 'quux',
        sequenceNumber: 'quuz'
      });
    });

    test('returns a function that decodes a message passed hrough s3', async () => {
      mockS3Client.getObject.mockResolvedValue({
        Body: Buffer.from('test body'),
        ContentType: 'octet/stream'
      });
      const decoder = getRecordsDecoder({
        compression: null,
        inputEncoding: null,
        s3Client: mockS3Client,
        shouldParseJson: 'auto',
        useS3ForLargeItems: true
      });
      expect(decoder).toEqual(expect.any(Function));
      await expect(
        decoder({
          ApproximateArrivalTimestamp: 'baz',
          Data: '{"@S3Item":{"bucket":"test bucket name","Body":"test body"}}',
          EncryptionType: 'qux',
          PartitionKey: 'quux',
          SequenceNumber: 'quuz'
        })
      ).resolves.toEqual({
        approximateArrivalTimestamp: 'baz',
        data: 'test body',
        encryptionType: 'qux',
        partitionKey: 'quux',
        sequenceNumber: 'quuz'
      });
    });

    test('returns a function that parses JSON objects sent over S3', async () => {
      mockS3Client.getObject.mockResolvedValue({ Body: 'test body' });
      const decoder = getRecordsDecoder({
        compression: null,
        inputEncoding: null,
        s3Client: mockS3Client,
        shouldParseJson: 'auto',
        useS3ForLargeItems: true
      });
      expect(decoder).toEqual(expect.any(Function));
      await expect(
        decoder({
          ApproximateArrivalTimestamp: 'baz',
          Data: '{"@S3Item":{"bucket":"test bucket name","Body":"test body"}}',
          EncryptionType: 'qux',
          PartitionKey: 'quux',
          SequenceNumber: 'quuz'
        })
      ).resolves.toEqual({
        approximateArrivalTimestamp: 'baz',
        data: 'test body',
        encryptionType: 'qux',
        partitionKey: 'quux',
        sequenceNumber: 'quuz'
      });
    });
  });

  describe('getRecordsEncoder', () => {
    beforeEach(() => {
      shortUuid.resetMockCounter();
    });

    test('returns a function', () => {
      const encoder = getRecordsEncoder({});
      expect(encoder).toEqual(expect.any(Function));
    });

    test('returns a function that throws if not given data', async () => {
      const encoder = getRecordsEncoder();
      await expect(encoder({})).rejects.toThrow('The "data" property is required.');
    });

    test('returns a function that compresses data', async () => {
      const encoder = getRecordsEncoder({ compression: 'LZ-UTF8', outputEncoding: 'Base64' });
      await expect(
        encoder({
          data: 'foo, bar, foo, bar',
          explicitHashKey: 'qux',
          partitionKey: 'quux',
          sequenceNumberForOrdering: 'quuz'
        })
      ).resolves.toEqual({
        Data: 'Zm9vLCBiYXIsIMgK',
        ExplicitHashKey: 'qux',
        PartitionKey: 'quux',
        SequenceNumberForOrdering: 'quuz'
      });
    });

    test('returns a function that compresses data and generates partition keys', async () => {
      const encoder = getRecordsEncoder({ compression: 'LZ-UTF8', outputEncoding: 'Base64' });
      await expect(encoder({ data: 'foo, bar, foo, bar' })).resolves.toEqual({
        Data: 'Zm9vLCBiYXIsIMgK',
        PartitionKey: 'Qk2rZuty0pO/vptdjx3KZ2hUqVM='
      });
    });

    test('returns a function that compresses objects', async () => {
      const encoder = getRecordsEncoder({ compression: 'LZ-UTF8', outputEncoding: 'Base64' });
      await expect(encoder({ data: { foo: 'bar, baz, bar, baz' } })).resolves.toEqual({
        Data: 'eyJmb28iOiJiYXIsIGJhesQFxgoifQ==',
        PartitionKey: 'n0vR6WyiMI1VeNqoISbuIEPoMPM='
      });
    });

    test('returns a function that offloads large messages to S3', async () => {
      mockS3Client.putObject.mockResolvedValueOnce({ ETag: shortUuid.generate() });
      const encoder = getRecordsEncoder({
        s3: { largeItemThreshold: 500 },
        s3Client: mockS3Client,
        streamName: 'core--test-1',
        useS3ForLargeItems: true
      });
      await expect(encoder({ data: largeDoc })).resolves.toEqual({
        Data: JSON.stringify({ '@S3Item': { eTag: '0000', key: 'core--test-1-0001.json' } }),
        PartitionKey: 'hoKe98qHWTKVJg+g8IEsdvvnrLI='
      });
    });

    test('returns a function that offloads large objects to S3 with selective keys', async () => {
      mockS3Client.putObject.mockResolvedValueOnce({ ETag: shortUuid.generate() });
      const encoder = getRecordsEncoder({
        s3: { largeItemThreshold: 500, nonS3Keys: ['bar', 'quux'] },
        s3Client: mockS3Client,
        streamName: 'core--test-1',
        useS3ForLargeItems: true
      });
      await expect(
        encoder({
          data: { bar: 'baz', foo: largeDoc, grault: 'garply', quux: 'corge', qux: 'quux' }
        })
      ).resolves.toEqual({
        Data: JSON.stringify({
          '@S3Item': { eTag: '0000', key: 'core--test-1-0001.json' },
          bar: 'baz',
          quux: 'corge'
        }),
        PartitionKey: 'uM9dgBWF4OGL42Uqbr61Yyt5h58='
      });
    });
  });
});
