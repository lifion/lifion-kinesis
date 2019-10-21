'use strict';

const Chance = require('chance');
const shortUuid = require('short-uuid');
const records = require('./records');

const chance = new Chance();

describe('lib/records', () => {
  const largeDoc = chance.paragraph({ sentences: 20000 });
  const mockLogger = { debug: jest.fn(), error: jest.fn(), warn: jest.fn() };
  const mockS3Client = {
    createBucket: jest.fn(),
    getBucketTagging: jest.fn(),
    getObject: jest.fn(),
    headBucket: jest.fn(),
    putBucketLifecycleConfiguration: jest.fn(),
    putBucketTagging: jest.fn(),
    putObject: jest.fn()
  };
  const { RecordsDecoder, getRecordsDecoder, getRecordsEncoder } = records;

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
    test('is able to transform SubscribeToShardEvent messages', async done => {
      const decoder = new RecordsDecoder({
        compression,
        logger: mockLogger,
        shouldParseJson: true
      });

      setTimeout(() => {
        decoder.on('error', done.fail);
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
          decoder.destroy();
          done();
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
      }, 100);
    });

    test('emits event message types that are not SubscribeToShardEvent', async done => {
      const decoder = new RecordsDecoder({ compression, logger: mockLogger });
      decoder.on('error', done.fail);
      decoder.on('data', done.fail);
      decoder.on('foo', data => {
        expect(data).toEqual({ bar: 'baz' });
        done();
      });
      decoder.write({
        headers: {
          ':event-type': 'foo',
          ':message-type': 'event'
        },
        payload: { bar: 'baz' }
      });
    });

    test('emits exception messages as errors', async done => {
      const decoder = new RecordsDecoder({ compression, logger: mockLogger });
      decoder.on('error', err => {
        const { code, message } = err;
        expect(code).toBe('foo');
        expect(message).toBe('bar');
        done();
      });
      decoder.on('data', done.fail);
      decoder.write({
        headers: {
          ':exception-type': 'foo',
          ':message-type': 'exception'
        },
        payload: { message: 'bar' }
      });
    });

    test('emits errors if the message type is not an event', async done => {
      const decoder = new RecordsDecoder({ compression, logger: mockLogger });
      decoder.on('error', err => {
        expect(err.message).toBe('Unknown event stream message type "foo".');
        done();
      });
      decoder.on('data', done.fail);
      decoder.write({
        headers: {
          ':message-type': 'foo'
        }
      });
    });

    test('emits errors when failing to parse SubscribeToShardEvent', async done => {
      const decoder = new RecordsDecoder({
        compression,
        logger: mockLogger,
        shouldParseJson: 'auto'
      });

      setTimeout(() => {
        decoder.on('error', done.fail);
        decoder.on('data', data => {
          expect(data).toMatchObject({
            records: [
              {
                data: '{"fn"bar"}'
              }
            ]
          });
          expect(mockLogger.warn).toHaveBeenCalled();
          done();
        });
        decoder.write({
          headers: {
            ':event-type': 'SubscribeToShardEvent',
            ':message-type': 'event'
          },
          payload: {
            Records: [
              {
                Data: 'eyJmbiJiYXIifQ=='
              }
            ]
          }
        });
      }, 100);
    });
  });

  describe('getRecordsDecoder', () => {
    test('returns a function', async () => {
      const decoder = getRecordsDecoder({});
      expect(decoder).toEqual(expect.any(Function));
    });

    [
      {
        compression: 'LZ-UTF8',
        expectedResult: {
          approximateArrivalTimestamp: 'baz',
          data: { foo: 'bar' },
          encryptionType: 'qux',
          partitionKey: 'quux',
          sequenceNumber: 'quuz'
        },
        inputEncoding: 'Base64',
        recordToDecode: {
          ApproximateArrivalTimestamp: 'baz',
          Data: 'eyJmb28iOiJiYXIifQ==',
          EncryptionType: 'qux',
          PartitionKey: 'quux',
          SequenceNumber: 'quuz'
        },
        shouldParseJson: 'auto',
        title: 'decompresses base64-based data'
      },
      {
        compression: null,
        expectedResult: {
          approximateArrivalTimestamp: 'baz',
          data: { foo: 'bar' },
          encryptionType: 'qux',
          partitionKey: 'quux',
          sequenceNumber: 'quuz'
        },
        inputEncoding: 'Base64',
        recordToDecode: {
          ApproximateArrivalTimestamp: 'baz',
          Data: 'eyJmb28iOiJiYXIifQ==',
          EncryptionType: 'qux',
          PartitionKey: 'quux',
          SequenceNumber: 'quuz'
        },
        shouldParseJson: 'auto',
        title: 'parses base64-based data'
      },
      {
        expectedResult: {
          approximateArrivalTimestamp: 'baz',
          data: { foo: 'bar' },
          encryptionType: 'qux',
          partitionKey: 'quux',
          sequenceNumber: 'quuz'
        },
        recordToDecode: {
          ApproximateArrivalTimestamp: 'baz',
          Data: JSON.stringify({ foo: 'bar' }),
          EncryptionType: 'qux',
          PartitionKey: 'quux',
          SequenceNumber: 'quuz'
        },
        shouldParseJson: true,
        title: 'parses JSON'
      },
      {
        expectedResult: {
          approximateArrivalTimestamp: 'baz',
          data: 'foo',
          encryptionType: 'qux',
          partitionKey: 'quux',
          sequenceNumber: 'quuz'
        },

        recordToDecode: {
          ApproximateArrivalTimestamp: 'baz',
          Data: 'foo',
          EncryptionType: 'qux',
          PartitionKey: 'quux',
          SequenceNumber: 'quuz'
        },
        shouldParseJson: false,
        title: 'parses strings'
      },
      {
        expectedResult: {
          approximateArrivalTimestamp: 'baz',
          data: 'foo',
          encryptionType: 'qux',
          partitionKey: 'quux',
          sequenceNumber: 'quuz'
        },

        recordToDecode: {
          ApproximateArrivalTimestamp: 'baz',
          Data: 'foo',
          EncryptionType: 'qux',
          PartitionKey: 'quux',
          SequenceNumber: 'quuz'
        },
        shouldParseJson: 'auto',
        title: 'parses strings'
      }
    ].forEach(
      ({ compression, expectedResult, inputEncoding, recordToDecode, shouldParseJson, title }) => {
        test(`returns a function that ${title}`, async () => {
          mockS3Client.getObject.mockResolvedValue(expectedResult.data);
          const decoder = getRecordsDecoder({
            compression,
            inputEncoding,
            mockLogger,
            s3Client: mockS3Client,
            shouldParseJson
          });

          expect(decoder).toEqual(expect.any(Function));
          await expect(decoder(recordToDecode)).resolves.toEqual(expectedResult);
        });
      }
    );

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

      const promise = decoder({
        ApproximateArrivalTimestamp: 'baz',
        Data: '{"@S3Item":{"bucket":"test bucket name","Body":"test body"}}',
        EncryptionType: 'qux',
        PartitionKey: 'quux',
        SequenceNumber: 'quuz'
      });

      await expect(promise).resolves.toEqual({
        approximateArrivalTimestamp: 'baz',
        data: {
          data: largeDoc
        },
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

    test('returns a function', async () => {
      const encoder = getRecordsEncoder({});
      expect(encoder).toEqual(expect.any(Function));
    });

    test('returns a function that throws if not given data', async () => {
      const encoder = getRecordsEncoder({});
      await expect(encoder({})).rejects.toThrow('The "data" property is required.');
    });

    [
      {
        compression: 'LZ-UTF8',
        expectedResult: {
          Data: 'eyJmb28iOiJiYXIifQ==',
          ExplicitHashKey: 'baz',
          PartitionKey: 'qux',
          SequenceNumberForOrdering: 'quux'
        },
        outputEncoding: 'Base 64',
        recordToEncode: {
          data: { foo: 'bar' },
          explicitHashKey: 'baz',
          partitionKey: 'qux',
          sequenceNumberForOrdering: 'quux'
        },
        s3: {
          nonS3Keys: []
        },
        title: 'compresses objects'
      },
      {
        compression: 'LZ-UTF8',
        expectedResult: {
          Data: 'Zm9v',
          PartitionKey: 'bar'
        },
        outputEncoding: 'Base 64',
        recordToEncode: {
          data: 'foo',
          partitionKey: 'bar'
        },
        s3: {
          nonS3Keys: []
        },
        title: 'compresses strings'
      },
      {
        compression: 'LZ-UTF8',
        expectedResult: {
          Data: 'Zm9v',
          PartitionKey: 'lnHUAlX30ntLtTZjZJH4Td1rkOA='
        },
        outputEncoding: 'Base 64',
        recordToEncode: { data: 'foo' },
        s3: {
          nonS3Keys: []
        },
        title: 'assigns partition keys if skipped'
      },
      {
        expectedResult: {
          Data: 'foo',
          PartitionKey: 'C+7Hteo/D9vJXQ3UfzxbwnXaijM='
        },
        recordToEncode: { data: 'foo' },
        s3: {
          nonS3Keys: []
        },
        title: 'parses strings'
      },
      {
        expectedResult: {
          Data: '{"@S3Item":{"eTag":"","key":"test-stream-name-0000.json"}}',
          PartitionKey: expect.any(String)
        },
        recordToEncode: { data: `{"some key":"${largeDoc}"}` },
        s3: {
          largeItemThreshold: 900,
          nonS3Keys: []
        },
        title: 'encodes a message to pass through s3',
        useS3ForLargeItems: true
      },
      {
        expectedResult: {
          Data: '{"@S3Item":{"eTag":"","key":"test-stream-name-0000.json"},"someKey":"some value"}',
          PartitionKey: expect.any(String)
        },
        recordToEncode: { data: { largeDoc, someKey: 'some value' } },
        s3: {
          largeItemThreshold: 900,
          nonS3Keys: ['someKey']
        },
        title: 'encodes a message to pass through s3 and keeps the non s3 keys on the record',
        useS3ForLargeItems: true
      }
    ].forEach(({ compression, expectedResult, recordToEncode, s3, title, useS3ForLargeItems }) => {
      test(`returns a function that ${title}`, async () => {
        mockS3Client.putObject.mockResolvedValue({ ETag: '', response: 's3 put response' });
        const encoder = getRecordsEncoder({
          compression,
          outputEncoding: 'Base64',
          s3,
          s3Client: mockS3Client,
          streamName: 'test-stream-name',
          useS3ForLargeItems
        });

        await expect(encoder(recordToEncode)).resolves.toEqual(expectedResult);
      });
    });
  });
});
