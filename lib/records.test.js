'use strict';

const records = require('./records');

describe('lib/records', () => {
  const compression = 'LZ-UTF8';
  const { RecordsDecoder, getRecordsDecoder, getRecordsEncoder } = records;

  test('the module exports the expected', () => {
    expect(records).toEqual({
      RecordsDecoder: expect.any(Function),
      getRecordsDecoder: expect.any(Function),
      getRecordsEncoder: expect.any(Function)
    });
    expect(RecordsDecoder).toThrow('Class constructor');
  });

  test('RecordsDecoder is able to transform SubscribeToShardEvent messages', done => {
    const decoder = new RecordsDecoder({ compression });
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
  });

  test('RecordsDecoder emits event message types that are not SubscribeToShardEvent', done => {
    const decoder = new RecordsDecoder({ compression });
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

  test('RecordsDecoder emits exception messages as errors', done => {
    const decoder = new RecordsDecoder({ compression });
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

  test('RecordsDecoder emits errors if the message type is not an event', done => {
    const decoder = new RecordsDecoder({ compression });
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

  test('RecordsDecoder emits errors when failing to parse SubscribeToShardEvent', done => {
    const decoder = new RecordsDecoder({ compression });
    decoder.on('error', err => {
      expect(err.message).toBe('Unexpected token b in JSON at position 5');
      done();
    });
    decoder.on('data', done.fail);
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
  });

  test('getRecordsDecoder returns a function', () => {
    const decoder = getRecordsDecoder();
    expect(decoder).toEqual(expect.any(Function));
  });

  test('getRecordsDecoder returns a function that decompresses base64-based data', async () => {
    const decoder = getRecordsDecoder(compression, 'Base64');
    expect(decoder).toEqual(expect.any(Function));
    await expect(
      decoder({
        ApproximateArrivalTimestamp: 'baz',
        Data: 'eyJmb28iOiJiYXIifQ==',
        EncryptionType: 'qux',
        PartitionKey: 'quux',
        SequenceNumber: 'quuz'
      })
    ).resolves.toEqual({
      approximateArrivalTimestamp: 'baz',
      data: { foo: 'bar' },
      encryptionType: 'qux',
      partitionKey: 'quux',
      sequenceNumber: 'quuz'
    });
  });

  test('getRecordsDecoder returns a function that parses base64-based data', async () => {
    const decoder = getRecordsDecoder(null, 'Base64');
    expect(decoder).toEqual(expect.any(Function));
    await expect(
      decoder({
        ApproximateArrivalTimestamp: 'baz',
        Data: 'eyJmb28iOiJiYXIifQ==',
        EncryptionType: 'qux',
        PartitionKey: 'quux',
        SequenceNumber: 'quuz'
      })
    ).resolves.toEqual({
      approximateArrivalTimestamp: 'baz',
      data: { foo: 'bar' },
      encryptionType: 'qux',
      partitionKey: 'quux',
      sequenceNumber: 'quuz'
    });
  });

  test('getRecordsDecoder returns a function that parses JSON', async () => {
    const decoder = getRecordsDecoder();
    expect(decoder).toEqual(expect.any(Function));
    await expect(
      decoder({
        ApproximateArrivalTimestamp: 'baz',
        Data: JSON.stringify({ foo: 'bar' }),
        EncryptionType: 'qux',
        PartitionKey: 'quux',
        SequenceNumber: 'quuz'
      })
    ).resolves.toEqual({
      approximateArrivalTimestamp: 'baz',
      data: { foo: 'bar' },
      encryptionType: 'qux',
      partitionKey: 'quux',
      sequenceNumber: 'quuz'
    });
  });

  test('getRecordsDecoder returns a function that parses strings', async () => {
    const decoder = getRecordsDecoder();
    expect(decoder).toEqual(expect.any(Function));
    await expect(
      decoder({
        ApproximateArrivalTimestamp: 'baz',
        Data: 'foo',
        EncryptionType: 'qux',
        PartitionKey: 'quux',
        SequenceNumber: 'quuz'
      })
    ).resolves.toEqual({
      approximateArrivalTimestamp: 'baz',
      data: 'foo',
      encryptionType: 'qux',
      partitionKey: 'quux',
      sequenceNumber: 'quuz'
    });
  });

  test('getRecordsEncoder returns a function', () => {
    const encoder = getRecordsEncoder();
    expect(encoder).toEqual(expect.any(Function));
  });

  test('getRecordsEncoder returns a function that throws if not given data', async () => {
    const encoder = getRecordsEncoder();
    await expect(encoder({})).rejects.toThrow('The "data" property is required.');
  });

  test('getRecordsEncoder returns a function that compresses objects', async () => {
    const encoder = getRecordsEncoder(compression, 'Base64');
    await expect(
      encoder({
        data: { foo: 'bar' },
        explicitHashKey: 'baz',
        partitionKey: 'qux',
        sequenceNumberForOrdering: 'quux'
      })
    ).resolves.toEqual({
      Data: 'eyJmb28iOiJiYXIifQ==',
      ExplicitHashKey: 'baz',
      PartitionKey: 'qux',
      SequenceNumberForOrdering: 'quux'
    });
  });

  test('getRecordsEncoder returns a function that compresses strings', async () => {
    const encoder = getRecordsEncoder(compression, 'Base64');
    await expect(
      encoder({
        data: 'foo',
        partitionKey: 'bar'
      })
    ).resolves.toEqual({
      Data: 'Zm9v',
      PartitionKey: 'bar'
    });
  });

  test('getRecordsEncoder returns a function that assigns partition keys if skipped', async () => {
    const encoder = getRecordsEncoder(compression, 'Base64');
    await expect(encoder({ data: 'foo' })).resolves.toEqual({
      Data: 'Zm9v',
      PartitionKey: 'lnHUAlX30ntLtTZjZJH4Td1rkOA='
    });
  });

  test('getRecordsEncoder returns a function that parses strings', async () => {
    const encoder = getRecordsEncoder();
    await expect(encoder({ data: 'foo' })).resolves.toEqual({
      Data: 'foo',
      PartitionKey: 'C+7Hteo/D9vJXQ3UfzxbwnXaijM='
    });
  });
});
