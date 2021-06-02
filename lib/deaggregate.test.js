'use strict';

const crypto = require('crypto');
const protobuf = require('protobufjs');

const deaggregate = require('./deaggregate');
const aggJson = require('./aggregate-protobuf.json');

const KPL_MAGIC_NUMBER = 'f3899ac2';
const builder = protobuf.Root.fromJSON(aggJson).lookupType('AggregatedRecord');

describe('lib/deaggregate', () => {
  let message;
  let messages;
  let content;

  beforeEach(() => {
    messages = [
      {
        data: Buffer.from('message1'),
        explicit_hash_key_index: 0,
        partition_key_index: 0,
        tags: []
      },
      {
        data: Buffer.from('message2'),
        explicit_hash_key_index: 1,
        partition_key_index: 1,
        tags: []
      }
    ];

    message = {
      explicit_hash_key_table: ['hash1', 'hash2'],
      partition_key_table: ['key1', 'key2'],
      records: messages
    };
    const buff = builder.encode(message).finish();

    const md5 = crypto.createHash('md5');
    md5.update(buff);
    const checksum = md5.digest();
    content = {
      ApproximateArrivalTimestamp: 400000,
      Data: Buffer.concat([Buffer.from(KPL_MAGIC_NUMBER, 'hex'), buff, checksum]),
      SequenceNumber: 0
    };
  });

  test('the message is deaggregated into multiple results', async () => {
    const result = await deaggregate([content]);
    expect(result.length).toEqual(2);
    expect(result[0].ExplicitPartitionKey).toEqual('hash1');
    expect(result[0].PartitionKey).toEqual('key1');
    expect(result[0].ApproximateArrivalTimestamp).toEqual(400000);
    expect(Buffer.from(result[0].Data).toString()).toEqual('message1');
    expect(result[1].ExplicitPartitionKey).toEqual('hash2');
    expect(result[1].ApproximateArrivalTimestamp).toEqual(400000);
    expect(Buffer.from(result[1].Data).toString()).toEqual('message2');
  });

  test('multipled records passed are each deaggregated', async () => {
    const result = await deaggregate([content, content]);
    expect(result.length).toEqual(4);
    expect(result[0].ExplicitPartitionKey).toEqual('hash1');
    expect(result[0].PartitionKey).toEqual('key1');
    expect(Buffer.from(result[0].Data).toString()).toEqual('message1');
    expect(result[1].ExplicitPartitionKey).toEqual('hash2');
    expect(Buffer.from(result[1].Data).toString()).toEqual('message2');
    expect(result[2].ExplicitPartitionKey).toEqual('hash1');
    expect(result[2].PartitionKey).toEqual('key1');
    expect(Buffer.from(result[2].Data).toString()).toEqual('message1');
    expect(result[3].ExplicitPartitionKey).toEqual('hash2');
    expect(Buffer.from(result[3].Data).toString()).toEqual('message2');
  });

  test('null values are handled properly', async () => {
    const result = await deaggregate(null);
    expect(result).toEqual(null);
  });

  test('case where the records are not aggregated', async () => {
    const result = await deaggregate([
      {
        Data: Buffer.from('deaggregate message'),
        ExplicitPartitionKey: 'expKey',
        PartitionKey: 'partKey',
        SequenceNumber: 1
      }
    ]);
    expect(result[0].ExplicitPartitionKey).toEqual('expKey');
    expect(Buffer.from(result[0].Data).toString()).toEqual('deaggregate message');
  });
});
