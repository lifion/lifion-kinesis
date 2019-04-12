'use strict';

const compressionLibs = require('./compression');
const { isJson } = require('./utils');

const privateData = new WeakMap();

function internal(instance) {
  if (!privateData.has(instance)) privateData.set(instance, {});
  return privateData.get(instance);
}

async function poll(instance) {
  const privateProps = internal(instance);

  const { client, compressionLib, pushToStream, streamName, shardId } = privateProps;
  let { iterator } = privateProps;

  if (!iterator) {
    const params = {
      ShardId: shardId,
      ShardIteratorType: 'LATEST',
      StreamName: streamName
    };
    const { ShardIterator } = await client.getShardIterator(params);
    iterator = ShardIterator;
  }

  const params = { ShardIterator: iterator, Limit: 10000 };
  const { Records, NextShardIterator, MillisBehindLatest } = await client.getRecords(params);

  const records = await Promise.all(
    Records.map(async record => {
      const {
        ApproximateArrivalTimestamp: approximateArrivalTimestamp,
        Data,
        EncryptionType: encryptionType,
        PartitionKey: partitionKey,
        SequenceNumber: sequenceNumber
      } = record;

      let data;
      if (compressionLib) data = await compressionLib.decompress(Data, 'Buffer');
      else data = Buffer.from(Data, 'base64').toString('utf8');
      if (isJson(data)) data = JSON.parse(data);

      return { approximateArrivalTimestamp, data, encryptionType, partitionKey, sequenceNumber };
    })
  );

  privateProps.iterator = NextShardIterator;

  pushToStream({ shardId, records, millisBehindLatest: MillisBehindLatest });

  privateProps.timeoutId = setTimeout(poll, MillisBehindLatest === 0 ? 5000 : 250, instance);
}

class PollingConsumer {
  constructor(options) {
    const {
      checkpoint,
      client,
      compression,
      hasChildren,
      pushToStream,
      logger,
      shardId,
      streamName,
      version
    } = options;

    Object.assign(internal(this), {
      checkpoint,
      client,
      compressionLib: compression && compressionLibs[compression],
      hasChildren,
      logger,
      shardId,
      pushToStream,
      streamName,
      timeoutId: null,
      version
    });
  }

  start() {
    poll(this);
  }

  stop() {
    const privateProps = internal(this);
    clearTimeout(privateProps.timeoutId);
    privateProps.timeoutId = null;
  }
}

module.exports = PollingConsumer;
