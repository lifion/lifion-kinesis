'use strict';

const { Transform } = require('stream');
const compressionLibs = require('./compression');
const { isJson } = require('./utils');

const privateData = new WeakMap();

function internal(instance) {
  if (!privateData.has(instance)) privateData.set(instance, {});
  return privateData.get(instance);
}

class RecordsDecoder extends Transform {
  constructor(options) {
    super({ objectMode: true });
    const { compression } = options;
    const compressionLib = compression && compressionLibs[compression];
    Object.assign(internal(this), { ...options, compressionLib });
  }

  async _transform({ headers, payload }, encoding, callback) {
    const { compressionLib, logger, shard } = internal(this);
    const msgType = headers[':message-type'];
    const eventType = headers[':event-type'];
    const { ShardId: shardId } = shard;

    if (msgType !== 'event') {
      this.emit('error', new Error(`Unknown event stream message type "${msgType}".`));
      return;
    }

    if (eventType === 'SubscribeToShardEvent') {
      try {
        const {
          ContinuationSequenceNumber: continuationSequenceNumber,
          MillisBehindLatest: millisBehindLatest,
          Records
        } = payload;
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
            if (compressionLib) data = await compressionLib.decompress(Data);
            else data = Buffer.from(Data, 'base64').toString('utf8');
            if (isJson(data)) data = JSON.parse(data);
            return {
              approximateArrivalTimestamp,
              data,
              encryptionType,
              partitionKey,
              sequenceNumber
            };
          })
        );
        this.push({ continuationSequenceNumber, millisBehindLatest, records, shardId });
        callback();
      } catch (err) {
        this.emit('error', err);
      }
      return;
    }

    logger.debug(`Event "${eventType}" emitted.`);
    this.emit(eventType, payload);
    callback();
  }
}

module.exports = RecordsDecoder;
