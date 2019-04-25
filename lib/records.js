'use strict';

const { Transform } = require('stream');
const { createHash } = require('crypto');

const compressionLibs = require('./compression');

const IS_JSON_REGEX = /^[{[].*[}\]]$/;

const privateData = new WeakMap();

function internal(instance) {
  if (!privateData.has(instance)) privateData.set(instance, {});
  return privateData.get(instance);
}

function hash(buffer) {
  return createHash('sha1')
    .update(buffer)
    .digest('base64');
}

function getRecordsDecoder(compression, inputEncoding) {
  const compressionLib = compression && compressionLibs[compression];

  return async record => {
    const {
      ApproximateArrivalTimestamp: approximateArrivalTimestamp,
      EncryptionType: encryptionType,
      PartitionKey: partitionKey,
      SequenceNumber: sequenceNumber
    } = record;

    let data = record.Data;

    if (compressionLib) {
      data = await compressionLib.decompress(data, inputEncoding);
    } else if (inputEncoding === 'Base64') {
      data = Buffer.from(data, 'base64').toString('utf8');
    }

    if (IS_JSON_REGEX.test(data)) {
      data = JSON.parse(data);
    }

    return {
      approximateArrivalTimestamp,
      data,
      encryptionType,
      partitionKey,
      sequenceNumber
    };
  };
}

function getRecordsEncoder(compression, outputEncoding) {
  const compressionLib = compression && compressionLibs[compression];

  return async record => {
    const { data, explicitHashKey, partitionKey, sequenceNumberForOrdering } = record;
    if (data === undefined) throw new TypeError('The "data" property is required.');
    let normData = typeof data !== 'string' ? JSON.stringify(data) : data;
    if (compressionLib) normData = await compressionLib.compress(normData, outputEncoding);
    return Object.assign(
      { Data: normData, PartitionKey: partitionKey || hash(normData) },
      explicitHashKey && { ExplicitHashKey: explicitHashKey },
      sequenceNumberForOrdering && { SequenceNumberForOrdering: sequenceNumberForOrdering }
    );
  };
}

class RecordsDecoder extends Transform {
  constructor({ compression }) {
    super({ objectMode: true });
    Object.assign(internal(this), {
      recordsDecoder: getRecordsDecoder(compression, 'Base64')
    });
  }

  async _transform({ headers, payload }, encoding, callback) {
    const { recordsDecoder } = internal(this);
    const msgType = headers[':message-type'];
    const eventType = headers[':event-type'];

    if (msgType === 'exception') {
      const err = new Error(payload.message);
      err.code = headers[':exception-type'];
      this.emit('error', err);
      return;
    }

    if (msgType !== 'event') {
      this.emit('error', new Error(`Unknown event stream message type "${msgType}".`));
      return;
    }

    if (eventType === 'SubscribeToShardEvent') {
      try {
        const continuationSequenceNumber = payload.ContinuationSequenceNumber;
        const millisBehindLatest = payload.MillisBehindLatest;
        const records = await Promise.all(payload.Records.map(recordsDecoder));
        this.push({ continuationSequenceNumber, millisBehindLatest, records });
        callback();
      } catch (err) {
        this.emit('error', err);
      }
      return;
    }

    this.emit(eventType, payload);
    callback();
  }
}

module.exports = {
  getRecordsDecoder,
  getRecordsEncoder,
  RecordsDecoder
};
