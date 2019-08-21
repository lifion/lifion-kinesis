/**
 * Module that contains a collection of classes and functions to encode and decode Kinesis records.
 *
 * @module records
 * @private
 */

'use strict';

const { Transform } = require('stream');
const { createHash } = require('crypto');

const compressionLibs = require('./compression');

const IS_JSON_REGEX = /^[{[].*[}\]]$/;

const privateData = new WeakMap();

/**
 * Provides access to the private data of the specified instance.
 *
 * @param {object} instance - The private data's owner.
 * @returns {object} The private data.
 * @private
 */
function internal(instance) {
  if (!privateData.has(instance)) privateData.set(instance, {});
  return privateData.get(instance);
}

/**
 * Hashes the given buffer into a SHA1-Base64 string.
 *
 * @param {Buffer} buffer - The buffer of bytes to hash.
 * @returns {string} A string with the hash of the buffer.
 * @private
 */
function hash(buffer) {
  return createHash('sha1')
    .update(buffer)
    .digest('base64');
}

/**
 * Returns a function that decodes Kinesis records as they are retrieved from AWS.Kinesis into
 * native objects. The decoder will also decompress the record data as instructed.
 *
 * @param {string} compression - The kind of compression used in the Kinesis record data.
 * @param {string} inputEncoding - The encoding of the `Data` property in the AWS.Kinesis record.
 * @returns {Function} A function that decodes `record` objects from AWS.Kinesis.
 * @memberof module:records
 */
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

/**
 * Returns a function that encodes native objects into records that can be sent to AWS.Kinesis.
 * The encoder will also compress the record data using the specified compression library.
 *
 * @param {string} compression - The kind of compression used for the Kinesis record data.
 * @param {string} outputEncoding - The encoding for the resulting `Data` property.
 * @returns {Function} A function that encodes objects into the format expected by AWS.Kinesis.
 * @memberof module:records
 */
function getRecordsEncoder(compression, outputEncoding) {
  const compressionLib = compression && compressionLibs[compression];

  return async record => {
    const { data, explicitHashKey, partitionKey, sequenceNumberForOrdering } = record;
    if (data === undefined) throw new TypeError('The "data" property is required.');
    let normData = typeof data !== 'string' ? JSON.stringify(data) : data;
    if (compressionLib) normData = await compressionLib.compress(normData, outputEncoding);
    return {
      Data: normData,
      PartitionKey: partitionKey || hash(normData),
      ...(explicitHashKey && { ExplicitHashKey: explicitHashKey }),
      ...(sequenceNumberForOrdering && { SequenceNumberForOrdering: sequenceNumberForOrdering })
    };
  };
}

/**
 * Implements a transform stream that would decode and decompress records in AWS.Kinesis format as
 * they arrive to the stream. The records are transformed into native objects.
 *
 * @augments external:Transform
 * @memberof module:records
 */
class RecordsDecoder extends Transform {
  /**
   * Initializes the decoder stream.
   *
   * @param {object} options - The initialization options.
   * @param {string} options.compression - The kind of compression to use in records data.
   */
  constructor({ compression }) {
    super({ objectMode: true });
    Object.assign(internal(this), {
      recordsDecoder: getRecordsDecoder(compression, 'Base64')
    });
  }

  /**
   * Transforms data as it passes through the stream.
   *
   * @param {object} chunk - The data to transform.
   * @param {object} chunk.headers - The headers from an AWS event stream chunk.
   * @param {object} chunk.payload - The payload from an AWS event stream chunk.
   * @param {string} encoding - The encoding used in the stream (ignored)
   * @param {Function} callback - The callback to signal for completion.
   * @returns {undefined}
   */
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

/**
 * @external Transform
 * @see https://nodejs.org/dist/latest-v10.x/docs/api/stream.html#stream_class_stream_transform
 */

module.exports = {
  RecordsDecoder,
  getRecordsDecoder,
  getRecordsEncoder
};
