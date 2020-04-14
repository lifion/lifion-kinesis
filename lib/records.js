/**
 * Module that contains a collection of classes and functions to encode and decode Kinesis records.
 *
 * @module records
 * @private
 */

'use strict';

const shortUuid = require('short-uuid');
const { Transform } = require('stream');
const { createHash } = require('crypto');

const compressionLibs = require('./compression');

const IS_JSON_REGEX = /^[[{].*[\]}]$/;

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
  return createHash('sha1').update(buffer).digest('base64');
}

/**
 * Returns a function that decodes Kinesis records as they are retrieved from AWS.Kinesis into
 * native objects. The decoder will also decompress the record data as instructed.
 *
 * @param {object} [options] - Options object.
 * @param {string} [options.compression] - The kind of compression used in the Kinesis record data.
 * @param {string} [options.inputEncoding] - The encoding of the `Data` property in the AWS.Kinesis record.
 * @param {boolean|string} [options.shouldParseJson] - Whether if retrieved records' data should be parsed as JSON or not.
 * @param {object} [options.logger] - An instance of a logger.
 * @param {object} [options.s3Client] - The s3Client in the current kinesis client.
 * @param {boolean} [options.useS3ForLargeItems] - Whether to automatically use an S3 bucket to store large items or not.
 * @returns {Function} A function that decodes `record` objects from AWS.Kinesis.
 * @memberof module:records
 */
function getRecordsDecoder({
  compression,
  inputEncoding,
  logger,
  s3Client,
  shouldParseJson,
  useS3ForLargeItems
} = {}) {
  const compressionLib = compression && compressionLibs[compression];

  return async (record) => {
    const {
      ApproximateArrivalTimestamp: approximateArrivalTimestamp,
      EncryptionType: encryptionType,
      PartitionKey: partitionKey,
      SequenceNumber: sequenceNumber
    } = record;
    const recordHeaders = {
      approximateArrivalTimestamp,
      encryptionType,
      partitionKey,
      sequenceNumber
    };

    let data = record.Data;

    try {
      if (compressionLib) {
        data = await compressionLib.decompress(data, inputEncoding);
      } else if (inputEncoding === 'Base64') {
        data = Buffer.from(data, 'base64').toString('utf8');
      }

      if (
        ((shouldParseJson === undefined || shouldParseJson === 'auto') &&
          IS_JSON_REGEX.test(data)) ||
        shouldParseJson === true
      ) {
        data = JSON.parse(data);
      }

      const { '@S3Item': s3Item, ...nonS3KeysData } = data;
      if (useS3ForLargeItems && s3Client && s3Item) {
        const { bucket, key } = s3Item;
        const { Body, ContentType } = await s3Client.getObject({ Bucket: bucket, Key: key });
        return {
          ...recordHeaders,
          ...nonS3KeysData,
          data: ContentType === 'application/json' ? JSON.parse(Body) : Body.toString('utf8')
        };
      }
    } catch (err) {
      if (logger) logger.warn('Could not decode record:', err);
    }

    return {
      ...recordHeaders,
      data
    };
  };
}

/**
 * Returns a function that encodes native objects into records that can be sent to AWS.Kinesis.
 * The encoder will also compress the record data using the specified compression library.
 *
 * @param {object} [options] - Options object.
 * @param {string} [options.compression] - The kind of compression used for the Kinesis record data.
 * @param {string} [options.outputEncoding] - The encoding for the resulting `Data` property.
 * @param {object} [options.s3] - The S3 options in the current kinesis client.
 * @param {object} [options.s3Client] - The s3Client in the current kinesis client.
 * @param {object} [options.streamName] - The name of the kinesis stream.
 * @param {boolean} [options.useS3ForLargeItems] - Whether to automatically use an S3 bucket to store large items or not.
 * @returns {Function} A function that encodes objects into the format expected by AWS.Kinesis.
 * @memberof module:records
 */
function getRecordsEncoder({
  compression,
  outputEncoding,
  s3 = {},
  s3Client,
  streamName,
  useS3ForLargeItems
} = {}) {
  const { bucketName, largeItemThreshold, nonS3Keys } = s3;
  const compressionLib = compression && compressionLibs[compression];

  return async (record) => {
    const { data, explicitHashKey, partitionKey, sequenceNumberForOrdering } = record;
    if (data === undefined) throw new TypeError('The "data" property is required.');
    let normData = typeof data !== 'string' ? JSON.stringify(data) : data;

    if (useS3ForLargeItems && s3Client && normData.length > largeItemThreshold * 1024) {
      const nonS3KeysData = {};

      if (data && typeof data === 'object') {
        Object.keys(data).forEach((key) => {
          if (nonS3Keys.includes(key)) {
            nonS3KeysData[key] = data[key];
          }
        });
      }

      const s3ItemKey = `${streamName}-${shortUuid.generate()}.json`;
      const putData = await s3Client.putObject({
        Body: data,
        Bucket: bucketName,
        Key: s3ItemKey
      });

      normData = JSON.stringify({
        '@S3Item': {
          bucket: bucketName,
          eTag: putData.ETag.replace(/"/g, ''),
          key: s3ItemKey
        },
        ...nonS3KeysData
      });
    }

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
 * @augments Transform
 * @memberof module:records
 * @see https://nodejs.org/dist/latest-v10.x/docs/api/stream.html#stream_class_stream_transform
 */
class RecordsDecoder extends Transform {
  /**
   * Initializes the decoder stream.
   *
   * @param {object} options - The initialization options.
   * @param {string} options.compression - The kind of compression to use in records data.
   * @param {string} options.shouldParseJson - If data is in JSON format and should be parsed.
   * @param {string} options.logger - An instance of a logger.
   * @param {object} options.s3Client - The s3Client in the current kinesis client.
   * @param {boolean} options.useS3ForLargeItems - Whether to automatically use an S3 bucket to store large items or not.
   */
  constructor({ compression, logger, s3Client, shouldParseJson, useS3ForLargeItems }) {
    super({ objectMode: true });

    Object.assign(internal(this), {
      compression,
      recordsDecoder: getRecordsDecoder({
        compression,
        inputEncoding: 'Base64',
        logger,
        s3Client,
        shouldParseJson,
        useS3ForLargeItems
      })
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
   * @returns {void}
   */
  _transform({ headers, payload }, encoding, callback) {
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
      const continuationSequenceNumber = payload.ContinuationSequenceNumber;
      const millisBehindLatest = payload.MillisBehindLatest;
      Promise.all(payload.Records.map(recordsDecoder)).then((records) => {
        this.push({ continuationSequenceNumber, millisBehindLatest, records });
        callback();
      });
      return;
    }

    this.emit(eventType, payload);
    callback();
  }
}

module.exports = {
  RecordsDecoder,
  getRecordsDecoder,
  getRecordsEncoder
};
