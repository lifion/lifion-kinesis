/**
 * Module that implements an AWS enhanced fan-out consumer.
 *
 * @module fan-out-consumer
 * @private
 */

import aws4 from 'aws4';
import got from 'got';
import AWS from 'aws-sdk';
import lifionAwsEventStream from 'lifion-aws-event-stream';
import { Transform, Writable, pipeline } from 'node:stream';
import { promisify } from 'node:util';

import { RecordsDecoder } from './records.js';
import { getStreamShards } from './stream.js';
import { reportError, reportResponse } from './stats.js';
import { shouldBailRetry } from './utils.js';
import deaggregate from './deaggregate.js';

const { CredentialProviderChain } = AWS;
const { Parser } = lifionAwsEventStream;

const AWS_API_TARGET = 'Kinesis_20131202.SubscribeToShard';
const AWS_EVENT_STREAM = 'application/vnd.amazon.eventstream';
const AWS_JSON = 'application/x-amz-json-1.1';
const DEFAULT_KINESIS_ENDPOINT = 'https://kinesis.us-east-1.amazonaws.com';
const EXPIRATION_TIMEOUT_OFFSET = 1000;

const asyncPipeline = promisify(pipeline);
const wait = promisify(setTimeout);

/**
 * Class that implements a deaggregation stream used to the convert aggregated
 * records into multiple records which it pushes to the stream.  Non- aggregated
 * records should come through normally as single records.
 *
 * @augments external:Transform
 * @memberof module:fan-out-consumer
 * @private
 */

class Deaggregate extends Transform {
  #data = {};

  /**
   * Initializes an instance of the deaggregation stream.
   *
   * @param {Object} options - The initialization options.
   * @param {Object} options.logger - An instance of a logger.
   */
  constructor({ logger }) {
    super({ objectMode: true });
    Object.assign(this.#data, { logger });
  }

  /**
   * The stream transformation logic.
   *
   * @param {Buffer} chunk - A chunk of data coming from the event stream.
   * @param {string} encoding - The stream encoding mode (ignored)
   * @param {function(Error=): void} callback - The callback for more data.
   */
  async _transform(chunk, encoding, callback) {
    const { logger } = this.#data;

    try {
      if (!chunk || !chunk.payload || !chunk.payload.Records) {
        this.push(chunk);
        callback();
        return;
      }
      const records = await deaggregate(chunk.payload.Records, false);

      this.push({ ...chunk, payload: { ...chunk.payload, Records: records } });
    } catch (err) {
      logger.warn('Error deaggregating record', err);
    }
    callback();
  }
}

/**
 * The flags tracking the state of a shard subscription request.
 *
 * @typedef {Object} RequestFlags
 * @property {boolean} isEventStream - If the request is sucessful and the headers in the initial
 *        response point to an even stream, this flag is set to `true`.
 * @property {number} statusCode - The status code of the last request response.
 * @private
 */

/**
 * Class that implements a pre-processing stream used as a filter to a stream request to the
 * shard subscription API. If the request is successful and the response is an event stream,
 * chunks are passed to subsequent streams in the pipeline. If the server responds with an error,
 * the error details are parsed and then thrown as an error, which breaks the entire pipeline.
 *
 * @augments external:Transform
 * @memberof module:fan-out-consumer
 * @private
 */
class PreProcess extends Transform {
  #data = {};

  /**
   * Initializes an instance of the pre-processing stream.
   *
   * @param {Object} options - The initialization options.
   * @param {RequestFlags} options.requestFlags - The object where the flags for the request are stored.
   */
  constructor({ requestFlags }) {
    super({ objectMode: true });
    Object.assign(this.#data, { requestFlags });
  }

  /**
   * The stream transformation logic.
   *
   * @param {Buffer} chunk - A chunk of data coming from the event stream.
   * @param {string} encoding - The stream encoding mode (ignored)
   * @param {function(Error=): void} callback - The callback for more data.
   */
  _transform(chunk, encoding, callback) {
    const { requestFlags } = this.#data;
    if (!requestFlags.isEventStream) {
      const { statusCode } = requestFlags;
      try {
        const { __type, message } = JSON.parse(chunk.toString('utf8'));
        const error = Object.assign(
          new Error(message || 'Failed to subscribe to shard.'),
          { isRetryable: true },
          __type && { code: __type },
          statusCode && { statusCode }
        );
        this.emit('error', error);
      } catch {
        const error = Object.assign(
          new Error(chunk),
          { isRetryable: true },
          statusCode && { statusCode }
        );
        this.emit('error', error);
      }
    } else {
      this.push(chunk);
    }
    callback();
  }
}

/**
 * Class that implements a post-processing stream used to push records outside the internal
 * stream pipeline. It also stores checkpoints as records arrive, and look for shard depletion.
 *
 * @augments external:Writable
 * @memberof module:fan-out-consumer
 * @private
 */
class PostProcess extends Writable {
  #data = {};

  /**
   * Initializes an instance of the post-processing stream.
   *
   * @param {Object} options - The initialization options.
   * @param {function(): void} options.abort - A function that will close the entire pipeline, called
   *        when no data has been pushed through the event stream on a given time window.
   * @param {Object} options.logger - An instance of a logger.
   * @param {function(): void} options.markShardAsDepleted - A function that will mark a given shard as
   *        depleted. Called when a shard depletion event has been detected.
   * @param {function(Error, Object): void} options.pushToStream - A function that pushes records out of the pipeline.
   * @param {function(string): Promise<void>} options.setCheckpoint - A function that stores the checkpoint for the shard.
   * @param {string} options.shardId - The ID of the shard.
   */
  constructor({ abort, logger, markShardAsDepleted, pushToStream, setCheckpoint, shardId }) {
    super({ objectMode: true });
    Object.assign(this.#data, {
      abort,
      logger,
      markShardAsDepleted,
      pushToStream,
      setCheckpoint,
      shardId,
      timeoutId: null
    });
  }

  cancelTimeout() {
    const { timeoutId } = this.#data;
    clearTimeout(timeoutId);
  }

  /**
   * The stream writable logic.
   *
   * @param {Object} chunk - A chunk of data coming from the pipeline.
   * @param {string} encoding - The stream encoding mode (ignored)
   * @param {function(Error=): void} callback - The callback for more data.
   */
  async _write(chunk, encoding, callback) {
    const { abort, logger, markShardAsDepleted, pushToStream, setCheckpoint, shardId, timeoutId } =
      this.#data;
    clearTimeout(timeoutId);
    this.#data.timeoutId = setTimeout(abort, 10000);
    const { continuationSequenceNumber, millisBehindLatest, records } = chunk;
    if (continuationSequenceNumber !== undefined) {
      await setCheckpoint(continuationSequenceNumber);
      const recordsCount = records.length;
      const msBehind = millisBehindLatest;
      if (recordsCount > 0) {
        logger.debug(`Got ${recordsCount} record(s) from "${shardId}" (${msBehind}ms behind)`);
        pushToStream(null, { ...chunk, shardId });
      }
      callback();
    } else {
      markShardAsDepleted();
    }
  }
}

/**
 * Class that implements an AWS enhanced fan-out consumer.
 *
 * @alias module:fan-out-consumer
 */
class FanOutConsumer {
  #data = {};

  /**
   * Initializes an instance of an enhanced fan-out consumer.
   *
   * @param {Object} options - The initialization options.
   * @param {Object} options.awsOptions - The AWS.Kinesis options to use in the HTTP request.
   * @param {string} options.checkpoint - The last-known checkpoint for the stream shard.
   * @param {Object} options.client - An instance of the Kinesis client.
   * @param {string} options.compression - The kind of data compression to use with records.
   * @param {string} options.consumerArn - The ARN of the enhanced consumer as registered in AWS.
   * @param {string} options.initialPositionInStream -  The location in the shard from which the Consumer will start
   *        fetching records from when the application starts for the first time and there is no checkpoint for the shard.
   * @param {string} options.leaseExpiration - The timestamp of the shard lease expiration.
   * @param {Object} options.logger - An instance of a logger.
   * @param {function(Error, Object): void} options.pushToStream - A function to push incoming records to the consumer.
   * @param {string} options.shardId - The ID of the stream shard to subscribe for records.
   * @param {Object} options.stateStore - An instance of the state store.
   * @param {function(): void} options.stopConsumer - A function that stops this consumer from the manager.
   * @param {string} options.streamName - The name of the Kinesis stream.
   *        user-intervention before polling for more records, or not.
   * @param {boolean} options.useS3ForLargeItems - Whether to automatically use an S3
   *        bucket to store large items or not.
   */
  constructor(options) {
    const {
      awsOptions,
      checkpoint,
      client,
      compression,
      consumerArn,
      initialPositionInStream,
      leaseExpiration,
      logger,
      pushToStream,
      s3,
      shardId,
      shouldDeaggregate,
      shouldParseJson,
      stateStore,
      stopConsumer,
      streamName,
      useS3ForLargeItems
    } = options;

    const { endpoint: configuredEndpoint, region } = awsOptions;
    const endpoint =
      configuredEndpoint ||
      (region ? `https://kinesis.${region}.amazonaws.com` : DEFAULT_KINESIS_ENDPOINT);
    const credentialsChain = new CredentialProviderChain();

    const signRequest = async (requestOptions) => {
      const { body, headers, method, url } = requestOptions;
      let { accessKeyId, secretAccessKey, sessionToken } = awsOptions;
      if (!accessKeyId && !secretAccessKey && !sessionToken) {
        ({ accessKeyId, secretAccessKey, sessionToken } = await credentialsChain.resolvePromise());
      }
      const { host, pathname, search } = new URL(url);
      const { headers: signedHeaders } = aws4.sign(
        {
          body,
          headers: { ...headers },
          host,
          method,
          path: `${pathname}${search}`,
          region,
          service: 'kinesis'
        },
        { accessKeyId, secretAccessKey, sessionToken }
      );
      Object.assign(headers, signedHeaders);
    };

    const httpClient = got.extend({
      headers: { 'Content-Type': AWS_JSON },
      hooks: { beforeRequest: [signRequest] },
      method: 'POST',
      prefixUrl: endpoint,
      throwHttpErrors: false
    });

    Object.assign(this.#data, {
      checkpoint,
      client,
      compression,
      consumerArn,
      expirationTimeoutId: null,
      httpClient,
      initialPositionInStream,
      leaseExpiration,
      logger,
      pushToStream,
      request: null,
      retryPipeline: true,
      s3,
      shardId,
      shouldDeaggregate,
      shouldParseJson,
      stateStore,
      stopConsumer,
      stream: null,
      streamName,
      useS3ForLargeItems
    });
  }

  /**
   * Starts the enhanced fan-out consumer by initializing the internal stream pipeline.
   *
   * @fulfil {undefined}
   * @returns {Promise}
   */
  async start() {
    const privateProps = this.#data;
    const {
      client,
      compression,
      consumerArn,
      httpClient,
      initialPositionInStream,
      leaseExpiration,
      logger,
      pushToStream,
      s3,
      s3Client,
      shardId,
      shouldDeaggregate,
      shouldParseJson,
      stateStore,
      stopConsumer,
      streamName,
      useS3ForLargeItems
    } = privateProps;

    logger.debug(`Starting an enhanced fan-out subscriber for shard "${shardId}"…`);

    this.updateLeaseExpiration(leaseExpiration);

    let shardsPath;
    let shardsPathNames;

    try {
      ({ shardsPath, shardsPathNames } = await stateStore.getShardsData());
    } catch (err) {
      logger.warn("Can't start the consumer as the state can't be resolved:", err);
      stopConsumer(shardId);
      return;
    }

    const requestFlags = {};

    const handleRequest = (req) => {
      privateProps.request = req;
    };

    const handleResponse = async (res) => {
      const { headers, statusCode } = res;
      requestFlags.statusCode = statusCode;
      if (headers['content-type'] !== AWS_EVENT_STREAM || statusCode !== 200) {
        logger.warn(`Subscription unsuccessful: ${statusCode}`);
        requestFlags.isEventStream = false;
        reportError('kinesis', { statusCode }, streamName);
      } else {
        logger.debug('Subscription to shard is successful.');
        requestFlags.isEventStream = true;
        reportResponse('kinesis', streamName);
      }
    };

    const markShardAsDepleted = async () => {
      const shards = await getStreamShards({ client, logger, streamName });
      logger.debug(`The parent shard "${shardId}" has been depleted.`);
      await stateStore.markShardAsDepleted(shards, shardId);
      stopConsumer(shardId);
    };

    const setCheckpoint = async (sequenceNumber) => {
      await stateStore.storeShardCheckpoint(shardId, sequenceNumber, shardsPath, shardsPathNames);
      privateProps.checkpoint = sequenceNumber;
    };

    const abort = () => {
      const { request, stream } = privateProps;
      if (request) {
        request.abort();
        privateProps.request = null;
      }
      if (stream) {
        stream.destroy();
        privateProps.stream = null;
      }
    };

    do {
      if (requestFlags.isEventStream === false) {
        logger.warn(`Waiting before retrying the pipeline…`);
        await wait(5000);
      }

      const { checkpoint } = privateProps;

      const stream = httpClient.stream({
        body: JSON.stringify({
          ConsumerARN: consumerArn,
          ShardId: shardId,
          StartingPosition: {
            ...(checkpoint && { SequenceNumber: checkpoint }),
            Type: checkpoint ? 'AFTER_SEQUENCE_NUMBER' : initialPositionInStream
          }
        }),
        headers: { 'X-Amz-Target': AWS_API_TARGET }
      });
      privateProps.stream = stream;
      stream.on('request', handleRequest);
      stream.on('response', handleResponse);

      const postProcess = new PostProcess({
        abort,
        logger,
        markShardAsDepleted,
        pushToStream,
        setCheckpoint,
        shardId
      });

      try {
        const processes = [
          stream,
          new PreProcess({ requestFlags }),
          new Parser(),
          new RecordsDecoder({
            compression,
            logger,
            s3,
            s3Client,
            shouldParseJson,
            useS3ForLargeItems
          }),
          postProcess
        ];

        if (shouldDeaggregate) {
          processes.splice(3, 0, new Deaggregate({ logger }));
        }
        await asyncPipeline(processes);
      } catch (err) {
        const { code, message, requestId, statusCode } = err;
        if (code !== 'ERR_STREAM_PREMATURE_CLOSE') {
          if (!shouldBailRetry(err) || code === 'ResourceInUseException') {
            logger.warn(
              [
                'Trying to recover from AWS.Kinesis error…',
                `- Message: ${message}`,
                `- Request ID: ${requestId}`,
                `- Code: ${code} (${statusCode})`,
                `- Stream: ${streamName}`
              ].join('\n\t')
            );
          } else {
            pushToStream(err);
            logger.error(`Pipeline closed with error: [${code}] ${message}`);
            privateProps.retryPipeline = false;
          }
        }
      }

      postProcess.cancelTimeout();
    } while (privateProps.retryPipeline);

    abort();
  }

  /**
   * Stops the internal stream pipeline.
   */
  stop() {
    const privateProps = this.#data;
    const { expirationTimeoutId, request, stream } = privateProps;
    if (request) {
      request.abort();
      privateProps.request = null;
      privateProps.retryPipeline = false;
    }
    if (stream) {
      stream.destroy();
      privateProps.stream = null;
    }
    clearTimeout(expirationTimeoutId);
    privateProps.expirationTimeoutId = null;
  }

  /**
   * Updates the shard lease expiration timestamp.
   *
   * @param {string} leaseExpiration - The updated timestamp when the shard lease expires.
   */
  updateLeaseExpiration(leaseExpiration) {
    const privateProps = this.#data;
    const { expirationTimeoutId, logger, shardId, stopConsumer } = privateProps;

    privateProps.leaseExpiration = leaseExpiration;

    clearTimeout(expirationTimeoutId);
    privateProps.expirationTimeoutId = null;

    const delay = new Date(leaseExpiration).getTime() - Date.now() - EXPIRATION_TIMEOUT_OFFSET;
    if (delay < 0) {
      return;
    }

    privateProps.expirationTimeoutId = setTimeout(() => {
      logger.debug(`The lease for "${shardId}" has expired.`);
      stopConsumer(shardId);
    }, delay);
  }
}

/**
 * @external Transform
 * @see https://nodejs.org/dist/latest-v10.x/docs/api/stream.html#stream_class_stream_transform
 */

/**
 * @external Writable
 * @see https://nodejs.org/dist/latest-v10.x/docs/api/stream.html#stream_class_stream_writable
 */

export default FanOutConsumer;
