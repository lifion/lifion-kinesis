'use strict';

const aws4 = require('aws4');
const got = require('got');
const { CredentialProviderChain } = require('aws-sdk');
const { Parser } = require('lifion-aws-event-stream');
const { Transform, Writable, pipeline } = require('stream');
const { promisify } = require('util');

const { RecordsDecoder } = require('./records');
const { getStreamShards } = require('./stream');
const { safeJsonParse, wait } = require('./utils');

const AWS_API_TARGET = 'Kinesis_20131202.SubscribeToShard';
const AWS_EVENT_STREAM = 'application/vnd.amazon.eventstream';
const AWS_JSON = 'application/x-amz-json-1.1';
const DEFAULT_KINESIS_ENDPOINT = 'https://kinesis.us-east-1.amazonaws.com';
const EXPIRATION_TIMEOUT_OFFSET = 1000;

const privateData = new WeakMap();
const asyncPipeline = promisify(pipeline);

function internal(instance) {
  if (!privateData.has(instance)) privateData.set(instance, {});
  return privateData.get(instance);
}

class PreProcess extends Transform {
  constructor({ requestFlags }) {
    super({ objectMode: true });
    Object.assign(internal(this), { requestFlags });
  }

  _transform(chunk, encoding, callback) {
    const { requestFlags } = internal(this);
    if (!requestFlags.isEventStream) {
      const { __type, message } = safeJsonParse(chunk.toString('utf8'));
      const err = new Error(message || 'Failed to subscribe to shard.');
      if (__type) err.code = __type;
      err.isRetryable = true;
      this.emit('error', err);
    } else {
      this.push(chunk);
    }
    callback();
  }
}

class PostProcess extends Writable {
  constructor({ markShardAsDepleted, pushToStream, shardId, stateStore }) {
    super({ objectMode: true });
    Object.assign(internal(this), { markShardAsDepleted, pushToStream, shardId, stateStore });
  }

  async _write(chunk, encoding, callback) {
    const { markShardAsDepleted, pushToStream, shardId, stateStore } = internal(this);
    const { continuationSequenceNumber, records } = chunk;
    if (continuationSequenceNumber !== undefined) {
      await stateStore.storeShardCheckpoint(shardId, continuationSequenceNumber);
      if (records.length > 0) pushToStream(null, { ...chunk, shardId });
      callback();
    } else {
      markShardAsDepleted();
    }
  }
}

class FanOutConsumer {
  constructor(options) {
    const {
      awsOptions,
      checkpoint,
      client,
      compression,
      consumerArn,
      leaseExpiration,
      logger,
      pushToStream,
      shardId,
      stateStore,
      stopConsumer,
      streamName
    } = options;

    const { endpoint = DEFAULT_KINESIS_ENDPOINT, region } = awsOptions;
    const credentialsChain = new CredentialProviderChain();

    const signRequest = async requestOptions => {
      let { accessKeyId, secretAccessKey, sessionToken } = awsOptions;
      if (!accessKeyId && !secretAccessKey && !sessionToken)
        ({ accessKeyId, secretAccessKey, sessionToken } = await credentialsChain.resolvePromise());
      aws4.sign(requestOptions, { accessKeyId, secretAccessKey, sessionToken });
    };

    const httpClient = got.extend({
      baseUrl: endpoint,
      headers: { 'Content-Type': AWS_JSON },
      hooks: { beforeRequest: [signRequest] },
      region,
      throwHttpErrors: false
    });

    Object.assign(internal(this), {
      checkpoint,
      client,
      compression,
      consumerArn,
      expirationTimeoutId: null,
      httpClient,
      leaseExpiration,
      logger,
      pushToStream,
      request: null,
      retryPipeline: true,
      shardId,
      stateStore,
      stopConsumer,
      streamName
    });
  }

  async start() {
    const privateProps = internal(this);
    const {
      checkpoint,
      client,
      compression,
      consumerArn,
      httpClient,
      leaseExpiration,
      logger,
      pushToStream,
      shardId,
      stateStore,
      stopConsumer,
      streamName
    } = privateProps;

    logger.debug(`Starting an enhanded fan-out subscriber for shard "${shardId}"…`);

    this.updateLeaseExpiration(leaseExpiration);

    let pipelineError;
    const requestFlags = {};
    let stream;

    const handleRequest = req => {
      privateProps.request = req;
    };

    const handleResponse = async res => {
      const { headers, statusCode } = res;
      if (headers['content-type'] !== AWS_EVENT_STREAM || statusCode !== 200) {
        logger.error(`Subscription unsuccessful: ${statusCode}`);
        requestFlags.isEventStream = false;
      } else {
        logger.debug('Subscription to shard is successful.');
        requestFlags.isEventStream = true;
      }
    };

    const markShardAsDepleted = async () => {
      const shards = await getStreamShards({ client, logger, streamName });
      logger.debug(`The parent shard "${shardId}" has been depleted.`);
      await stateStore.markShardAsDepleted(shards, shardId);
      stopConsumer(shardId);
    };

    do {
      if (requestFlags.isEventStream === false) {
        logger.warn(`Waiting before retrying the pipeline…`);
        await wait(5000);
      }

      stream = httpClient.stream('/', {
        body: JSON.stringify({
          ConsumerARN: consumerArn,
          ShardId: shardId,
          StartingPosition: Object.assign(
            { Type: checkpoint ? 'AFTER_SEQUENCE_NUMBER' : 'LATEST' },
            checkpoint && { SequenceNumber: checkpoint }
          )
        }),
        headers: { 'X-Amz-Target': AWS_API_TARGET },
        service: 'kinesis'
      });

      stream.on('request', handleRequest);
      stream.on('response', handleResponse);

      try {
        await asyncPipeline([
          stream,
          new PreProcess({ requestFlags }),
          new Parser(),
          new RecordsDecoder({ compression }),
          new PostProcess({ markShardAsDepleted, pushToStream, shardId, stateStore })
        ]);
      } catch (err) {
        const { code, isRetryable, message } = err;
        if (isRetryable) {
          logger.warn(`Pipeline closed with retryable error: [${code}] ${message}`);
        } else {
          pushToStream(err);
          logger.error(`Pipeline closed with error: [${code}] ${message}`);
        }
        pipelineError = err;
      }
    } while (privateProps.retryPipeline && (!pipelineError || pipelineError.isRetryable));

    if (privateProps.request) {
      privateProps.request.abort();
    }
  }

  stop() {
    const privateProps = internal(this);
    const { request } = privateProps;
    if (request) {
      request.abort();
      privateProps.request = null;
      privateProps.retryPipeline = false;
    }
  }

  updateLeaseExpiration(leaseExpiration) {
    const privateProps = internal(this);
    const { expirationTimeoutId, logger, shardId, stopConsumer } = privateProps;

    privateProps.leaseExpiration = leaseExpiration;

    clearTimeout(expirationTimeoutId);

    const delay = new Date(leaseExpiration).getTime() - Date.now() - EXPIRATION_TIMEOUT_OFFSET;
    if (delay < 0) return;

    privateProps.expirationTimeoutId = setTimeout(() => {
      logger.debug(`The lease for "${shardId}" has expired.`);
      stopConsumer(shardId);
    }, delay);
  }
}

module.exports = FanOutConsumer;
