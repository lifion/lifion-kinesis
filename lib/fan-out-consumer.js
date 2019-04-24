'use strict';

const aws4 = require('aws4');
const got = require('got');
const { CredentialProviderChain } = require('aws-sdk');
const { Parser } = require('lifion-aws-event-stream');
const { Transform, Writable, pipeline } = require('stream');
const { promisify } = require('util');

const { RecordsDecoder } = require('./records');
const { safeJsonParse, wait } = require('./utils');

const AWS_API_TARGET = 'Kinesis_20131202.SubscribeToShard';
const AWS_EVENT_STREAM = 'application/vnd.amazon.eventstream';
const AWS_JSON = 'application/x-amz-json-1.1';
const DEFAULT_KINESIS_ENDPOINT = 'https://kinesis.us-east-1.amazonaws.com';

const privateData = new WeakMap();
const asyncPipeline = promisify(pipeline);

function internal(instance) {
  if (!privateData.has(instance)) privateData.set(instance, {});
  return privateData.get(instance);
}

class PreProcess extends Transform {
  constructor(privateProps) {
    super({ objectMode: true });
    internal(this).privateProps = privateProps;
  }

  _transform(chunk, encoding, callback) {
    const { privateProps } = internal(this);
    if (!privateProps.isEventStream) {
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
  constructor({ pushToStream, shardId }) {
    super({ objectMode: true });
    Object.assign(internal(this), { pushToStream, shardId });
  }

  _write(chunk, encoding, callback) {
    const { pushToStream, shardId } = internal(this);
    pushToStream(null, { ...chunk, shardId });
    callback();
  }
}

class FanOutConsumer {
  constructor(options) {
    const {
      awsOptions,
      compression,
      consumerArn,
      leaseExpiration,
      logger,
      pushToStream,
      shardId
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
      compression,
      consumerArn,
      httpClient,
      isEventStream: null,
      leaseExpiration,
      logger,
      pushToStream,
      request: null,
      shardId
    });
  }

  async start() {
    const privateProps = internal(this);
    const { compression, consumerArn, httpClient, logger, pushToStream, shardId } = privateProps;
    logger.debug(`Starting an enhanded fan-out subscriber for shard "${shardId}"…`);

    let pipelineError;
    let request;
    let stream;

    const handleRequest = req => {
      request = req;
      privateProps.request = request;
    };

    const handleResponse = async res => {
      const { headers, statusCode } = res;
      if (headers['content-type'] !== AWS_EVENT_STREAM || statusCode !== 200) {
        logger.error(`Subscription unsuccessful: ${statusCode}`);
        privateProps.isEventStream = false;
      } else {
        logger.debug('Subscription to shard is successful.');
        privateProps.isEventStream = true;
      }
    };

    do {
      if (privateProps.isEventStream === false) {
        logger.warn(`Waiting before retrying the pipeline…`);
        await wait(5000);
      }

      stream = httpClient.stream('/', {
        body: JSON.stringify({
          ConsumerARN: consumerArn,
          ShardId: shardId,
          StartingPosition: { Type: 'LATEST' }
        }),
        headers: { 'X-Amz-Target': AWS_API_TARGET },
        service: 'kinesis'
      });

      stream.on('request', handleRequest);
      stream.on('response', handleResponse);

      try {
        await asyncPipeline([
          stream,
          new PreProcess(privateProps),
          new Parser(),
          new RecordsDecoder({ compression }),
          new PostProcess({ pushToStream, shardId })
        ]);
      } catch (err) {
        if (err.isRetryable) {
          const { code, message } = err;
          logger.warn(`Pipeline closed with retryable error: [${code}] ${message}`);
        } else {
          pushToStream(err);
          logger.error('Pipeline closed with error:', err.stack);
        }
        pipelineError = err;
      }
    } while (!pipelineError || pipelineError.isRetryable);

    if (request) request.abort();

    return this;
  }

  stop() {
    const { request } = internal(this);
    if (request) request.abort();
  }

  updateLeaseExpiration(leaseExpiration) {
    internal(this).leaseExpiration = new Date(leaseExpiration).getTime();
  }
}

module.exports = FanOutConsumer;
