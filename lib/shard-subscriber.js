/* eslint-disable no-await-in-loop, no-loop-func */

'use strict';

const AWS = require('aws-sdk');
const aws4 = require('aws4');
const got = require('got');
const { Parser } = require('lifion-aws-event-stream');
const { PassThrough, Transform, Writable, pipeline } = require('stream');
const { promisify } = require('util');
const Decoder = require('./records-decoder');
const { safeJsonParse, wait } = require('./utils');

const AWS_API_TARGET = 'Kinesis_20131202.SubscribeToShard';
const AWS_EVENT_STREAM = 'application/vnd.amazon.eventstream';
const AWS_JSON = 'application/x-amz-json-1.1';

const privateData = new WeakMap();
const asyncPipeline = promisify(pipeline);

function internal(instance) {
  if (!privateData.has(instance)) privateData.set(instance, {});
  return privateData.get(instance);
}

class ShardSubscriber extends PassThrough {
  constructor(ctx) {
    super({ objectMode: true });
    const { options } = ctx;
    const { endpoint = 'https://kinesis.us-east-1.amazonaws.com', region } = options;
    const credentialsChain = new AWS.CredentialProviderChain();

    const signRequest = async requestOptions => {
      let { accessKeyId, secretAccessKey, sessionToken } = options;
      if (!accessKeyId && !secretAccessKey && !sessionToken)
        ({ accessKeyId, secretAccessKey, sessionToken } = await credentialsChain.resolvePromise());
      aws4.sign(requestOptions, { accessKeyId, secretAccessKey, sessionToken });
    };

    const httpClient = got.extend({
      baseUrl: endpoint,
      headers: { 'content-type': AWS_JSON },
      hooks: { beforeRequest: [signRequest] },
      region,
      throwHttpErrors: false
    });

    Object.assign(internal(this), { ...ctx, httpClient });
  }

  async start() {
    const ctx = internal(this);
    const { consumerArn: ConsumerARN, consumerName, httpClient, logger, shard } = ctx;
    const { ShardId } = shard;
    const checkpoints = {};
    const instance = this;

    let isEventStream;
    let pipelineError;
    let request;
    let stream;

    const handleRequest = req => {
      request = req;
    };

    const handleResponse = async res => {
      const { headers, statusCode } = res;
      if (headers['content-type'] !== AWS_EVENT_STREAM || statusCode !== 200) {
        logger.error(`Subscription unsuccessful: ${statusCode}`);
        isEventStream = false;
      } else {
        logger.debug('Subscription to shard is successful.');
        isEventStream = true;
      }
    };

    logger.debug(`Starting a "${consumerName}" subscriber for shard "${ShardId}"…`);

    do {
      if (isEventStream === false) {
        logger.warn(`Waiting before retrying the pipeline…`);
        await wait(5000);
      }

      const checkpoint = checkpoints[ShardId];
      const StartingPosition = {};
      if (checkpoint) {
        logger.debug('Starting from local checkpoint.');
        StartingPosition.Type = 'AFTER_SEQUENCE_NUMBER';
        StartingPosition.SequenceNumber = checkpoint;
      } else {
        logger.debug('Starting position: LATEST');
        StartingPosition.Type = 'LATEST';
      }

      stream = httpClient.stream('/', {
        body: JSON.stringify({ ConsumerARN, ShardId, StartingPosition }),
        headers: { 'X-Amz-Target': AWS_API_TARGET },
        service: 'kinesis'
      });

      stream.on('request', handleRequest);
      stream.on('response', handleResponse);

      try {
        await asyncPipeline([
          stream,
          new Transform({
            objectMode: true,
            write(chunk, encoding, callback) {
              if (!isEventStream) {
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
          }),
          new Parser(),
          new Decoder({ ...ctx, checkpoints }),
          new Writable({
            objectMode: true,
            write(chunk, encoding, callback) {
              instance.push(chunk);
              callback();
            }
          })
        ]);
      } catch (err) {
        if (err.isRetryable) {
          const { code, message } = err;
          logger.warn(`Pipeline closed with retryable error: [${code}] ${message}`);
        } else {
          this.emit('error', err);
          logger.error('Pipeline closed with error:', err.stack);
        }
        pipelineError = err;
      }
    } while (!pipelineError || pipelineError.isRetryable);

    if (request) request.abort();

    return this;
  }
}

module.exports = ShardSubscriber;
