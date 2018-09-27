'use strict';

const AWS = require('aws-sdk');
const aws4 = require('aws4');
const got = require('got');
const { pipeline, Writable } = require('stream');
const MessagesParser = require('./messages-parser');

const AWS_API_TARGET = 'Kinesis_20131202.SubscribeToShard';
const AWS_EVENT_STREAM = 'application/vnd.amazon.eventstream';
const AWS_JSON = 'application/x-amz-json-1.1';

const privateData = new WeakMap();

function internal(instance) {
  if (!privateData.has(instance)) privateData.set(instance, {});
  return privateData.get(instance);
}

class ShardSubscriber {
  constructor(ctx) {
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
    const { logger, consumerName, consumerArn: ConsumerARN, shard, httpClient } = ctx;
    const { ShardId } = shard;
    logger.debug(`Starting a "${consumerName}" subscriber for shard "${ShardId}"…`);

    const stream = httpClient.stream('/', {
      body: JSON.stringify({ ConsumerARN, ShardId, StartingPosition: { Type: 'LATEST' } }),
      headers: { 'X-Amz-Target': AWS_API_TARGET },
      service: 'kinesis'
    });

    let request;
    stream.on('request', req => {
      request = req;
    });

    stream.on('response', res => {
      const { headers, statusCode } = res;
      if (headers['content-type'] !== AWS_EVENT_STREAM || statusCode !== 200) {
        logger.error(`Subscription unsuccessful: ${statusCode}`);
        request.abort();
      } else logger.debug('Subscription to shard is successful.');
    });

    pipeline(
      stream,
      new MessagesParser(ctx),
      new Writable({
        write(chunk, encoding, callback) {
          logger.debug(chunk.toString('utf8'));
          callback();
        }
      }),
      err => {
        if (err) {
          logger.error(err);
          request.abort();
        } else logger.debug('Subscription pipeline completed.');
      }
    );

    return this;
  }
}

module.exports = ShardSubscriber;
