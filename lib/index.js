'use strict';

const { Kinesis: AwsKinesis } = require('aws-sdk');
const { PassThrough } = require('stream');
const ShardSubscriber = require('./shard-subscriber');
const consumer = require('./consumer');
const stream = require('./stream');
const { noop } = require('./utils');

const privateData = new WeakMap();

function internal(instance) {
  if (!privateData.has(instance)) privateData.set(instance, {});
  return privateData.get(instance);
}

class Kinesis extends PassThrough {
  constructor(options = {}) {
    super({ objectMode: true });
    const {
      compression,
      consumerName,
      createStreamIfNeeded = true,
      encryption,
      logger = {},
      shardCount = 1,
      streamName,
      tags,
      ...otherOptions
    } = options;

    const normLogger = {
      debug: typeof logger.debug === 'function' ? logger.debug.bind(logger) : noop,
      error: typeof logger.error === 'function' ? logger.error.bind(logger) : noop,
      warn: typeof logger.warn === 'function' ? logger.warn.bind(logger) : noop
    };

    if (!consumerName) {
      const errorMsg = 'The "consumerName" option is required.';
      normLogger.error(errorMsg);
      throw new TypeError(errorMsg);
    }

    if (!streamName) {
      const errorMsg = 'The "streamName" option is required.';
      normLogger.error(errorMsg);
      throw new TypeError(errorMsg);
    }

    Object.assign(internal(this), {
      compression,
      consumerName,
      createStreamIfNeeded,
      encryption,
      logger: normLogger,
      shardCount,
      streamName,
      tags,
      options: otherOptions
    });
  }

  async connect() {
    const ctx = internal(this);
    const { consumerName, encryption, tags, logger, options } = ctx;

    logger.debug('Trying to connect the client…');
    ctx.client = new AwsKinesis(options);

    ctx.streamArn = await stream.activate(ctx);
    if (encryption) await stream.encrypt(ctx);
    if (tags) await stream.tag(ctx);
    ctx.consumerArn = await consumer.activate(ctx);
    ctx.shards = (await stream.getShards(ctx)) || [];

    logger.debug(`Creating subscribers for the stream shards using "${consumerName}"…`);
    ctx.shards.forEach(shard => {
      const subscriber = new ShardSubscriber({ ...ctx, emitter: this, shard });
      subscriber.start();
      subscriber.on('error', err => this.emit('error', err));
      subscriber.pipe(this);
    });

    logger.debug('The client is now connected.');
  }
}

module.exports = Kinesis;
