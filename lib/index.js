/**
 * Lifion's Node.js client for Amazon Kinesis Data Streams.
 *
 * @module lifion-kinesis
 */

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

/**
 * A specialization of the [readable stream]{@link external:Readable} class implementing a
 * consumer of Kinesis Data Streams using the
 * [enhanced fan-out feature]{@link external:enhancedFanOut}. Upon connection, instances of this
 * class will subscribe to receive data from all the shards of the given stream. Incoming data can
 * be retrieved through either the `data` event or by piping the instance to a writable stream.
 *
 * @alias module:lifion-kinesis
 * @extends external:Readable
 */
class Kinesis extends PassThrough {
  /**
   * Initializes a new instance of the Kinesis client.
   *
   * @param {Object} options - The initialization options. In addition to the below options, this
   *        object can also contain the [`AWS.Kinesis` options]{@link external:sdkOptions}.
   * @param {string} [options.compression] - The kind of data compression to use with records.
   *        The currently available compression options are either `"LZ-UTF8"` or none.
   * @param {string} options.consumerName - The unique name of the consumer for the given stream.
   *        This option is required.
   * @param {boolean} [options.createStreamIfNeeded=false] - Whether if the Kinesis stream should
   *        be created if it doesn't exist upon connection.
   * @param {Object} [options.compression] - The kind of compression to enforce in the stream.
   * @param {string} [options.compression.type] - The encryption type to use.
   * @param {string} [options.compression.keyId] - The GUID for the customer-managed AWS KMS key
   *        to use for encryption. This value can be a globally unique identifier, a fully
   *        specified ARN to either an alias or a key, or an alias name prefixed by "alias/".
   * @param {Object} [options.logger] - An object with the `warn`, `debug`, and `error` functions
   *        that will be used for logging purposes. If not provided, logging will be omitted.
   * @param {number} [options.shardCount=1] - The number of shards that the newly-created stream
   *        will use (if the `createStreamIfNeeded` option is set).
   * @param {string} options.streamName - The name of the stream to consume data from. This option
   *        is required.
   * @param {Object} [options.tags] - If provided, the client will ensure that the stream is tagged
   *        with these hash of tags upon connection. If the stream is already tagged same tag keys,
   *        they won't be overriden. If the stream is already tagged with different tag keys, they
   *        won't be removed.
   */
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

  /**
   * Initializes the Kinesis client, then it proceeds to:
   * 1. Create the stream if asked for.
   * 2. Ensure that the stream is active.
   * 3. Ensure that the stream is encrypted as indicated.
   * 4. Ensure that the stream is tagged as requested.
   * 5. Ensure an enhanced fan-out consumer with the given name exists.
   * 6. Ensure that the enhanced fan-out consumer is active.
   * 7. A subscription for data is issued to all the shard in the stream.
   * 8. Data will then be available in both [stream read modes]{@link external:readModes}.
   *
   * @returns {Promise}
   * @fulfil Once the stream is active, encrypted, tagged, the enhanced fan-out consumer is active,
   *    and the client is subscribed to the data in all the stream shards.
   * @reject {Error} - If at least one of the above steps fails to succeed.
   */
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

/**
 * @external Readable
 * @see https://nodejs.org/dist/latest-v10.x/docs/api/stream.html#stream_readable_streams
 */

/**
 * @external enhancedFanOut
 * @see https://docs.aws.amazon.com/streams/latest/dev/introduction-to-enhanced-consumers.html
 */

/**
 * @external sdkOptions
 * @see https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/Kinesis.html#constructor-property
 */

module.exports = Kinesis;
