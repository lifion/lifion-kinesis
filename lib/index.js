/**
 * Lifion's Node.js client for Amazon Kinesis Data Streams.
 *
 * @module lifion-kinesis
 */

'use strict';

const projectName = require('project-name');
const { PassThrough } = require('stream');
const { generate } = require('short-uuid');

const ConsumersManager = require('./consumers-manager');
const HeartbeatManager = require('./heartbeat-manager');
const KinesisClient = require('./kinesis-client');
const LeaseManager = require('./lease-manager');
const StateStore = require('./state-store');
const { confirmStreamTags, ensureStreamEncription, ensureStreamExists } = require('./stream');
const { noop } = require('./utils');

const privateData = new WeakMap();

function internal(instance) {
  if (!privateData.has(instance)) privateData.set(instance, {});
  return privateData.get(instance);
}

/**
 * A [pass-through stream]{@link external:NodeJsPassThrough} class specialization implementing a
 * consumer of Kinesis Data Streams using the [AWS SDK for JavaScript]{@link external:AwsJsSdk}.
 * Incoming data can be retrieved through either the `data` event or by piping the instance to a
 * writable stream.
 *
 * @alias module:lifion-kinesis
 * @extends external:Readable
 */
class Kinesis extends PassThrough {
  /**
   * Initializes a new instance of the Kinesis client.
   *
   * @param {Object} options - The initialization options. In addition to the below options, it
   *        can also contain any of the [`AWS.Kinesis` options]{@link external:AwsJsSdkKinesis}.
   * @param {string} [options.compression] - The kind of data compression to use with records.
   *        The currently available compression options are either `"LZ-UTF8"` or none.
   * @param {string} [options.consumerGroup] - The name of the group of consumers in which shards
   *        will be distributed and checkpoints will be shared. If not provided, it defaults to
   *        the name of the application/project using this module.
   * @param {boolean} [options.createStreamIfNeeded=true] - Whether if the Kinesis stream should
   *        be automatically created if it doesn't exist upon connection
   * @param {Object} [options.dynamoDb={}] - The initialization options for the DynamoDB client
   *        used to store the state of the stream consumers. In addition to `tableNames` and
   *        `tags`, it can also contain any of the [`AWS.DynamoDB` options]{@link AwsJsSdkDynamoDb}.
   * @param {string} [options.dynamoDb.tableName] - The name of the table in which to store the
   *        state of consumers. If not provided, it defaults to "lifion-kinesis-state".
   * @param {Object} [options.dynamoDb.tags] - If provided, the client will ensure that the
   *        DynamoDB table where the state is stored is tagged with these tags. If the table
   *        already has tags, they will be merged.
   * @param {Object} [options.encryption] - The encryption options to enforce in the stream.
   * @param {string} [options.encryption.type] - The encryption type to use.
   * @param {string} [options.encryption.keyId] - The GUID for the customer-managed AWS KMS key
   *        to use for encryption. This value can be a globally unique identifier, a fully
   *        specified ARN to either an alias or a key, or an alias name prefixed by "alias/".
   * @param {Object} [options.logger] - An object with the `warn`, `debug`, and `error` functions
   *        that will be used for logging purposes. If not provided, logging will be omitted.
   * @param {number} [options.shardCount=1] - The number of shards that the newly-created stream
   *        will use (if the `createStreamIfNeeded` option is set).
   * @param {string} options.streamName - The name of the stream to consume data from. This option
   *        is required.
   * @param {Object} [options.tags] - If provided, the client will ensure that the stream is tagged
   *        with these tags upon connection. If the stream is already tagged, the existing tags
   *        will be merged with the provided ones before updating them.
   * @param {boolean} [options.useAutoCheckpoints=true] - Set to `true` to automatically store
   *        shard checkpoints for the consumer group as messages are reported as processed.
   * @param {boolean} [options.useAutoShardAssignment=true] - Set to `true` to automatically assign
   *        the stream shards to the active consumers in the same group (so only one client reads
   *      from one shard at the same time). Set to `false` to make the client read from all shards.
   * @param {boolean} [options.useEnhancedFanOut=false] - Set to `true` to make the client use
   *        enhanced fan-out consumers to read from shards.
   */
  constructor(options = {}) {
    super({ objectMode: true });

    const {
      compression,
      consumerGroup = projectName(process.cwd()),
      createStreamIfNeeded = true,
      dynamoDb,
      encryption,
      logger = {},
      shardCount,
      streamName,
      tags,
      useAutoCheckpoints = true,
      useAutoShardAssignment = true,
      useEnhancedFanOut = false,
      ...awsOptions
    } = options;

    const normLogger = {
      debug: typeof logger.debug === 'function' ? logger.debug.bind(logger) : noop,
      error: typeof logger.error === 'function' ? logger.error.bind(logger) : noop,
      warn: typeof logger.warn === 'function' ? logger.warn.bind(logger) : noop
    };

    if (!streamName) {
      const errorMsg = 'The "streamName" option is required.';
      normLogger.error(errorMsg);
      throw new TypeError(errorMsg);
    }

    const normShardCount = Number(shardCount);

    Object.assign(internal(this), {
      awsOptions,
      compression,
      consumerGroup,
      consumerId: generate(),
      createStreamIfNeeded,
      dynamoDb,
      encryption,
      logger: normLogger,
      shardCount: normShardCount >= 1 ? normShardCount : 1,
      streamName,
      tags,
      useAutoCheckpoints: Boolean(useAutoCheckpoints),
      useAutoShardAssignment: Boolean(useAutoShardAssignment),
      useEnhancedFanOut: Boolean(useEnhancedFanOut)
    });
  }

  /**
   * Initializes the client, by ensuring that the stream exists, it's ready, and configured as
   * requested. The internal managers that deal with heartbeats, state, and consumers will also
   * be started.
   *
   * @fulfil Once the client has successfully started.
   * @reject {Error} - On any unexpected error while trying to start.
   */
  async start() {
    const privateProps = internal(this);

    const { awsOptions, encryption, logger, tags } = privateProps;
    logger.debug('Trying to start the client…');

    privateProps.client = new KinesisClient(awsOptions);

    privateProps.streamArn = await ensureStreamExists(privateProps);
    if (encryption) await ensureStreamEncription(privateProps);
    if (tags) await confirmStreamTags(privateProps);

    const stateStore = new StateStore(privateProps);
    privateProps.stateStore = stateStore;
    await stateStore.start();

    const heartbeatManager = new HeartbeatManager(privateProps);
    privateProps.heartbeatManager = heartbeatManager;
    await heartbeatManager.start();

    privateProps.pushToStream = (...args) => this.push(...args);

    const consumersManager = new ConsumersManager(privateProps);
    privateProps.consumersManager = consumersManager;

    const leaseManager = new LeaseManager(privateProps);
    privateProps.leaseManager = leaseManager;
    await leaseManager.start();

    logger.debug('The client is now ready.');
  }
}

/**
 * @external AwsJsSdk
 * @see https://docs.aws.amazon.com/AWSJavaScriptSDK/latest
 */

/**
 * @external AwsJsSdkKinesis
 * @see https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/Kinesis.html#constructor-property
 */

/**
 * @external AwsJsSdkDynamoDb
 * @see https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#constructor-property
 */

/**
 * @external NodeJsPassThrough
 * @see https://nodejs.org/dist/latest-v10.x/docs/api/stream.html#stream_class_stream_passthrough
 */

module.exports = Kinesis;
