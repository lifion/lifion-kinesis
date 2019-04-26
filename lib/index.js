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
const { getRecordsEncoder } = require('./records');
const { name: moduleName } = require('../package.json');
const { noop } = require('./utils');
const { getStats, reportRecordConsumed, startStatsEmitter } = require('./stats');

const MAX_ENHANCED_CONSUMERS = 5;

const {
  confirmStreamTags,
  ensureStreamEncription,
  ensureStreamExists,
  getEnhancedConsumers,
  registerEnhancedConsumer
} = require('./stream');

const privateData = new WeakMap();

function internal(instance) {
  if (!privateData.has(instance)) privateData.set(instance, {});
  return privateData.get(instance);
}

async function ensureStreamInitialized(instance) {
  const privateProps = internal(instance);
  startStatsEmitter(instance, privateProps);

  const { encryption, streamArn, streamCreatedOn, tags } = privateProps;
  if (streamArn && streamCreatedOn) return;

  Object.assign(privateProps, await ensureStreamExists(privateProps));
  if (encryption) await ensureStreamEncription(privateProps);
  if (tags) await confirmStreamTags(privateProps);
}

/**
 * If the `useEnhancedFanOut` option is enabled, this function will be called to prepare for the
 * automated distribution of the enhanced fan-out consumers into the consumers of this module on
 * the same consumer group. The preparation consist in the pre-registration of the maximum allowed
 * number of enhanced fan-out consumers in Amazon, and also in making sure that the state of the
 * stream reflects the existing enhanced consumers. Stale state will be removed, existing enhanced
 * consumers will be preserved.
 *
 * @param {Object} instance - A reference to the instance of the Kinesis class.
 * @returns {Promise}
 * @private
 */
async function setUpEnhancedConsumers(instance) {
  const { client, logger, stateStore, streamArn, streamName } = internal(instance);
  logger.debug(`Cleaning up enhanced consumers for "${streamName}"…`);

  // Retrieve the existing enhanced fan-out consumers for the stream.
  let enhancedConsumers = await getEnhancedConsumers({ client, logger, streamArn });
  const enhancedConsumersCount = Object.keys(enhancedConsumers).length;

  // Register new enhanced fan-out consumers until reaching the maximum allowed.
  for (let i = enhancedConsumersCount; i < MAX_ENHANCED_CONSUMERS; i += 1) {
    const consumerName = `${moduleName}-${generate()}`;
    await registerEnhancedConsumer({ client, consumerName, logger, streamArn });
  }

  // Retrieve the enhanced fan-out consumers again (will include the newly registered ones).
  enhancedConsumers = await getEnhancedConsumers({ client, logger, streamArn });

  // Make sure the stream state contains the newly registered consumers.
  await Promise.all(
    Object.keys(enhancedConsumers).map(consumerName => {
      const { arn } = enhancedConsumers[consumerName];
      return stateStore.registerEnhancedConsumer(consumerName, arn);
    })
  );

  // Get the enhanced consumers from the stream state.
  const enhancedConsumersState = await stateStore.getEnhancedConsumers();

  // Remove old enhanced fan-out consumers from the stream state.
  await Promise.all(
    Object.keys(enhancedConsumersState)
      .filter(consumerName => !Object.keys(enhancedConsumers).includes(consumerName))
      .map(async consumerName => stateStore.deregisterEnhancedConsumer(consumerName))
  );
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
   * @param {number} [options.noRecordsPollDelay=1000] - The delay in milliseconds before
   *        attempting to get more records when there were none in the previous attempt (only
   *        applicable when `useEnhancedFanOut` is set to `false`)
   * @param {number} [options.pollDelay=250] - When the `usePausedPolling` option is `false`, this
   *        option defines the delay in milliseconds in between poll requests for more records
   *        (only applicable when `useEnhancedFanOut` is set to `false`)
   * @param {number} [options.shardCount=1] - The number of shards that the newly-created stream
   *        will use (if the `createStreamIfNeeded` option is set)
   * @param {string} options.streamName - The name of the stream to consume data from (required)
   * @param {Object} [options.tags] - If provided, the client will ensure that the stream is tagged
   *        with these tags upon connection. If the stream is already tagged, the existing tags
   *        will be merged with the provided ones before updating them.
   * @param {boolean} [options.useAutoCheckpoints=true] - Set to `true` to make the client
   *        automatically store shard checkpoints using the sequence number of the most-recently
   *        received record. If set to `false` consumers can use the `setCheckpoint()` function to
   *        store any sequence number as the checkpoint for the shard.
   * @param {boolean} [options.useAutoShardAssignment=true] - Set to `true` to automatically assign
   *        the stream shards to the active consumers in the same group (so only one client reads
   *      from one shard at the same time). Set to `false` to make the client read from all shards.
   * @param {boolean} [options.useEnhancedFanOut=false] - Set to `true` to make the client use
   *        enhanced fan-out consumers to read from shards.
   * @param {boolean} [options.usePausedPolling=false] - Set to `true` to make the client not to
   *        poll for more records until the consumer calls `continuePolling()`. This option is
   *        useful when consumers want to make sure the records are fully processed before
   *        receiving more (only applicable when `useEnhancedFanOut` is set to `false`)
   */
  constructor(options = {}) {
    super({ objectMode: true });

    const {
      compression,
      consumerGroup = projectName(process.cwd()),
      createStreamIfNeeded = true,
      dynamoDb = {},
      encryption,
      logger = {},
      noRecordsPollDelay = 1000,
      pollDelay = 250,
      shardCount,
      statsInterval,
      streamName,
      tags,
      useAutoCheckpoints = true,
      useAutoShardAssignment = true,
      useEnhancedFanOut = false,
      usePausedPolling = false,
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

    const normNoRecordsPollDelay = Number(noRecordsPollDelay);
    const normPollDelay = Number(pollDelay);
    const normShardCount = Number(shardCount);

    Object.assign(internal(this), {
      awsOptions,
      client: new KinesisClient({ awsOptions, logger: normLogger, streamName }),
      compression,
      consumerGroup,
      consumerId: generate(),
      createStreamIfNeeded,
      dynamoDb,
      encryption,
      logger: normLogger,
      noRecordsPollDelay: normNoRecordsPollDelay >= 250 ? normNoRecordsPollDelay : 250,
      pollDelay: normPollDelay >= 0 ? normPollDelay : 250,
      recordsEncoder: getRecordsEncoder(compression, 'Buffer'),
      shardCount: normShardCount >= 1 ? normShardCount : 1,
      statsInterval,
      streamName,
      tags,
      useAutoCheckpoints: Boolean(useAutoCheckpoints),
      useAutoShardAssignment: Boolean(useAutoShardAssignment),
      useEnhancedFanOut: Boolean(useEnhancedFanOut),
      usePausedPolling: Boolean(usePausedPolling)
    });
  }

  /**
   * Initializes the client, by ensuring that the stream exists, it's ready, and configured as
   * requested. The internal managers that deal with heartbeats, state, and consumers will also
   * be started.
   *
   * @fulfil Once the client has successfully started.
   * @reject {Error} - On any unexpected error while trying to start.
   * @returns {Promise}
   */
  async startConsumer() {
    const privateProps = internal(this);
    const { logger, streamName, useEnhancedFanOut } = privateProps;

    await ensureStreamInitialized(this);

    logger.debug('Trying to start the consumer…');

    const stateStore = new StateStore(privateProps);
    privateProps.stateStore = stateStore;
    await stateStore.start();

    if (useEnhancedFanOut) await setUpEnhancedConsumers(this);

    const heartbeatManager = new HeartbeatManager(privateProps);
    privateProps.heartbeatManager = heartbeatManager;
    await heartbeatManager.start();

    privateProps.pushToStream = (err, ...args) => {
      if (err) this.emit('error', err);
      else {
        this.push(...args);
        reportRecordConsumed(streamName);
      }
    };

    const consumersManager = new ConsumersManager(privateProps);
    privateProps.consumersManager = consumersManager;

    const leaseManager = new LeaseManager(privateProps);
    privateProps.leaseManager = leaseManager;
    await leaseManager.start();

    logger.debug('The consumer is now ready.');
  }

  /**
   * Puts a record to a stream.
   *
   * @param {Object} params - The putRecord parameters. In addition to the params described here,
   *        uses [`AWS.Kinesis.putRecord` parameters]{@link external:AwsJsSdkKinesisPutRecord}
   *        from the `AWS.Kinesis.putRecord` method in camel case.
   * @param {(Object|string)} params.data - The data to be used as the Kinesis message.
   * @param {string} [params.streamName] - If provided, overrides the stream name provided on
   *        client instantiation.
   * @fulfil If record is successfully pushed to stream.
   * @reject {Error} - On any unexpected error while pushing to stream.
   * @returns {Promise}
   */
  async putRecord({ streamName, ...record }) {
    const privateProps = internal(this);
    const { client, recordsEncoder } = privateProps;
    await ensureStreamInitialized(this);
    await client.putRecord({
      ...(await recordsEncoder(record)),
      StreamName: streamName || privateProps.streamName
    });
  }

  /**
   * Batch puts multiple records to a stream.
   *
   * @param {Object} params - The putRecords parameters. In addition to the params described here,
   *        uses [`AWS.Kinesis.putRecords` parameters]{@link external:AwsJsSdkKinesisPutRecords}
   *        from the `AWS.Kinesis.putRecords` method in camel case.
   * @param {Array} params.records - A list of records to push to a Kinesis stream.
   * @param {(Object|string)} params.records.data - The data to be used as the Kinesis message.
   * @param {string} [params.streamName] - If provided, overrides the stream name provided
   *        on client instantiation.
   * @fulfil If records are successfully pushed to stream.
   * @reject {Error} - On any unexpected error while pushing to stream.
   * @returns {Promise}
   */
  async putRecords({ records, streamName }) {
    const privateProps = internal(this);
    const { client, recordsEncoder } = privateProps;
    await ensureStreamInitialized(this);
    return client.putRecords({
      Records: await Promise.all(records.map(recordsEncoder)),
      StreamName: streamName || privateProps.streamName
    });
  }

  getStats() {
    const { streamName } = internal(this);
    return getStats(streamName);
  }

  static getStats() {
    return getStats();
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
 * @external AwsJsSdkKinesisPutRecord
 * @see https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/Kinesis.html#putRecord-property
 */

/**
 * @external AwsJsSdkKinesisPutRecords
 * @see https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/Kinesis.html#putRecords-property
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
