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
const S3Client = require('./s3-client');
const LeaseManager = require('./lease-manager');
const StateStore = require('./state-store');
const RecordsModule = require('./records');
const {
  confirmBucketLifecycleConfiguration,
  confirmBucketTags,
  ensureBucketExists
} = require('./bucket');
const { getStats, reportRecordConsumed } = require('./stats');
const { name: moduleName } = require('../package.json');

const {
  confirmStreamTags,
  ensureStreamEncription,
  ensureStreamExists,
  getEnhancedConsumers,
  registerEnhancedConsumer
} = require('./stream');

const MAX_ENHANCED_CONSUMER_PER_CREATION = 5;

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
 * Ensures a Kinesis stream exists and that is encrypted and tagged as required.
 *
 * @param {object} instance - The instance of the Kinesis class where the call originated from.
 * @param {string} [streamName] - The name of the stream to check initialization for.
 * @fulfil {undefined}
 * @returns {Promise}
 * @private
 */
async function ensureStreamInitialized(instance, streamName) {
  const privateProps = internal(instance);
  const { compression, logger, s3, useS3ForLargeItems } = privateProps;

  let params;
  let s3Client;

  if (!streamName || streamName === privateProps.streamName) {
    const { streamArn, streamCreatedOn } = await ensureStreamExists(privateProps);
    Object.assign(privateProps, { streamArn, streamCreatedOn });
    params = privateProps;

    if (useS3ForLargeItems) {
      s3Client = useS3ForLargeItems && new S3Client({ ...s3, logger });
      const { bucketName, tags: s3Tags } = s3;
      await ensureBucketExists({ bucketName, client: s3Client, logger });
      await confirmBucketTags({ bucketName, client: s3Client, logger, tags: s3Tags });
      await confirmBucketLifecycleConfiguration({
        bucketName,
        client: s3Client,
        logger,
        streamName: params.streamName
      });
    }
  } else {
    params = { ...privateProps, streamName };
    await ensureStreamExists(params);
  }
  const { encryption, tags } = params;
  if (encryption) await ensureStreamEncription(params);
  if (tags) await confirmStreamTags(params);

  privateProps.s3Client = s3Client;

  privateProps.recordsEncoder = RecordsModule.getRecordsEncoder({
    compression,
    outputEncoding: 'Buffer',
    s3,
    s3Client,
    streamName: params.streamName,
    useS3ForLargeItems
  });
}

/**
 * If the `useEnhancedFanOut` option is enabled, this function will be called to prepare for the
 * automated distribution of the enhanced fan-out consumers into the consumers of this module on
 * the same consumer group. The preparation consist in the pre-registration of the maximum allowed
 * number of enhanced fan-out consumers in Amazon, and also in making sure that the state of the
 * stream reflects the existing enhanced consumers. Stale state will be removed, existing enhanced
 * consumers will be preserved.
 *
 * @param {object} instance - A reference to the instance of the Kinesis class.
 * @returns {Promise}
 * @private
 */
async function setUpEnhancedConsumers(instance) {
  const { client, logger, maxEnhancedConsumers, stateStore, streamArn, streamName } = internal(
    instance
  );
  logger.debug(`Cleaning up enhanced consumers for "${streamName}"…`);

  // Retrieve the existing enhanced fan-out consumers for the stream.
  let enhancedConsumers = await getEnhancedConsumers({ client, logger, streamArn });
  const enhancedConsumersCount = Object.keys(enhancedConsumers).length;

  // Register new enhanced fan-out consumers until reaching the maximum allowed.
  const newEnhancedConsumerNames = [];
  for (let i = enhancedConsumersCount; i < maxEnhancedConsumers; i += 1) {
    newEnhancedConsumerNames.push(`${moduleName}-${generate()}`);
  }

  const newEnhancedConsumerBatches = newEnhancedConsumerNames.reduce((batches, item, index) => {
    const newBatch = index % MAX_ENHANCED_CONSUMER_PER_CREATION === 0;
    if (newBatch) {
      batches.push(
        newEnhancedConsumerNames.slice(index, index + MAX_ENHANCED_CONSUMER_PER_CREATION)
      );
    }
    return batches;
  }, []);

  for (const batch of newEnhancedConsumerBatches) {
    await Promise.all(
      batch.map((consumerName) =>
        registerEnhancedConsumer({ client, consumerName, logger, streamArn })
      )
    );
  }

  // Retrieve the enhanced fan-out consumers again (will include the newly registered ones).
  enhancedConsumers = await getEnhancedConsumers({ client, logger, streamArn });

  // Make sure the stream state contains the newly registered consumers.
  await Promise.all(
    Object.keys(enhancedConsumers).map((consumerName) => {
      const { arn } = enhancedConsumers[consumerName];
      return stateStore.registerEnhancedConsumer(consumerName, arn);
    })
  );

  // Get the enhanced consumers from the stream state.
  const enhancedConsumersState = await stateStore.getEnhancedConsumers();

  // Remove old enhanced fan-out consumers from the stream state.
  await Promise.all(
    Object.keys(enhancedConsumersState)
      .filter((consumerName) => !Object.keys(enhancedConsumers).includes(consumerName))
      .map(async (consumerName) => stateStore.deregisterEnhancedConsumer(consumerName))
  );
}

function parsePutRecordResult({ EncryptionType, SequenceNumber, ShardId }) {
  return {
    encryptionType: EncryptionType,
    sequenceNumber: SequenceNumber,
    shardId: ShardId
  };
}

function parsePutRecordsResult({ EncryptionType, Records }) {
  return {
    encryptionType: EncryptionType,
    records: Records.map(({ SequenceNumber, ShardId }) => ({
      sequenceNumber: SequenceNumber,
      shardId: ShardId
    }))
  };
}

/**
 * A [pass-through stream]{@link external:PassThrough} class specialization implementing a consumer
 * of Kinesis Data Streams using the [AWS SDK for JavaScript]{@link external:AwsJsSdk}. Incoming
 * data can be retrieved through either the `data` event or by piping the instance to other streams.
 *
 * @alias module:lifion-kinesis
 * @augments external:PassThrough
 */
class Kinesis extends PassThrough {
  /**
   * Initializes a new instance of the Kinesis client.
   *
   * @param {object} options - The initialization options. In addition to the below options, it
   *        can also contain any of the [`AWS.Kinesis` options]{@link external:AwsJsSdkKinesis}.
   * @param {string} [options.compression] - The kind of data compression to use with records.
   *        The currently available compression options are either `"LZ-UTF8"` or none.
   * @param {string} [options.consumerGroup] - The name of the group of consumers in which shards
   *        will be distributed and checkpoints will be shared. If not provided, it defaults to
   *        the name of the application/project using this module.
   * @param {boolean} [options.createStreamIfNeeded=true] - Whether if the Kinesis stream should
   *        be automatically created if it doesn't exist upon connection
   * @param {object} [options.dynamoDb={}] - The initialization options for the DynamoDB client
   *        used to store the state of the consumers. In addition to `tableNames` and `tags`, it
   *        can also contain any of the [`AWS.DynamoDB` options]{@link external:AwsJsSdkDynamoDb}.
   * @param {string} [options.dynamoDb.tableName] - The name of the table in which to store the
   *        state of consumers. If not provided, it defaults to "lifion-kinesis-state".
   * @param {object} [options.dynamoDb.tags] - If provided, the client will ensure that the
   *        DynamoDB table where the state is stored is tagged with these tags. If the table
   *        already has tags, they will be merged.
   * @param {object} [options.encryption] - The encryption options to enforce in the stream.
   * @param {string} [options.encryption.type] - The encryption type to use.
   * @param {string} [options.encryption.keyId] - The GUID for the customer-managed AWS KMS key
   *        to use for encryption. This value can be a globally unique identifier, a fully
   *        specified ARN to either an alias or a key, or an alias name prefixed by "alias/".
   * @param {number} [options.limit=10000] - The limit of records per get records call (only
   *        applicable with `useEnhancedFanOut` is set to `false`)
   * @param {object} [options.logger] - An object with the `warn`, `debug`, and `error` functions
   *        that will be used for logging purposes. If not provided, logging will be omitted.
   * @param {number} [options.maxEnhancedConsumers=5] - An option to set the number of enhanced
   *        fan-out consumer ARNs that the module should initialize. Defaults to 5.
   *        Providing a number above the AWS limit (20) or below 1 will result in using the default.
   * @param {number} [options.noRecordsPollDelay=1000] - The delay in milliseconds before
   *        attempting to get more records when there were none in the previous attempt (only
   *        applicable when `useEnhancedFanOut` is set to `false`)
   * @param {number} [options.pollDelay=250] - When the `usePausedPolling` option is `false`, this
   *        option defines the delay in milliseconds in between poll requests for more records
   *        (only applicable when `useEnhancedFanOut` is set to `false`)
   * @param {object} [options.s3={}] - The initialization options for the S3 client used
   *        to store large items in buckets. In addition to `bucketName` and `endpoint`, it
   *        can also contain any of the [`AWS.S3` options]{@link external:AwsJsSdkS3}.
   * @param {string} [options.s3.bucketName] - The name of the bucket in which to store
   *        large messages. If not provided, it defaults to the name of the Kinesis stream.
   * @param {number} [options.s3.largeItemThreshold=900] - The size in KB above which an item
   *        should automatically be stored in s3.
   * @param {Array<string>} [options.s3.nonS3Keys=[]] - If the `useS3ForLargeItems` option is set to
   *        `true`, the `nonS3Keys` option lists the keys that will be sent normally on the kinesis record.
   * @param {string} [options.s3.tags] - If provided, the client will ensure that the
   *        S3 bucket is tagged with these tags. If the bucket already has tags, they will be merged.
   * @param {number} [options.shardCount=1] - The number of shards that the newly-created stream
   *        will use (if the `createStreamIfNeeded` option is set)
   * @param {string|boolean} [options.shouldParseJson=auto] - Whether if retrieved records' data should be parsed as JSON or not.
   *        Set to "auto" to only attempt parsing if data looks like JSON. Set to true to force data parse.
   * @param {number} [options.statsInterval=30000] - The interval in milliseconds for how often to
   *        emit the "stats" event. The event is only available while the consumer is running.
   * @param {string} options.streamName - The name of the stream to consume data from (required)
   * @param {boolean} [options.supressThroughputWarnings=false] - Set to `true` to make the client
   *        log ProvisionedThroughputExceededException as debug rather than warning.
   * @param {object} [options.tags] - If provided, the client will ensure that the stream is tagged
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
   * @param {boolean} [options.useS3ForLargeItems=false] - Whether to automatically use an S3
   *        bucket to store large items or not.
   */
  constructor(options = {}) {
    super({ objectMode: true });

    const {
      compression,
      consumerGroup = projectName(process.cwd()),
      createStreamIfNeeded = true,
      dynamoDb = {},
      encryption,
      limit = 10000,
      logger = {},
      maxEnhancedConsumers = 5,
      noRecordsPollDelay = 1000,
      pollDelay = 250,
      s3 = {},
      shardCount = 1,
      shouldParseJson = 'auto',
      statsInterval = 30000,
      streamName,
      supressThroughputWarnings = false,
      tags,
      useAutoCheckpoints = true,
      useAutoShardAssignment = true,
      useEnhancedFanOut = false,
      usePausedPolling = false,
      useS3ForLargeItems = false,
      ...awsOptions
    } = options;

    const normLogger = {
      debug: typeof logger.debug === 'function' ? logger.debug.bind(logger) : Function.prototype,
      error: typeof logger.error === 'function' ? logger.error.bind(logger) : Function.prototype,
      warn: typeof logger.warn === 'function' ? logger.warn.bind(logger) : Function.prototype
    };

    if (!streamName) {
      const errorMsg = 'The "streamName" option is required.';
      normLogger.error(errorMsg);
      throw new TypeError(errorMsg);
    }

    const limitNumber = Number(limit);
    const maxConsumersNumber = Number(maxEnhancedConsumers);
    const noRecordsPollDelayNumber = Number(noRecordsPollDelay);
    const pollDelayNumber = Number(pollDelay);
    const shardCountNumber = Number(shardCount);
    const statsIntervalNumber = Number(statsInterval);
    const largeItemThresholdNumber = Number(s3.largeItemThreshold || 900);

    const s3BucketName = useS3ForLargeItems && (s3.bucketName || streamName);
    const recordsEncoder = useS3ForLargeItems
      ? null
      : RecordsModule.getRecordsEncoder({
          compression,
          outputEncoding: 'Buffer',
          streamName
        });

    Object.assign(internal(this), {
      awsOptions,
      client: new KinesisClient({
        awsOptions,
        logger: normLogger,
        streamName,
        supressThroughputWarnings
      }),
      compression,
      consumerGroup,
      consumerId: generate(),
      createStreamIfNeeded,
      dynamoDb,
      encryption,
      getStatsIntervalId: null,
      limit: limitNumber > 0 && limitNumber <= 10000 ? limitNumber : 10000,
      logger: normLogger,
      maxEnhancedConsumers:
        maxConsumersNumber > 0 && maxConsumersNumber <= 20 ? maxConsumersNumber : 5,
      noRecordsPollDelay: noRecordsPollDelayNumber >= 250 ? noRecordsPollDelayNumber : 250,
      pollDelay: pollDelayNumber >= 0 ? pollDelayNumber : 250,
      recordsEncoder,
      s3: {
        largeItemThreshold: largeItemThresholdNumber,
        nonS3Keys: [],
        ...s3,
        bucketName: s3BucketName
      },
      s3Client: null,
      shardCount: shardCountNumber >= 1 ? shardCountNumber : 1,
      shouldParseJson,
      statsInterval: statsIntervalNumber >= 1000 ? statsIntervalNumber : 30000,
      streamName,
      tags,
      useAutoCheckpoints: Boolean(useAutoCheckpoints),
      useAutoShardAssignment: Boolean(useAutoShardAssignment),
      useEnhancedFanOut: Boolean(useEnhancedFanOut),
      usePausedPolling: Boolean(usePausedPolling),
      useS3ForLargeItems
    });
  }

  /**
   * Starts the stream consumer, by ensuring that the stream exists, that it's ready, and
   * configured as requested. The internal managers that deal with heartbeats, state, and
   * consumers will also be started.
   *
   * @fulfil {undefined} - Once the consumer has successfully started.
   * @reject {Error} - On any unexpected error while trying to start.
   * @returns {Promise}
   */
  async startConsumer() {
    const privateProps = internal(this);
    const { logger, statsInterval, streamName, useEnhancedFanOut } = privateProps;

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
    await consumersManager.reconcile();

    const leaseManager = new LeaseManager(privateProps);
    privateProps.leaseManager = leaseManager;
    await leaseManager.start();

    const getStatsTimeout = () => {
      this.emit('stats', getStats(streamName));
      privateProps.getStatsIntervalId = setTimeout(getStatsTimeout, statsInterval);
    };
    privateProps.getStatsIntervalId = setTimeout(getStatsTimeout, statsInterval);

    logger.debug('The consumer is now ready.');
  }

  /**
   * Stops the stream consumer. The internal managers will also be stopped.
   */
  stopConsumer() {
    const privateProps = internal(this);
    const { consumersManager, getStatsIntervalId, heartbeatManager, leaseManager } = privateProps;
    heartbeatManager.stop();
    consumersManager.stop();
    leaseManager.stop();
    clearTimeout(getStatsIntervalId);
    privateProps.getStatsIntervalId = null;
  }

  /**
   * Writes a single data record into a stream.
   *
   * @param {object} params - The parameters.
   * @param {*} params.data - The data to put into the record.
   * @param {string} [params.explicitHashKey] - The hash value used to explicitly determine the
   *        shard the data record is assigned to by overriding the partition key hash.
   * @param {string} [params.partitionKey] - Determines which shard in the stream the data record
   *        is assigned to. If omitted, it will be calculated based on a SHA-1 hash of the data.
   * @param {string} [params.sequenceNumberForOrdering] - Set this to the sequence number obtained
   *        from the last put record operation to guarantee strictly increasing sequence numbers,
   *        for puts from the same client and to the same partition key. If omitted, records are
   *        coarsely ordered based on arrival time.
   * @param {string} [params.streamName] - If provided, the record will be put into the specified
   *        stream instead of the stream name provided during the consumer instantiation.
   * @fulfil {Object} - The de-serialized data returned from the request.
   * @reject {Error} - On any unexpected error while writing to the stream.
   * @returns {Promise}
   */
  async putRecord(params = {}) {
    const privateProps = internal(this);
    const { client, createStreamIfNeeded } = privateProps;
    const { streamName, ...record } = params;

    if (!privateProps.recordsEncoder) {
      await ensureStreamInitialized(this, streamName);
    }

    const awsParams = {
      ...(await privateProps.recordsEncoder(record)),
      StreamName: streamName || privateProps.streamName
    };
    try {
      return parsePutRecordResult(await client.putRecord(awsParams));
    } catch (err) {
      const { code } = err;
      const streamDoesNotExist =
        code === 'ResourceNotFoundException' ||
        (code === 'UnknownError' && client.isEndpointLocal());
      if (createStreamIfNeeded && streamDoesNotExist) {
        await ensureStreamInitialized(this, streamName);
        return parsePutRecordResult(await client.putRecord(awsParams));
      }
      throw err;
    }
  }

  /**
   * List the shards of a stream.
   *
   * @param {object} params - The parameters.
   * @param {string} [params.streamName] - If provided, the method will list the shards of the
   *        specific stream instead of the stream name provided during the consumer instantiation.
   * @fulfil {Object} - The de-serialized data returned from the request.
   * @reject {Error} - On any unexpected error while writing to the stream.
   * @returns {Promise}
   */
  async listShards(params = {}) {
    const privateProps = internal(this);
    const { client } = privateProps;
    const { streamName } = params;

    const awsParams = {
      StreamName: streamName || privateProps.streamName
    };

    const { Shards } = await client.listShards(awsParams);
    return Shards.map(
      ({
        HashKeyRange: { EndingHashKey, StartingHashKey },
        SequenceNumberRange: { StartingSequenceNumber },
        ShardId
      }) => ({
        hashKeyRange: { endingHashKey: EndingHashKey, startingHashKey: StartingHashKey },
        sequenceNumberRange: { startingSequenceNumber: StartingSequenceNumber },
        shardId: ShardId
      })
    );
  }

  /**
   * Writes multiple data records into a stream in a single call.
   *
   * @param {object} params - The parameters.
   * @param {Array<object>} params.records - The records associated with the request.
   * @param {*} params.records[].data - The record data.
   * @param {string} [params.records[].explicitHashKey] - The hash value used to explicitly
   *        determine the shard the data record is assigned to by overriding the partition key hash.
   * @param {string} [params.records[].partitionKey] - Determines which shard in the stream the
   *        data record is assigned to. If omitted, it will be calculated based on a SHA-1 hash
   *        of the data.
   * @param {string} [params.streamName] - If provided, the record will be put into the specified
   *        stream instead of the stream name provided during the consumer instantiation.
   * @fulfil {Object} - The de-serialized data returned from the request.
   * @reject {Error} - On any unexpected error while writing to the stream.
   * @returns {Promise}
   */
  async putRecords(params = {}) {
    const privateProps = internal(this);
    const { client, createStreamIfNeeded } = privateProps;
    const { records, streamName } = params;

    if (!privateProps.recordsEncoder) {
      await ensureStreamInitialized(this, streamName);
    }

    if (!Array.isArray(records)) throw new TypeError('The "records" property is required.');
    const awsParams = {
      Records: await Promise.all(records.map(privateProps.recordsEncoder)),
      StreamName: streamName || privateProps.streamName
    };
    try {
      return parsePutRecordsResult(await client.putRecords(awsParams));
    } catch (err) {
      const { code } = err;
      const streamDoesNotExist =
        code === 'ResourceNotFoundException' ||
        (code === 'UnknownError' && client.isEndpointLocal());
      if (createStreamIfNeeded && streamDoesNotExist) {
        await ensureStreamInitialized(this, streamName);
        return parsePutRecordsResult(await client.putRecords(awsParams));
      }
      throw err;
    }
  }

  /**
   * Returns statistics for the instance of the client.
   *
   * @returns {object} An object with the statistics.
   */
  getStats() {
    const { streamName } = internal(this);
    return getStats(streamName);
  }

  /**
   * Returns the aggregated statistics of all the instances of the client.
   *
   * @returns {object} An object with the statistics.
   */
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
 * @external AwsJsSdkDynamoDb
 * @see https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#constructor-property
 */

/**
 * @external AwsJsSdkS3
 * @see https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/S3.html#constructor-property
 */

/**
 * @external PassThrough
 * @see https://nodejs.org/dist/latest-v10.x/docs/api/stream.html#stream_class_stream_passthrough
 */

module.exports = Kinesis;
