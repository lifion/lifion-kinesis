/**
 * Module that ensures there are active consumers for the shards with an active lease.
 *
 * @module consumers-manager
 * @private
 */

'use strict';

const FanOutConsumer = require('./fan-out-consumer');
const PollingConsumer = require('./polling-consumer');

const privateData = new WeakMap();

/**
 * Provides access to the private data of the specified instance.
 *
 * @param {Object} instance - The private data's owner.
 * @returns {Object} The private data.
 * @private
 */
function internal(instance) {
  if (!privateData.has(instance)) privateData.set(instance, {});
  return privateData.get(instance);
}

/**
 * Class that implements the consumers manager module.
 *
 * @alias module:consumers-manager
 */
class ConsumersManager {
  /**
   * Initializes an instance of the consumers manager.
   *
   * @param {Object} options - The initialization options.
   * @param {Object} options.awsOptions - The initialization options for AWS.Kinesis.
   * @param {Object} options.client - An instance of the Kinesis client.
   * @param {string} options.compression - The kind of data compression to use with records.
   * @param {number} options.limit - The limit of records per get records call.
   * @param {Object} options.logger - An instance of a logger.
   * @param {number} options.noRecordsPollDelay - The delay in milliseconds before attempting to
   *        get more records when there were none in the previous attempt.
   * @param {number} options.pollDelay - When the `usePausedPolling` option is `false`, this
   *        option defines the delay in milliseconds in between poll requests for more records.
   * @param {Function} options.pushToStream - A function to push incoming records to the consumer.
   * @param {Object} options.s3 - The S3 options in the current kinesis client.
   * @param {string|boolean} [options.shouldParseJson] - Whether if retrieved records' data should
   *        be parsed as JSON or not.
   * @param {Object} options.stateStore - An instance of the state store.
   * @param {string} options.streamName - The name of the Kinesis stream.
   * @param {boolean} options.useAutoCheckpoints - Whether to automatically store shard checkpoints
   *        using the sequence number of the most-recently received record or not.
   * @param {boolean} options.useEnhancedFanOut - Whether if the consumer is using enhanced
   *        fan-out shard consumers or not.
   * @param {boolean} options.usePausedPolling - Whether if the client is waiting for
   *        user-intervention before polling for more records, or not.
   * @param {boolean} options.useS3ForLargeItems - Whether to automatically use an S3
   *        bucket to store large items or not.
   */
  constructor(options) {
    const {
      awsOptions,
      client,
      compression,
      limit,
      logger,
      noRecordsPollDelay,
      pollDelay,
      pushToStream,
      s3,
      s3Client,
      shouldDeaggregate,
      shouldParseJson,
      stateStore,
      streamName,
      useAutoCheckpoints,
      useEnhancedFanOut,
      usePausedPolling,
      useS3ForLargeItems
    } = options;

    Object.assign(internal(this), {
      awsOptions,
      client,
      compression,
      consumers: {},
      limit,
      logger,
      noRecordsPollDelay,
      pollDelay,
      pushToStream,
      s3,
      s3Client,
      shouldDeaggregate,
      shouldParseJson,
      stateStore,
      streamName,
      useAutoCheckpoints,
      useEnhancedFanOut,
      usePausedPolling,
      useS3ForLargeItems
    });
  }

  /**
   * Triggers the reconciliation of shard consumers where new instances of either the fan-out or
   * polling consumers will be initialized for newly acquired shard leases, or where running
   * consumers will be stopped for lost or expired shard leases.
   *
   * @fulfil {undefined}
   * @returns {Promise}
   */
  async reconcile() {
    const {
      awsOptions,
      client,
      compression,
      consumers,
      limit,
      logger,
      noRecordsPollDelay,
      pollDelay,
      pushToStream,
      s3,
      s3Client,
      shouldDeaggregate,
      shouldParseJson,
      stateStore,
      streamName,
      useAutoCheckpoints,
      useEnhancedFanOut,
      usePausedPolling,
      useS3ForLargeItems
    } = internal(this);

    logger.debug('Reconciling shard consumers…');

    const stopConsumer = (shardId) => {
      const consumer = consumers[shardId];
      if (consumer) {
        try {
          logger.debug(`Stopping the consumer for "${shardId}"…`);
          consumer.stop();
        } catch (err) {
          logger.error('Unexpected recoverable failure when trying to stop a consumer:', err);
        }
        consumers[shardId] = undefined;
      }
    };

    // If using enhanced fan-out, make sure there's an assigned enhanced consumer.
    let consumerArn;
    if (useEnhancedFanOut) {
      consumerArn = await stateStore.getAssignedEnhancedConsumer();
      if (!consumerArn) {
        Object.keys(consumers).forEach(stopConsumer);
        return;
      }
    }

    const ownedShards = await stateStore.getOwnedShards();
    const ownedShardIds = Object.keys(ownedShards);

    // Start consumers for the shards the consumer owns.
    await Promise.all(
      ownedShardIds.map(async (shardId) => {
        const runningConsumer = consumers[shardId];
        const shard = ownedShards[shardId];
        if (!runningConsumer) {
          try {
            logger.debug(`Starting a consumer for "${shardId}"…`);
            const consumer = !useEnhancedFanOut
              ? new PollingConsumer({
                  client,
                  compression,
                  limit,
                  logger,
                  noRecordsPollDelay,
                  pollDelay,
                  pushToStream,
                  s3,
                  s3Client,
                  shardId,
                  shouldDeaggregate,
                  shouldParseJson,
                  stateStore,
                  stopConsumer,
                  streamName,
                  useAutoCheckpoints,
                  usePausedPolling,
                  useS3ForLargeItems,
                  ...shard
                })
              : new FanOutConsumer({
                  awsOptions,
                  client,
                  compression,
                  consumerArn,
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
                  useS3ForLargeItems,
                  ...shard
                });
            consumers[shardId] = consumer;
            consumer.start();
          } catch (err) {
            logger.error('Unexpected recoverable error when trying to start a consumer:', err);
            consumers[shardId].stop();
            consumers[shardId] = undefined;
            throw err;
          }
        } else {
          runningConsumer.updateLeaseExpiration(shard.leaseExpiration);
        }
      })
    );

    // Stop the consumers whose leases were lost.
    Object.keys(consumers)
      .filter((shardId) => !ownedShards[shardId])
      .forEach(stopConsumer);
  }

  /**
   * Stops all the running shard consumers.
   *
   * @returns {undefined}
   */
  stop() {
    const { consumers } = internal(this);
    Object.keys(consumers).forEach((shardId) => consumers[shardId].stop());
  }
}

module.exports = ConsumersManager;
