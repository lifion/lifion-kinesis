'use strict';

const FanOutConsumer = require('./fan-out-consumer');
const PollingConsumer = require('./polling-consumer');

const privateData = new WeakMap();

function internal(instance) {
  if (!privateData.has(instance)) privateData.set(instance, {});
  return privateData.get(instance);
}

class ConsumersManager {
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
      stateStore,
      streamName,
      useAutoCheckpoints,
      useEnhancedFanOut,
      usePausedPolling
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
      stateStore,
      streamName,
      useAutoCheckpoints,
      useEnhancedFanOut,
      usePausedPolling
    });
  }

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
      stateStore,
      streamName,
      useAutoCheckpoints,
      useEnhancedFanOut,
      usePausedPolling
    } = internal(this);

    logger.debug('Reconciling shard consumers…');

    const stopConsumer = shardId => {
      const consumer = consumers[shardId];
      if (consumer) {
        logger.debug(`Stopping the consumer for "${shardId}"…`);
        consumer.stop();
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
    ownedShardIds.forEach(shardId => {
      const runningConsumer = consumers[shardId];
      const shard = ownedShards[shardId];
      if (!runningConsumer) {
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
              shardId,
              stateStore,
              stopConsumer,
              streamName,
              useAutoCheckpoints,
              usePausedPolling,
              ...shard
            })
          : new FanOutConsumer({
              awsOptions,
              client,
              compression,
              consumerArn,
              logger,
              pushToStream,
              shardId,
              stateStore,
              stopConsumer,
              streamName,
              ...shard
            });
        consumers[shardId] = consumer;
        consumer.start();
      } else {
        runningConsumer.updateLeaseExpiration(shard.leaseExpiration);
      }
    });

    // Stop the consumers whose leases were lost.
    Object.keys(consumers)
      .filter(shardId => !ownedShards[shardId])
      .forEach(stopConsumer);
  }

  stop() {
    const { consumers } = internal(this);
    Object.keys(consumers).forEach(shardId => consumers[shardId].stop());
  }
}

module.exports = ConsumersManager;
