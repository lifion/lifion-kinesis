'use strict';

const PollingConsumer = require('./polling-consumer');

const privateData = new WeakMap();

function internal(instance) {
  if (!privateData.has(instance)) privateData.set(instance, {});
  return privateData.get(instance);
}

class ConsumersManager {
  constructor(options) {
    const {
      client,
      compression,
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
      client,
      compression,
      consumers: {},
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
      client,
      compression,
      consumers,
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

    if (useEnhancedFanOut) {
      throw new Error('The fan-out consumers are not supported yet.');
    }

    const ownedShards = await stateStore.getOwnedShards();
    const ownedShardIds = Object.keys(ownedShards);

    const stopConsumer = shardId => {
      consumers[shardId].stop();
      consumers[shardId] = undefined;
    };

    // Start consumers for the shards the consumer owns.
    ownedShardIds.forEach(shardId => {
      const runningConsumer = consumers[shardId];
      const shard = ownedShards[shardId];
      if (!runningConsumer) {
        const consumer = new PollingConsumer({
          client,
          compression,
          logger,
          noRecordsPollDelay,
          pollDelay,
          pushToStream,
          shardId,
          stateStore,
          streamName,
          useAutoCheckpoints,
          usePausedPolling,
          stopConsumer,
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
      .forEach(shardId => {
        const consumer = consumers[shardId];
        if (consumer) {
          consumers[shardId].stop();
          consumers[shardId] = undefined;
        }
      });
  }
}

module.exports = ConsumersManager;
