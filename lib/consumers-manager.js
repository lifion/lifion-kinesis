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

    // Start consumers for the shards the consumer owns.
    ownedShardIds.forEach(shardId => {
      if (!consumers[shardId]) {
        const shard = ownedShards[shardId];
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
          ...shard
        });
        consumers[shardId] = consumer;
        consumer.start();
      }
    });

    // Stop the consumers whose leases were lost.
    Object.keys(consumers)
      .filter(shardId => !ownedShards[shardId])
      .forEach(async shardId => {
        await consumers[shardId].stop();
        consumers[shardId] = undefined;
      });
  }
}

module.exports = ConsumersManager;
