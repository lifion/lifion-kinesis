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
      streamName,
      logger,
      pushToStream,
      stateStore,
      useEnhancedFanOut
    } = options;

    Object.assign(internal(this), {
      client,
      compression,
      consumers: {},
      logger,
      stateStore,
      pushToStream,
      streamName,
      useEnhancedFanOut
    });
  }

  async reconcile() {
    const {
      client,
      consumers,
      compression,
      pushToStream,
      logger,
      stateStore,
      streamName,
      useEnhancedFanOut
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
          pushToStream,
          logger,
          shardId,
          streamName,
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
