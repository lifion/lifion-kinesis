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
      consumerId,
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
      consumerId,
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
      awsOptions,
      client,
      compression,
      consumerId,
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

    const ownedShards = await stateStore.getOwnedShards();
    const ownedShardIds = Object.keys(ownedShards);

    const stopConsumer = shardId => {
      const consumer = consumers[shardId];
      if (consumer) consumer.stop();
      consumers[shardId] = undefined;
    };

    let consumerArn;
    let consumerName;

    // If using enhanced fan-out, make sure there's a consumer.
    if (useEnhancedFanOut) {
      const streamConsumers = await stateStore.getStreamConsumers();
      const consumerNames = Object.keys(streamConsumers);
      let assignedConsumer = consumerNames.find(name => {
        const { arn, isUsedBy } = streamConsumers[name];
        if (isUsedBy === consumerId) {
          consumerName = name;
          consumerArn = arn;
          return true;
        }
        return false;
      });
      if (!assignedConsumer) {
        const availableConsumers = consumerNames.filter(
          name => streamConsumers[name].isUsedBy === null
        );
        for (let i = 0; i < availableConsumers.length; i += 1) {
          const name = availableConsumers[i];
          const { arn, version } = streamConsumers[name];
          if (await stateStore.lockStreamConsumer(name, version)) {
            assignedConsumer = streamConsumers[name];
            consumerArn = arn;
            consumerName = name;
            break;
          }
        }
      }
      if (!assignedConsumer) {
        logger.warn(`Couldn't lock an enhanced fan-out consumer.`);
        await stateStore.updateConsumerIsActive(false);
        return;
      }
      await stateStore.updateConsumerIsActive(true);
      logger.debug(`Using the "${consumerName}" enhanced fan-out consumer.`);
    }

    // Start consumers for the shards the consumer owns.
    ownedShardIds.forEach(shardId => {
      const runningConsumer = consumers[shardId];
      const shard = ownedShards[shardId];
      if (!runningConsumer) {
        logger.debug(`Starting polling consumer for "${shardId}"…`);

        const consumer = !useEnhancedFanOut
          ? new PollingConsumer({
              client,
              compression,
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
              compression,
              consumerArn,
              logger,
              pushToStream,
              shardId,
              ...shard
            });

        consumers[shardId] = consumer;
        consumer.start();
      } else {
        logger.debug(`Updating the lease expiration for "${shardId}"…`);
        runningConsumer.updateLeaseExpiration(shard.leaseExpiration);
      }
    });

    // Stop the consumers whose leases were lost.
    Object.keys(consumers)
      .filter(shardId => !ownedShards[shardId])
      .forEach(shardId => {
        logger.debug(`Stopping the polling consumer for "${shardId}"…`);
        const consumer = consumers[shardId];
        if (consumer) consumer.stop();
        consumers[shardId] = undefined;
      });
  }
}

module.exports = ConsumersManager;
