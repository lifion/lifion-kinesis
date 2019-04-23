/* eslint-disable no-await-in-loop */

'use strict';

const privateData = new WeakMap();

function internal(instance) {
  if (!privateData.has(instance)) privateData.set(instance, {});
  return privateData.get(instance);
}

class FanOutConsumer {
  constructor(options) {
    const { consumerArn, logger, shardId, stateStore } = options;
    Object.assign(internal(this), { logger, stateStore, shardId, consumerArn });
  }

  async start() {
    const { consumerArn, logger, shardId } = internal(this);
    logger.debug('START FAN-OUT CONSUMER!', { shardId });
    // const consumers = await stateStore.getStreamConsumers();
    logger.debug({ shardId, consumerArn });
  }

  stop() {
    const { logger } = internal(this);
    logger.debug('STOP FAN-OUT CONSUMER!');
  }

  updateLeaseExpiration(leaseExpiration) {
    const { logger } = internal(this);
    logger.debug('UPDATE LEASE EXPIRATION FAN-OUT CONSUMER!', { leaseExpiration });
  }
}

module.exports = FanOutConsumer;
