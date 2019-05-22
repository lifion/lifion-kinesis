'use strict';

const HEARTBEAT_INTERVAL = 20 * 1000;
const HEARTBEAT_FAILURE_TIMEOUT = HEARTBEAT_INTERVAL * 2;

const privateData = new WeakMap();

function internal(instance) {
  if (!privateData.has(instance)) privateData.set(instance, {});
  return privateData.get(instance);
}

class HeartbeatManager {
  constructor({ consumerId, logger, stateStore }) {
    Object.assign(internal(this), { consumerId, logger, stateStore });
  }

  async start() {
    const privateProps = internal(this);
    const { logger, stateStore } = privateProps;

    const heartbeat = async () => {
      await stateStore.registerConsumer();
      await stateStore.clearOldConsumers(HEARTBEAT_FAILURE_TIMEOUT);
      logger.debug('Heartbeat sent.');
      privateProps.timeoutId = setTimeout(heartbeat, HEARTBEAT_INTERVAL);
    };

    await heartbeat();
  }

  stop() {
    const { timeoutId } = internal(this);
    clearTimeout(timeoutId);
  }
}

module.exports = HeartbeatManager;
