'use strict';

const HEARTBEAT_INTERVAL = 10000;
const HEARTBEAT_FAILURE_TIMEOUT = HEARTBEAT_INTERVAL * 3;

const privateData = new WeakMap();

function internal(instance) {
  if (!privateData.has(instance)) privateData.set(instance, {});
  return privateData.get(instance);
}

async function heartbeat(instance) {
  const privateProps = internal(instance);
  const { logger, stateStore } = privateProps;
  logger.debug('Starting heartbeat…');

  await stateStore.clearOldConsumers(HEARTBEAT_FAILURE_TIMEOUT);
  await stateStore.registerConsumer();
  logger.debug('Heartbeat completed.');

  privateProps.timeoutId = setTimeout(heartbeat, HEARTBEAT_INTERVAL, instance);
}

class HeartbeatManager {
  constructor({ consumerId, logger, stateStore }) {
    Object.assign(internal(this), { consumerId, logger, stateStore });
  }

  async start() {
    await heartbeat(this);
  }

  stop() {
    const { timeoutId } = internal(this);
    clearTimeout(timeoutId);
  }
}

module.exports = HeartbeatManager;
