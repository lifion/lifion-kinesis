'use strict';

const ROLL_CALL_INTERVAL = 3000;
const OLD_ROLL_CALL_AGE = ROLL_CALL_INTERVAL * 3;

const privateData = new WeakMap();

function internal(instance) {
  if (!privateData.has(instance)) privateData.set(instance, {});
  return privateData.get(instance);
}

function getNextCoordinatedDelay() {
  return ROLL_CALL_INTERVAL - (Date.now() % ROLL_CALL_INTERVAL);
}

async function rollCall(instance) {
  const privateProps = internal(instance);
  const { logger, stateStore } = privateProps;

  await stateStore.clearOldConsumers(OLD_ROLL_CALL_AGE);
  await stateStore.registerConsumer();
  logger.debug('Roll call completed.');

  // Schedule the next roll call.
  const coordinatedDelay = getNextCoordinatedDelay();
  privateProps.timeoutId = setTimeout(rollCall, coordinatedDelay, instance);
}

class RollCaller {
  constructor({ consumerId, logger, stateStore }) {
    Object.assign(internal(this), { consumerId, logger, stateStore });
  }

  start() {
    const privateProps = internal(this);
    const coordinatedDelay = getNextCoordinatedDelay();
    privateProps.timeoutId = setTimeout(rollCall, coordinatedDelay, this);
  }

  stop() {
    const { timeoutId } = internal(this);
    clearTimeout(timeoutId);
  }
}

module.exports = RollCaller;
