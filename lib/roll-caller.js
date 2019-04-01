'use strict';

const ROLL_CALL_INTERVAL = 30000;
const OLD_ROLL_CALL_AGE = ROLL_CALL_INTERVAL * 3;

const privateData = new WeakMap();

function internal(instance) {
  if (!privateData.has(instance)) privateData.set(instance, {});
  return privateData.get(instance);
}

async function rollCall(instance) {
  const privateProps = internal(instance);
  const { logger, stateStore } = privateProps;
  logger.debug('Starting roll call…');

  await stateStore.clearOldConsumers(OLD_ROLL_CALL_AGE);
  await stateStore.registerConsumer();
  logger.debug('Roll call completed.');

  privateProps.timeoutId = setTimeout(rollCall, ROLL_CALL_INTERVAL, instance);
}

class RollCaller {
  constructor({ consumerId, logger, stateStore }) {
    Object.assign(internal(this), { consumerId, logger, stateStore });
  }

  start() {
    rollCall(this);
  }

  stop() {
    const { timeoutId } = internal(this);
    clearTimeout(timeoutId);
  }
}

module.exports = RollCaller;
