/**
 * Module that makes sure this client is registered as a known consumer of a stream. The module
 * will also keep storing a hearbeat in the shared stream state and will detect old clients.
 * Clients are considered as old when they miss a given number of hartbeats (currently 3). Old
 * clients are removed from the state and any shard leases or enhanced fan-out consumers in use by
 * those clients will get released.
 *
 * @module heartbeat-manager
 * @private
 */

'use strict';

const HEARTBEAT_INTERVAL = 20 * 1000;
const HEARTBEAT_FAILURE_TIMEOUT = HEARTBEAT_INTERVAL * 2;

const privateData = new WeakMap();

/**
 * Provides access to the private data of the specified instance.
 *
 * @param {object} instance - The private data's owner.
 * @returns {object} The private data.
 * @private
 */
function internal(instance) {
  if (!privateData.has(instance)) privateData.set(instance, {});
  return privateData.get(instance);
}

/**
 * Class that implements the heartbeat manager.
 *
 * @alias module:heartbeat-manager
 */
class HeartbeatManager {
  /**
   * Initializes an instance of the hearbeat manager.
   *
   * @param {object} options - The initialization options.
   * @param {object} options.logger - An instance of a logger.
   * @param {object} options.stateStore - An instance of the state store.
   */
  constructor({ logger, stateStore }) {
    Object.assign(internal(this), { logger, stateStore });
  }

  /**
   * Starts the hearbeat interval where this client is registered, its hearbeat updated, and old
   * clients are detected and handled.
   *
   * @fulfil {undefined}
   * @returns {Promise}
   */
  async start() {
    const privateProps = internal(this);
    const { logger, stateStore, timeoutId } = privateProps;

    if (timeoutId) return;

    const heartbeat = async () => {
      try {
        await stateStore.registerConsumer();
        await stateStore.clearOldConsumers(HEARTBEAT_FAILURE_TIMEOUT);
        logger.debug('Heartbeat sent.');
      } catch (err) {
        logger.error('Unexpected recoverable failure when trying to send a hearbeat:', err);
      }
      privateProps.timeoutId = setTimeout(heartbeat, HEARTBEAT_INTERVAL);
    };

    await heartbeat();
  }

  /**
   * Stops the hearbeat interval.
   */
  stop() {
    const privateProps = internal(this);
    const { timeoutId } = privateProps;
    clearTimeout(timeoutId);
    privateProps.timeoutId = null;
  }
}

module.exports = HeartbeatManager;
