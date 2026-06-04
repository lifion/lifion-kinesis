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

const HEARTBEAT_INTERVAL = 20 * 1000;
const HEARTBEAT_FAILURE_TIMEOUT = HEARTBEAT_INTERVAL * 2;

/**
 * Class that implements the heartbeat manager.
 *
 * @alias module:heartbeat-manager
 */
class HeartbeatManager {
  #data = {};

  /**
   * Initializes an instance of the hearbeat manager.
   *
   * @param {Object} options - The initialization options.
   * @param {Object} options.client - An instance of the Kinesis client, used to deregister idle
   *        enhanced fan-out consumers from AWS.
   * @param {number} options.enhancedConsumerIdleTimeout - When greater than `0`, the idle timeout
   *        in milliseconds after which unused enhanced fan-out consumers are deregistered.
   * @param {Object} options.logger - An instance of a logger.
   * @param {Object} options.stateStore - An instance of the state store.
   * @param {boolean} options.useEnhancedFanOut - Whether the client is using enhanced fan-out.
   */
  constructor({ client, enhancedConsumerIdleTimeout, logger, stateStore, useEnhancedFanOut }) {
    Object.assign(this.#data, {
      client,
      enhancedConsumerIdleTimeout,
      logger,
      stateStore,
      useEnhancedFanOut
    });
  }

  /**
   * Starts the hearbeat interval where this client is registered, its hearbeat updated, and old
   * clients are detected and handled.
   *
   * @fulfil {undefined}
   * @returns {Promise}
   */
  async start() {
    const privateProps = this.#data;
    const {
      client,
      enhancedConsumerIdleTimeout,
      logger,
      stateStore,
      timeoutId,
      useEnhancedFanOut
    } = privateProps;

    if (timeoutId) return;

    const heartbeat = async () => {
      try {
        await stateStore.registerConsumer();
        await stateStore.clearOldConsumers(HEARTBEAT_FAILURE_TIMEOUT);
        if (useEnhancedFanOut && enhancedConsumerIdleTimeout > 0) {
          await stateStore.deregisterIdleEnhancedConsumers(client, enhancedConsumerIdleTimeout);
        }
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
    const privateProps = this.#data;
    const { timeoutId } = privateProps;
    clearTimeout(timeoutId);
    privateProps.timeoutId = null;
  }
}

export default HeartbeatManager;
