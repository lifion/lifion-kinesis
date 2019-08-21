/**
 * Module in charge of acquiring and renewing shard leases. The module exports a class whose
 * instances will periodically try to acquire a lease for all the stream shards. The lease manager
 * won't try to renew or acquire more leases than the maximum allowed. The maximum allowed number
 * of active leases is calculated by dividing the number of stream shards by the number of known
 * stream consumers. Another restriction, that the manager handles, is that for splitted shards,
 * children shards won't be leased until the parent shard is reported as depleted. If the manager
 * detects changes in the leases, an instance of the consumers manager is signaled so it can
 * start or stop shard consumers for the active leases as needed.
 *
 * @module lease-manager
 * @private
 */

'use strict';

const { checkIfStreamExists, getStreamShards } = require('./stream');

const ACQUIRE_LEASES_INTERVAL = 20 * 1000;
const ACQUIRE_LEASES_RECOVERY_INTERVAL = 5 * 1000;
const LEASE_TERM_TIMEOUT = 5 * 60 * 1000;
const LEASE_RENEWAL_OFFSET = Math.round(LEASE_TERM_TIMEOUT * 0.25);

const privateData = new WeakMap();

/**
 * Provides access to the private data of the specified instance.
 *
 * @param {object} instance - The private data's owner.
 * @returns {object} The private data.
 */
function internal(instance) {
  if (!privateData.has(instance)) privateData.set(instance, {});
  return privateData.get(instance);
}

/**
 * Tries to acquire the lease for a specific shard. The lease won't be acquired or renewed if:
 *
 * - The shard is a parent of a splitted shard and it has been marked as depleted.
 * - The shard is currently leased by this consumer and the lease is active.
 * - The shard is currently leased by another consumer and the lease is active.
 * - The shard is a children of a splitted shard and the parent shard hasn't been depleted.
 * - Acquiring or reneweing the lease would go beyond the maximum count of allowed active leases.
 *
 * The lease will be renewed if the owner is the current consumer and if the lease period is
 * about to expire. The lease will be released the lease owner is gone of if the lease expired.
 * Which would make the shard available for leasing for the next leasing attempt.
 *
 * @param {object} instance - The instance of Lease Manager from which the attempt originated.
 * @param {string} shardId - The ID of the shard to acquire or renew a lease for.
 * @param {object} shardsDescription - The AWS-provided data describing the shards.
 * @fulfil {boolean} - `true` if a change in the leases was detected, `false` otherwise.
 * @returns {Promise}
 */
async function acquireLease(instance, shardId, shardsDescription) {
  const privateProps = internal(instance);
  const { consumerId, isStandalone, logger, stateStore } = privateProps;

  // Retrieve the state of the shard and the stream.
  const shardDescription = shardsDescription[shardId];
  const state = await stateStore.getShardAndStreamState(shardId, shardDescription);
  const { shardState, streamState } = state;
  const { consumers, shards } = streamState;
  let { leaseExpiration, leaseOwner, version } = shardState;
  const { depleted, parent } = shardState;
  let ownLeasesCount = Object.values(shards).filter(
    shard => shard.leaseOwner === consumerId && !shard.depleted
  ).length;

  // If the shard has been marked as depleted, don't lease it.
  if (depleted) {
    logger.debug(`Shard "${shardId}" has been marked as depleted. Can't be leased.`);
    return false;
  }

  // If this consumer is the lease owner, check if the lease needs to be renewed. For the
  // renewal process, the lease isn't released as other consumer would steal it. Instead,
  // the local status is changed so the acquire attempt is triggered.
  if (leaseOwner === consumerId) {
    if (Date.now() > new Date(leaseExpiration).getTime() - LEASE_RENEWAL_OFFSET) {
      logger.debug(`It's time to renew the lease of "${shardId}" for this consumer.`);
      leaseExpiration = null;
      leaseOwner = null;
      ownLeasesCount -= 1;
    } else {
      logger.debug(`Shard "${shardId}" is currently owned by this consumer.`);
      return false;
    }
  }

  // If the lease expired or if the owner is gone, try to release it.
  const theLeaseExpired = leaseExpiration && Date.now() > new Date(leaseExpiration).getTime();
  const theOwnerIsGone = leaseOwner && !consumers[leaseOwner];
  if (theLeaseExpired || theOwnerIsGone) {
    const newVersion = await stateStore.releaseShardLease(shardId, version, streamState);
    if (newVersion) {
      logger.debug(
        `Lease for shard "${shardId}" released. ${
          theLeaseExpired ? 'The lease expired.' : 'The owner is gone.'
        }`
      );
      leaseExpiration = null;
      leaseOwner = null;
      version = newVersion;
    } else {
      logger.debug(`The lease for shard "${shardId}" couldn't be released.`);
      return true;
    }
  }

  // If the shard has an owner that is still there, don't lease it.
  if (leaseOwner) {
    logger.debug(`The shard "${shardId}" is owned by "${leaseOwner}".`);
    return false;
  }

  // If the shard has a parent that hasn't been depleted, don't lease it.
  const parentShard = parent && shards[parent];
  if (parentShard && !parentShard.depleted) {
    logger.debug(`Cannot lease "${shardId}", the parent "${parent}" hasn't been depleted.`);
    return false;
  }

  // Check if leasing one more shard won't go over the maximum of allowed active leases.
  if (!isStandalone) {
    const shardsCount = Object.values(shards).filter(shard => !shard.depleted).length;
    const consumersCount = Object.values(consumers).filter(
      consumer => !consumer.isStandalone && consumer.isActive
    ).length;
    const maxActiveLeases = Math.ceil(shardsCount / consumersCount);
    if (ownLeasesCount + 1 > maxActiveLeases) {
      logger.debug(`Max. of ${maxActiveLeases} active leases reached, can't lease "${shardId}".`);
      return true;
    }
  }

  // Try to lock the shard lease.
  if (await stateStore.lockShardLease(shardId, LEASE_TERM_TIMEOUT, version, streamState)) {
    logger.debug(`Lease for "${shardId}" acquired.`);
    return true;
  }

  logger.debug(`Can't acquire lease for "${shardId}", someone else did it.`);
  return false;
}

/**
 * Class that implements the lease manager logic.
 *
 * @alias module:lease-manager
 */
class LeaseManager {
  /**
   * Initializes an instance of the lease manager.
   *
   * @param {object} options - Initialization options.
   * @param {object} options.client - An instance of AWS.Kinesis.
   * @param {string} options.consumerId - The unique ID of the current Kinesis consumer.
   * @param {object} options.consumersManager - An instance of the ConsumersManager module.
   * @param {object} options.logger - An instance of a logger.
   * @param {object} options.stateStore - An instance of the StateStore module.
   * @param {string} options.streamName - The name of the Kinesis stream.
   * @param {boolean} options.useAutoShardAssignment - Whether if the consumer is automatically
   *        asigning the shards in between the known consumers or just consuming from all shards.
   * @param {boolean} options.useEnhancedFanOut - Whether if the consumer is using enhanced fan-out
   *        consumer or just simple polling consumers.
   */
  constructor(options) {
    const {
      client,
      consumerId,
      consumersManager,
      logger,
      stateStore,
      streamName,
      useAutoShardAssignment,
      useEnhancedFanOut
    } = options;

    Object.assign(internal(this), {
      client,
      consumerId,
      consumersManager,
      isStandalone: !useAutoShardAssignment,
      logger,
      stateStore,
      streamName,
      useEnhancedFanOut
    });
  }

  /**
   * Tries to acquire leases for all the shards in the stream and continues to do so periodically
   * until the lease manager instance is stopped.
   *
   * @returns {Promise}
   */
  async start() {
    const privateProps = internal(this);
    const { consumersManager, logger, stateStore, timeoutId, useEnhancedFanOut } = privateProps;

    if (timeoutId) return;

    let shouldReconciliate = false;

    const acquireLeases = async () => {
      let nextDelay = ACQUIRE_LEASES_INTERVAL;

      try {
        logger.debug('Trying to acquire leases…');

        const { streamArn } = await checkIfStreamExists(privateProps);
        if (!streamArn) {
          logger.debug("Can't acquire leases as the stream is gone.");
          consumersManager.stop();
          this.stop();
          return;
        }

        if (useEnhancedFanOut) {
          const consumerArn = await stateStore.getAssignedEnhancedConsumer();
          if (!consumerArn) {
            privateProps.timeoutId = setTimeout(acquireLeases, ACQUIRE_LEASES_RECOVERY_INTERVAL);
            return;
          }
        }

        const shards = await getStreamShards(privateProps);

        const changesDetected = (await Object.keys(shards).reduce(async (result, id) => {
          const acc = await result;
          try {
            return acc.concat(await acquireLease(this, id, shards));
          } catch (err) {
            logger.error('Unexpected recoverable failure when trying to acquire a lease:', err);
            nextDelay = ACQUIRE_LEASES_RECOVERY_INTERVAL;
            return acc;
          }
        }, [])).some(Boolean);

        if (changesDetected || shouldReconciliate) {
          await consumersManager.reconcile();
          shouldReconciliate = false;
        }
      } catch (err) {
        nextDelay = ACQUIRE_LEASES_RECOVERY_INTERVAL;
        logger.error('Unexpected recoverable failure when trying to acquire leases:', err);
        shouldReconciliate = true;
      }

      privateProps.timeoutId = setTimeout(acquireLeases, nextDelay);
    };

    await acquireLeases();
  }

  /**
   * Stops the lease manager attempts to acquire leases for the shards.
   */
  stop() {
    const privateProps = internal(this);
    const { logger, timeoutId } = privateProps;
    clearTimeout(timeoutId);
    privateProps.timeoutId = null;
    logger.debug('The lease manager has stopped.');
  }
}

module.exports = LeaseManager;
