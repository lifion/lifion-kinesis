'use strict';

const ACQUIRE_LEASES_INTERVAL = 20 * 1000;
const LEASE_TERM_TIMEOUT = 5 * 60 * 1000;
const LEASE_RENEWAL_OFFSET = Math.round(LEASE_TERM_TIMEOUT * 0.25);

const { checkIfStreamExists, getStreamShards } = require('./stream');

const privateData = new WeakMap();

function internal(instance) {
  if (!privateData.has(instance)) privateData.set(instance, {});
  return privateData.get(instance);
}

function stopManager(instance) {
  const { consumersManager, logger, timeoutId } = internal(instance);
  clearTimeout(timeoutId);
  consumersManager.stop();
  logger.debug('The lease manager has stopped.');
}

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
        `Lease for shard "${shardId}" released.`,
        theLeaseExpired ? 'The lease expired.' : 'The owner is gone.'
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

class LeaseManager {
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

  async start() {
    const privateProps = internal(this);
    const { consumersManager, logger, stateStore, useEnhancedFanOut } = privateProps;

    const acquireLeases = async () => {
      logger.debug('Trying to acquire leases…');

      const { streamArn } = await checkIfStreamExists(privateProps);
      if (streamArn === null) {
        logger.debug("Can't acquire leases as the stream is gone.");
        stopManager(this);
        return;
      }

      if (useEnhancedFanOut) {
        const consumerArn = await stateStore.getAssignedEnhancedConsumer();
        if (!consumerArn) {
          logger.debug("Can't acquire leases now as there's no assigned enhanced consumer.");
          privateProps.timeoutId = setTimeout(acquireLeases, ACQUIRE_LEASES_INTERVAL);
          return;
        }
      }

      const shards = await getStreamShards(privateProps);

      const changesDetected = (await Object.keys(shards).reduce(async (result, id) => {
        return (await result).concat(await acquireLease(this, id, shards));
      }, [])).some(Boolean);
      if (changesDetected) consumersManager.reconcile();

      privateProps.timeoutId = setTimeout(acquireLeases, ACQUIRE_LEASES_INTERVAL);
    };

    await acquireLeases();
  }

  stop() {
    stopManager(this);
  }
}

module.exports = LeaseManager;
