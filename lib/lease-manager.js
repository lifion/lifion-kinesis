'use strict';

const ACQUIRE_LEASES_INTERVAL = 10 * 1000;
const LEASE_TERM_TIMEOUT = 1 * 60 * 1000;
const LEASE_RENEWAL_OFFSET = Math.round(LEASE_TERM_TIMEOUT * 0.25);

const stream = require('./stream');

const privateData = new WeakMap();

function internal(instance) {
  if (!privateData.has(instance)) privateData.set(instance, {});
  return privateData.get(instance);
}

function stopManager(instance) {
  const { logger, timeoutId } = internal(instance);
  clearTimeout(timeoutId);
  logger.debug('The lease manager has stopped.');
}

async function acquireLease(instance, shardId, shardsDescription) {
  const privateProps = internal(instance);
  const { consumerId, logger, stateStore } = privateProps;

  // Retrieve the state of the shard and the stream.
  const shardDescription = shardsDescription[shardId];
  const state = await stateStore.getShardAndStreamState(shardId, shardDescription);
  const { shardState, streamState } = state;
  const { consumers, shards } = streamState;
  let ownLeasesCount = Object.values(shards).filter(i => i.leaseOwner === consumerId).length;
  let { leaseExpiration, leaseOwner, version } = shardState;
  const { parent } = shardState;

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
    const newVersion = await stateStore.releaseShardLease(shardId, version);
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
  const shardsCount = Object.keys(shards).length;
  const consumersCount = Object.keys(consumers).length;
  const maxActiveLeases = Math.ceil(shardsCount / consumersCount);
  if (ownLeasesCount + 1 > maxActiveLeases) {
    logger.debug(`Maximum of ${maxActiveLeases} active leases reached, cannot lease "${shardId}".`);
    return true;
  }

  // Try to lock the shard lease.
  if (await stateStore.lockShardLease(shardId, LEASE_TERM_TIMEOUT, version)) {
    logger.debug(`Lease for "${shardId}" acquired.`);
    return true;
  }

  logger.debug(`Can't acquire lease for "${shardId}", someone else did it.`);
  return false;
}

async function acquireLeases(instance) {
  const privateProps = internal(instance);
  const { logger } = privateProps;

  logger.debug('Trying to acquire leases…');
  const streamExists = await stream.isActive(privateProps);

  if (streamExists === null) {
    logger.debug("Can't acquire leases as the stream is gone.");
    stopManager(instance);
    return;
  }

  const shards = await stream.getShards(privateProps);
  const shardIds = Object.keys(shards);
  const attempts = await Promise.all(shardIds.map(id => acquireLease(instance, id, shards)));
  const changesDetected = attempts.some(Boolean);

  if (changesDetected) {
    logger.debug('At least one shard lease changed.');
  } else {
    logger.debug('No changes in lease acquisition.');
  }

  privateProps.timeoutId = setTimeout(acquireLeases, ACQUIRE_LEASES_INTERVAL, instance);
}

class LeaseManager {
  constructor({ consumerId, logger, client, streamName, stateStore }) {
    Object.assign(internal(this), {
      client,
      consumerId,
      logger,
      stateStore,
      streamName
    });
  }

  async start() {
    await acquireLeases(this);
  }

  stop() {
    stopManager(this);
  }
}

module.exports = LeaseManager;
