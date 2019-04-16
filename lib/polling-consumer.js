'use strict';

const { getRecordsDecoder } = require('./records-decoder');
const { getStreamShards } = require('./stream');

const privateData = new WeakMap();

function internal(instance) {
  if (!privateData.has(instance)) privateData.set(instance, {});
  return privateData.get(instance);
}

/**
 * Requests an new shard iterator form the given stream and shard. If a sequence number is
 * provided the iterator points to the next record after the sequence number, if not provided,
 * the iterator points to the latest record.
 *
 * @param {Object} client - The AWS.Kinesis instance to use for the request.
 * @param {Object} logger - An instance of a logger.
 * @param {string} streamName - The name of the stream where the shard belongs.
 * @param {string} shardId - The ID of the shard to get an iterator for.
 * @param {string} [sequenceNumber] - Where the iterator should point, latest otherwise.
 * @fulfil {string} The new shard iterator.
 * @returns {Promise} A promise for a new shard iterator.
 */
async function getShardIterator(client, logger, streamName, shardId, sequenceNumber) {
  const params = {
    ShardId: shardId,
    ShardIteratorType: sequenceNumber ? 'AFTER_SEQUENCE_NUMBER' : 'LATEST',
    StreamName: streamName
  };
  if (sequenceNumber) {
    params.StartingSequenceNumber = sequenceNumber;
  }
  try {
    const { ShardIterator } = await client.getShardIterator(params);
    return ShardIterator;
  } catch (err) {
    if (err.code === 'InvalidArgumentException' && sequenceNumber) {
      logger.warn(`The stored checkpoint for "${streamName}/${shardId}" is invalid. Ignoring it.`);
      return getShardIterator(client, logger, streamName, shardId);
    }
    throw err;
  }
}

async function storeShardCheckpoint(instance, shardId, sequenceNumber) {
  if (!sequenceNumber || typeof sequenceNumber !== 'string') {
    throw new TypeError('The sequence number argument is required');
  }
  const { stateStore } = internal(instance);
  await stateStore.storeShardCheckpoint(shardId, sequenceNumber);
}

/**
 * Polls for records and pushes them to the parent stream. If auto-checkpoints are enabled, they
 * will be stored before the request for records.
 *
 * @param {Object} instance - The instance for which the private data will be retrieved for.
 * @returns {Promise}
 */
async function pollForRecords(instance) {
  const privateProps = internal(instance);

  const {
    checkpoint,
    client,
    leaseExpiration,
    logger,
    noRecordsPollDelay,
    pollDelay,
    pushToStream,
    recordsDecoder,
    seqNumToCheckpoint,
    setCheckpoint,
    shardId,
    stateStore,
    stopConsumer,
    streamName,
    useAutoCheckpoints,
    usePausedPolling
  } = privateProps;

  if (Date.now() > leaseExpiration) {
    logger.debug(`Can't read from shard "${shardId}" anymore, the lease expired.`);
    stopConsumer(shardId);
    return;
  }

  if (seqNumToCheckpoint) {
    await storeShardCheckpoint(instance, shardId, seqNumToCheckpoint);
    privateProps.seqNumToCheckpoint = null;
  }

  let { iterator } = privateProps;

  if (!iterator && checkpoint) {
    logger.debug(`Starting to read shard "${shardId}" from a known checkpoint.`);
    iterator = await getShardIterator(client, logger, streamName, shardId, checkpoint);
  }

  if (!iterator) {
    logger.debug(`Starting to read shard "${shardId}" from the latest record.`);
    iterator = await getShardIterator(client, logger, streamName, shardId);
  }

  const data = await client.getRecords({ ShardIterator: iterator });
  const { NextShardIterator, Records } = data;
  const millisBehindLatest = data.MillisBehindLatest;
  privateProps.iterator = NextShardIterator;
  const recordsCount = Records.length;

  if (recordsCount === 0) {
    if (NextShardIterator === undefined) {
      const shards = await getStreamShards(privateProps);
      logger.debug(`The parent shard "${shardId}" has been depleted.`);
      await stateStore.markShardAsDepleted(shards, shardId);
      return;
    }

    const noMsgsDelay = millisBehindLatest <= 0 ? noRecordsPollDelay : 250;
    privateProps.timeoutId = setTimeout(pollForRecords, noMsgsDelay, instance);
    return;
  }

  const records = await Promise.all(Records.map(recordsDecoder));
  logger.debug(`Got ${recordsCount} records(s) from "${shardId}" (${millisBehindLatest}ms behind)`);

  if (useAutoCheckpoints) {
    const { sequenceNumber } = records[recordsCount - 1];
    if (!usePausedPolling) {
      await storeShardCheckpoint(instance, shardId, sequenceNumber);
    } else {
      privateProps.seqNumToCheckpoint = sequenceNumber;
    }
  }

  const propsToPush = { millisBehindLatest, records, setCheckpoint, shardId, streamName };

  if (usePausedPolling) {
    const continuePolling = pollForRecords.bind(this, instance);
    pushToStream({ ...propsToPush, continuePolling });
  } else {
    privateProps.timeoutId = setTimeout(pollForRecords, pollDelay, instance);
    pushToStream(propsToPush);
  }
}

class PollingConsumer {
  constructor(options) {
    const {
      checkpoint,
      client,
      compression,
      leaseExpiration,
      logger,
      noRecordsPollDelay,
      pollDelay,
      pushToStream,
      shardId,
      stateStore,
      stopConsumer,
      streamName,
      useAutoCheckpoints,
      usePausedPolling
    } = options;

    Object.assign(internal(this), {
      checkpoint,
      client,
      iterator: null,
      leaseExpiration: new Date(leaseExpiration).getTime(),
      logger,
      noRecordsPollDelay,
      pollDelay,
      pushToStream,
      recordsDecoder: getRecordsDecoder(compression, 'Buffer'),
      seqNumToCheckpoint: null,
      setCheckpoint: storeShardCheckpoint.bind(this, this, shardId),
      shardId,
      stateStore,
      stopConsumer,
      streamName,
      timeoutId: null,
      useAutoCheckpoints,
      usePausedPolling
    });
  }

  start() {
    pollForRecords(this);
  }

  stop() {
    const privateProps = internal(this);
    clearTimeout(privateProps.timeoutId);
    privateProps.timeoutId = null;
  }

  updateLeaseExpiration(leaseExpiration) {
    internal(this).leaseExpiration = new Date(leaseExpiration).getTime();
  }
}

module.exports = PollingConsumer;
