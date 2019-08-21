/**
 * Module that implements a shard polling consumer.
 *
 * @module polling-consumer
 * @private
 */

'use strict';

const { getRecordsDecoder } = require('./records');
const { getStreamShards } = require('./stream');

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
 * Requests an new shard iterator form the given stream and shard. If a sequence number is
 * provided the iterator points to the next record after the sequence number, if not provided,
 * the iterator points to the latest record.
 *
 * @param {object} client - The AWS.Kinesis instance to use for the request.
 * @param {object} logger - An instance of a logger.
 * @param {string} streamName - The name of the stream where the shard belongs.
 * @param {string} shardId - The ID of the shard to get an iterator for.
 * @param {string} [sequenceNumber] - Where the iterator should point, latest otherwise.
 * @fulfil {string} The new shard iterator.
 * @returns {Promise} A promise for a new shard iterator.
 * @private
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

/**
 * Polls for records and pushes them to the parent stream. If auto-checkpoints are enabled, they
 * will be stored before the request for records.
 *
 * @param {object} instance - The instance for which the private data will be retrieved for.
 * @returns {Promise}
 * @private
 */
async function pollForRecords(instance) {
  const privateProps = internal(instance);

  const {
    checkpoint,
    client,
    continuePolling,
    leaseExpiration,
    limit,
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

  try {
    if (Date.now() > leaseExpiration) {
      logger.debug(`Unable to read from shard "${shardId}" anymore, the lease expired.`);
      stopConsumer(shardId);
      return;
    }

    if (seqNumToCheckpoint) {
      await setCheckpoint(seqNumToCheckpoint);
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

    const data = await client.getRecords({ Limit: limit, ShardIterator: iterator });
    const { NextShardIterator, Records } = data;
    const msBehind = data.MillisBehindLatest;
    privateProps.iterator = NextShardIterator;
    const recordsCount = Records.length;

    if (recordsCount === 0) {
      if (NextShardIterator === undefined) {
        const shards = await getStreamShards(privateProps);
        logger.debug(`The parent shard "${shardId}" has been depleted.`);
        await stateStore.markShardAsDepleted(shards, shardId);
        stopConsumer(shardId);
        return;
      }

      const delay = msBehind <= 0 ? noRecordsPollDelay : 0;
      if (delay === 0) logger.debug(`Fast-forwarding "${shardId}"… (${msBehind}ms behind)`);
      privateProps.timeoutId = setTimeout(pollForRecords, delay, instance);
      return;
    }

    const records = await Promise.all(Records.map(recordsDecoder));
    logger.debug(`Got ${recordsCount} record(s) from "${shardId}" (${msBehind}ms behind)`);

    if (useAutoCheckpoints) {
      const { sequenceNumber } = records[recordsCount - 1];
      if (!usePausedPolling) {
        await setCheckpoint(sequenceNumber);
      } else {
        privateProps.seqNumToCheckpoint = sequenceNumber;
      }
    }

    const propsToPush = {
      millisBehindLatest: msBehind,
      records,
      shardId,
      streamName,
      ...(!useAutoCheckpoints && { setCheckpoint }),
      ...(usePausedPolling && { continuePolling })
    };

    pushToStream(null, propsToPush);

    if (!usePausedPolling) {
      privateProps.timeoutId = setTimeout(pollForRecords, pollDelay, instance);
    }
  } catch (err) {
    if (err.code === 'ExpiredIteratorException') {
      logger.warn('Previous shard iterator expired, recreating…');
      privateProps.iterator = null;
      await pollForRecords(instance);
      return;
    }
    logger.error(err);
    pushToStream(err);
  }
}

/**
 * Class that implements a polling consumer.
 *
 * @alias module:polling-consumer
 */
class PollingConsumer {
  /**
   * Initializes an instance of the polling consumer.
   *
   * @param {object} options - The initialization options.
   * @param {string} options.checkpoint - The last-known checkpoint for the stream shard.
   * @param {object} options.client - An instance of the Kinesis client.
   * @param {string} options.compression - The kind of data compression to use with records.
   * @param {string} options.leaseExpiration - The timestamp of the shard lease expiration.
   * @param {number} options.limit - The limit of records per get records call.
   * @param {object} options.logger - An instance of a logger.
   * @param {number} options.noRecordsPollDelay - The delay in milliseconds before attempting to
   *        get more records when there were none in the previous attempt.
   * @param {number} options.pollDelay - When the `usePausedPolling` option is `false`, this
   *        option defines the delay in milliseconds in between poll requests for more records.
   * @param {Function} options.pushToStream - A function to push incoming records to the consumer.
   * @param {string} options.shardId - The ID of the stream shard to retrieve records for.
   * @param {object} options.stateStore - An instance of the state store.
   * @param {Function} options.stopConsumer - A function that stops this consumer from the manager.
   * @param {string} options.streamName - The name of the Kinesis stream.
   * @param {boolean} options.useAutoCheckpoints - Whether to automatically store shard checkpoints
   *        using the sequence number of the most-recently received record or not.
   * @param {boolean} options.usePausedPolling - Whether if the client is waiting for
   *        user-intervention before polling for more records, or not.
   */
  constructor(options) {
    const {
      checkpoint,
      client,
      compression,
      leaseExpiration,
      limit,
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
      continuePolling: null,
      iterator: null,
      leaseExpiration: new Date(leaseExpiration).getTime(),
      limit,
      logger,
      noRecordsPollDelay,
      pollDelay,
      pushToStream,
      recordsDecoder: getRecordsDecoder(compression, 'Buffer'),
      seqNumToCheckpoint: null,
      setCheckpoint: null,
      shardId,
      stateStore,
      stopConsumer,
      streamName,
      timeoutId: null,
      useAutoCheckpoints,
      usePausedPolling
    });
  }

  /**
   * Starts the timers to poll for records.
   *
   * @fulfil {undefined}
   * @returns {Promise}
   */
  async start() {
    const privateProps = internal(this);
    const { shardId, stateStore } = privateProps;
    const { shardsPath, shardsPathNames } = await stateStore.getShardsData();

    privateProps.setCheckpoint = async sequenceNumber => {
      await stateStore.storeShardCheckpoint(shardId, sequenceNumber, shardsPath, shardsPathNames);
      privateProps.checkpoint = sequenceNumber;
    };

    privateProps.continuePolling = () => pollForRecords(this);

    pollForRecords(this);
  }

  /**
   * Stops the timers that poll for records.
   */
  stop() {
    const privateProps = internal(this);
    clearTimeout(privateProps.timeoutId);
    privateProps.timeoutId = null;
  }

  /**
   * Updates the shard lease expiration timestamp.
   *
   * @param {string} leaseExpiration - The updated timestamp when the shard lease expires.
   */
  updateLeaseExpiration(leaseExpiration) {
    internal(this).leaseExpiration = new Date(leaseExpiration).getTime();
  }
}

module.exports = PollingConsumer;
