'use strict';

const equal = require('fast-deep-equal');
const { wait } = require('./utils');

const CONSUMER_STATE_CHECK_DELAY = 3000;

async function checkIfStreamExists({ client, logger, streamName }) {
  try {
    const params = { StreamName: streamName };
    const { StreamDescription } = await client.describeStream(params);
    const { StreamARN, StreamCreationTimestamp, StreamStatus } = StreamDescription;

    if (StreamStatus === 'DELETING') {
      logger.debug('Waiting for the stream to complete deletion…');
      await client.waitFor('streamNotExists', params);
      logger.debug('The stream is now gone.');
      return { streamArn: null };
    }

    if (StreamStatus && StreamStatus !== 'ACTIVE') {
      logger.debug('Waiting for the stream to be active…');
      await client.waitFor('streamExists', params);
      logger.debug('The stream is now active.');
    }

    return {
      streamArn: StreamARN,
      streamCreatedOn: StreamCreationTimestamp.toISOString()
    };
  } catch (err) {
    if (err.code === 'ResourceNotFoundException') {
      return { streamArn: null };
    }
    logger.error(err);
    throw err;
  }
}

async function confirmStreamTags({ client, logger, streamName, tags }) {
  const params = { StreamName: streamName };
  const { Tags } = await client.listTagsForStream(params);
  const existingTags = Tags.reduce((obj, { Key, Value }) => ({ ...obj, [Key]: Value }), {});
  const mergedTags = { ...existingTags, ...tags };

  if (!equal(existingTags, mergedTags)) {
    await client.addTagsToStream({ ...params, Tags: mergedTags });
    logger.debug(`The stream tags have been updated.`);
  } else {
    logger.debug('The stream is already tagged as required.');
  }
}

async function describeStreamConsumer(props) {
  const { client, consumerName, streamArn } = props;
  const { Consumers } = await client.listStreamConsumers({ StreamARN: streamArn });
  return Consumers.find(i => i.ConsumerName === consumerName) || {};
}

async function deregisterStreamConsumer(props) {
  const { client, consumerArn, consumerName, logger, streamArn } = props;
  await client.deregisterStreamConsumer({
    ConsumerARN: consumerArn,
    ConsumerName: consumerName,
    StreamARN: streamArn
  });
  let { ConsumerStatus } = await describeStreamConsumer({ client, streamArn, consumerName });
  if (ConsumerStatus === 'DELETING') {
    logger.debug(`Waiting for the consumer "${consumerName}" to complete deletion…`);
    do {
      await wait(CONSUMER_STATE_CHECK_DELAY);
      ({ ConsumerStatus } = await describeStreamConsumer({ client, streamArn, consumerName }));
    } while (ConsumerStatus === 'DELETING');
    logger.debug(`The consumer "${consumerName}" is gone.`);
  }
}

async function ensureStreamEncription(props) {
  const { client, encryption, logger, streamName: StreamName } = props;
  const { keyId: KeyId, type: EncryptionType } = encryption;

  const { StreamDescription } = await client.describeStream({ StreamName });

  if (StreamDescription.EncryptionType === 'NONE') {
    logger.debug('Trying to encrypt the stream…');
    await client.startStreamEncryption({ StreamName, EncryptionType, KeyId });
    logger.debug('Waiting for the stream to update…');
    await client.waitFor('streamExists', { StreamName });
    logger.debug('The stream is now encrypted.');
  } else {
    logger.debug('The stream is already encrypted.');
  }
}

async function ensureStreamExists(props) {
  const { client, createStreamIfNeeded, logger, shardCount, streamName } = props;
  logger.debug(`Verifying the "${streamName}" stream exists and it's active…`);

  const { streamArn, streamCreatedOn } = await checkIfStreamExists(props);

  if (createStreamIfNeeded && streamArn === null) {
    logger.debug('Trying to create the stream…');
    const params = { StreamName: streamName };
    await client.createStream({ ...params, ShardCount: shardCount });
    logger.debug('Waiting for the new stream to be active…');
    const { StreamDescription } = await client.waitFor('streamExists', params);
    logger.debug('The new stream is now active.');
    const { StreamARN, StreamCreationTimestamp } = StreamDescription;
    return {
      streamArn: StreamARN,
      streamCreatedOn: StreamCreationTimestamp.toISOString()
    };
  }

  logger.debug("The stream exists and it's active.");
  return { streamArn, streamCreatedOn };
}

async function getStreamConsumers(props) {
  const { client, logger, streamArn } = props;
  const { Consumers } = await client.listStreamConsumers({ StreamARN: streamArn });
  const consumers = Consumers.reduce(
    (result, consumer) => ({
      ...result,
      [consumer.ConsumerName]: {
        arn: consumer.ConsumerARN,
        status: consumer.ConsumerStatus
      }
    }),
    {}
  );
  const shouldWaitForConsumer = Object.keys(consumers).some(
    consumerName => consumers[consumerName].status !== 'ACTIVE'
  );
  if (shouldWaitForConsumer) {
    logger.debug(`A stream consumer is not yet active, waiting…`);
    await wait(CONSUMER_STATE_CHECK_DELAY);
    return getStreamConsumers(props);
  }
  return consumers;
}

async function getStreamShards(props) {
  const { client, logger, streamName } = props;
  logger.debug(`Retrieving shards for the "${streamName}" stream…`);

  const params = { StreamName: streamName };
  const { Shards } = await client.listShards(params);

  const shards = Shards.reduce((obj, item) => {
    const { ParentShardId, SequenceNumberRange, ShardId } = item;
    return {
      ...obj,
      [ShardId]: {
        parent: ParentShardId || null,
        startingSequenceNumber: SequenceNumberRange.StartingSequenceNumber
      }
    };
  }, {});

  Object.keys(shards).forEach(id => {
    const shard = shards[id];
    const { parent } = shard;
    if (parent && !shards[parent]) {
      shard.parent = null;
    }
  });

  return shards;
}

async function registerStreamConsumer(props) {
  const { client, consumerName, logger, streamArn } = props;
  logger.debug(`Registering stream consumer "${consumerName}"…`);
  let { ConsumerStatus } = await client.registerStreamConsumer({
    ConsumerName: consumerName,
    StreamARN: streamArn
  });
  logger.debug(`Waiting for the new stream consumer "${consumerName}" to be active…`);
  do {
    await wait(CONSUMER_STATE_CHECK_DELAY);
    ({ ConsumerStatus } = await describeStreamConsumer({ client, streamArn, consumerName }));
  } while (ConsumerStatus !== 'ACTIVE');
  logger.debug(`The consumer "${consumerName}" is now active.`);
}

module.exports = {
  checkIfStreamExists,
  confirmStreamTags,
  deregisterStreamConsumer,
  ensureStreamEncription,
  ensureStreamExists,
  getStreamConsumers,
  getStreamShards,
  registerStreamConsumer
};
