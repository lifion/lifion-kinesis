'use strict';

const equal = require('fast-deep-equal');

async function checkIfStreamExists({ client, logger, streamName }) {
  try {
    const params = { StreamName: streamName };
    const { StreamDescription } = await client.describeStream(params);
    const { StreamStatus } = StreamDescription;

    if (StreamStatus === 'DELETING') {
      logger.debug('Waiting for the stream to complete deletion…');
      await client.waitFor('streamNotExists', params);
      logger.debug('The stream is now gone.');
      return null;
    }

    if (StreamStatus && StreamStatus !== 'ACTIVE') {
      logger.debug('Waiting for the stream to be active…');
      await client.waitFor('streamExists', params);
      logger.debug('The stream is now active.');
    }

    return StreamDescription.StreamARN;
  } catch (err) {
    if (err.code === 'ResourceNotFoundException') return null;
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

async function ensureStreamEncription(props) {
  const { client, encryption, logger, streamName: StreamName } = props;
  const { type: EncryptionType, keyId: KeyId } = encryption;

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

  const streamArn = await checkIfStreamExists(props);

  if (createStreamIfNeeded && streamArn === null) {
    logger.debug('Trying to create the stream…');
    const params = { StreamName: streamName };
    await client.createStream({ ...params, ShardCount: shardCount });
    logger.debug('Waiting for the new stream to be active…');
    const { StreamDescription } = await client.waitFor('streamExists', params);
    logger.debug('The new stream is now active.');
    return StreamDescription.StreamARN;
  }

  logger.debug("The stream exists and it's active.");
  return streamArn;
}

async function getStreamShards(props) {
  const { logger, client, streamName } = props;
  logger.debug(`Retrieving shards for the "${streamName}" stream…`);

  const params = { StreamName: streamName };
  const { Shards } = await client.listShards(params);

  const shards = Shards.reduce((obj, item) => {
    const { ShardId, ParentShardId } = item;
    return { ...obj, [ShardId]: { parent: ParentShardId || null } };
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

module.exports = {
  checkIfStreamExists,
  confirmStreamTags,
  ensureStreamEncription,
  ensureStreamExists,
  getStreamShards
};
