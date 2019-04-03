'use strict';

const equal = require('fast-deep-equal');

async function isActive(props) {
  const { client, logger, streamName } = props;

  try {
    const params = { StreamName: streamName };
    let { StreamDescription } = await client.describeStream(params);
    const { StreamStatus } = StreamDescription;

    if (StreamStatus === 'DELETING') {
      logger.debug('Waiting for the stream to complete deletion…');
      await client.waitFor('streamNotExists', params).promise();
      logger.debug('The stream is now gone.');
      return null;
    }

    if (StreamStatus && StreamStatus !== 'ACTIVE') {
      logger.debug('Waiting for the stream to be active…');
      ({ StreamDescription } = await client.waitFor('streamExists', params).promise());
      logger.debug('The stream is now active.');
    }

    return StreamDescription.StreamARN;
  } catch (err) {
    if (err.code === 'ResourceNotFoundException') return null;
    logger.error(err);
    throw err;
  }
}

async function activate(props) {
  const { client, createStreamIfNeeded, logger, shardCount, streamName } = props;
  logger.debug("Verifying the stream exists and it's active…");

  const streamArn = await isActive(props);

  if (createStreamIfNeeded && streamArn === null) {
    logger.debug('Trying to create the stream…');
    const params = { StreamName: streamName };
    await client.createStream({ ...params, ShardCount: shardCount }).promise();
    logger.debug('Waiting for the new stream to be active…');
    const { StreamDescription } = await client.waitFor('streamExists', params).promise();
    logger.debug('The new stream is now active.');
    return StreamDescription.StreamARN;
  }

  logger.debug("The stream exists and it's active.");
  return streamArn;
}

async function encrypt(props) {
  const { client, encryption, logger, streamName: StreamName } = props;
  const { type: EncryptionType, keyId: KeyId } = encryption;
  logger.debug(`Checking if the stream "${StreamName}" is encrypted…`);

  const { StreamDescription } = await client.describeStream({ StreamName });

  if (StreamDescription.EncryptionType === 'NONE') {
    logger.debug('Trying to encrypt the stream…');
    await client.startStreamEncryption({ StreamName, EncryptionType, KeyId }).promise();
    logger.debug('Waiting for the stream to update…');
    await client.waitFor('streamExists', { StreamName }).promise();
    logger.debug('The stream is now encrypted.');
  } else {
    logger.debug('The stream is encrypted.');
  }
}

async function getShards(props) {
  const { logger, client, streamName } = props;
  logger.debug(`Retrieving shards for the "${streamName}" stream…`);

  const params = { StreamName: streamName, MaxResults: 1000 };
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

async function tag(props) {
  const { client, logger, streamName: StreamName, tags } = props;
  logger.debug(`Checking if the stream "${StreamName}" is already tagged…`);

  const { Tags } = await client.listTagsForStream({ StreamName });
  const existingTags = Tags.reduce((obj, { Key, Value }) => ({ ...obj, [Key]: Value }), {});
  const mergedTags = { ...existingTags, ...tags };

  if (!equal(existingTags, mergedTags)) {
    await client.addTagsToStream({ StreamName, Tags: mergedTags }).promise();
    logger.debug(`The stream tags have been updated.`);
  } else {
    logger.debug('The stream is already tagged as required.');
  }
}

module.exports = { activate, encrypt, isActive, getShards, tag };
