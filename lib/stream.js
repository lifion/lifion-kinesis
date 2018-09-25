'use strict';

const equal = require('fast-deep-equal');

module.exports.activate = async ctx => {
  const {
    client,
    createStreamIfNeeded,
    logger,
    shardCount: ShardCount,
    streamName: StreamName
  } = ctx;

  logger.debug(`Checking if the stream "${StreamName}" exists…`);

  let StreamARN;
  let StreamStatus;

  try {
    let { StreamDescription } = await client.describeStream({ StreamName }).promise();
    ({ StreamStatus, StreamARN } = StreamDescription);
    logger.debug(`The stream status is ${StreamStatus}.`);

    if (StreamStatus === 'DELETING') {
      logger.debug('Waiting for the stream to complete deletion…');
      await client.waitFor('streamNotExists', { StreamName, Limit: 1 }).promise();
      StreamStatus = '';
      logger.debug('The stream is now gone.');
    } else if (StreamStatus && StreamStatus !== 'ACTIVE') {
      logger.debug('Waiting for the stream to be active…');
      ({ StreamDescription } = await client.waitFor('streamExists', { StreamName }).promise());
      ({ StreamARN } = StreamDescription);
      logger.debug('The stream is now active.');
    }
  } catch (err) {
    if (!createStreamIfNeeded || err.code !== 'ResourceNotFoundException') {
      logger.error(err);
      throw err;
    }
  }

  if (!StreamStatus) {
    logger.debug('Trying to create the stream…');
    await client.createStream({ StreamName, ShardCount }).promise();
    logger.debug('Waiting for the new stream to be active…');
    const { StreamDescription } = await client.waitFor('streamExists', { StreamName }).promise();
    ({ StreamARN } = StreamDescription);
    logger.debug('The new stream is now active.');
  }

  return StreamARN;
};

module.exports.encrypt = async ctx => {
  const {
    client,
    encryption: { type: EncryptionType, keyId: KeyId },
    logger,
    streamName: StreamName
  } = ctx;

  logger.debug(`Checking if the stream "${StreamName}" is encrypted…`);

  const { StreamDescription } = await client.describeStream({ StreamName }).promise();

  if (StreamDescription.EncryptionType === 'NONE') {
    logger.debug('Trying to encrypt the stream…');
    await client.startStreamEncryption({ StreamName, EncryptionType, KeyId }).promise();
    logger.debug('Waiting for the stream to update…');
    await client.waitFor('streamExists', { StreamName }).promise();
    logger.debug('The stream is now encrypted.');
  } else {
    logger.debug('The stream is encrypted.');
  }
};

module.exports.tag = async ctx => {
  const { client, logger, streamName: StreamName, tags } = ctx;

  logger.debug(`Checking if the stream "${StreamName}" is already tagged…`);

  const { Tags } = await client.listTagsForStream({ StreamName }).promise();
  const existingTags = Tags.reduce((obj, { Key, Value }) => ({ ...obj, [Key]: Value }), {});
  const mergedTags = { ...existingTags, ...tags };

  if (!equal(existingTags, mergedTags)) {
    await client.addTagsToStream({ StreamName, Tags: mergedTags }).promise();
    logger.debug(`The stream tags have been updated.`);
  } else {
    logger.debug('The stream is already tagged as required.');
  }
};
