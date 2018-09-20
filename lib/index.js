'use strict';

const { Kinesis } = require('aws-sdk');
const { noop } = require('./utils');

const privateData = new WeakMap();

function internal(instance) {
  if (!privateData.has(instance)) privateData.set(instance, {});
  return privateData.get(instance);
}

/**
 * Ensures that the stream exists.
 *
 * @param {Object} ctx - The private context of the client instance.
 * @param {Object} ctx.client - The AWS SDK Kinesis instance.
 * @param {string} ctx.streamName - The name
 */
async function ensureStreamExists(ctx) {
  const { client, createStreamIfNeeded, logger, shardCount, streamName } = ctx;
  logger.debug(`Checking if the stream "${streamName}" exists…`);
  let streamExists;
  try {
    const awsParams = { StreamName: streamName, Limit: 1 };
    const stream = await client.describeStream(awsParams).promise();
    const status = stream.StreamDescription.StreamStatus;
    logger.debug(`The stream status is ${status}.`);
    switch (status) {
      case 'ACTIVE':
        streamExists = true;
        break;
      case 'DELETING':
        logger.debug('Waiting for the stream to complete deletion…');
        await client.waitFor('streamNotExists', awsParams).promise();
        streamExists = false;
        logger.debug('The stream is now gone.');
        break;
      default:
        logger.debug('Waiting for the stream to be active…');
        await client.waitFor('streamExists', awsParams).promise();
        streamExists = true;
        logger.debug('The stream is now active.');
        break;
    }
  } catch (err) {
    if (createStreamIfNeeded && err.code === 'ResourceNotFoundException') {
      streamExists = false;
    } else throw err;
  }
  if (!streamExists) {
    logger.debug('Trying to create the stream…');
    let awsParams = { ShardCount: shardCount, StreamName: streamName };
    await client.createStream(awsParams).promise();
    awsParams = { StreamName: streamName, Limit: 1 };
    logger.debug('Waiting for the stream to be active…');
    await client.waitFor('streamExists', awsParams).promise();
    logger.debug('The stream is now active.');
  }
}

async function ensureStreamIsEncrypted(ctx) {
  const { client, encryption, logger, streamName } = ctx;
  logger.debug(`Checking if the stream "${streamName}" is encrypted…`);
  let awsParams = { StreamName: streamName, Limit: 1 };
  const stream = await client.describeStream(awsParams).promise();
  if (stream.StreamDescription.EncryptionType === 'NONE') {
    logger.debug('Trying to encrypt the stream…');
    awsParams = {
      StreamName: streamName,
      EncryptionType: encryption.type,
      KeyId: encryption.keyId
    };
    await client.startStreamEncryption(awsParams).promise();
    awsParams = { StreamName: streamName, Limit: 1 };
    logger.debug('Waiting for the stream to update…');
    await client.waitFor('streamExists', awsParams).promise();
    logger.debug('The stream is now encrypted.');
  } else logger.debug('The stream is encrypted.');
}

async function ensureStreamIsTagged(ctx) {
  const { client, logger, streamName, tags } = ctx;
  logger.debug(`Checking if the stream "${streamName}" is already tagged…`);
  let awsParams = { StreamName: streamName, Limit: 50 };
  const data = await client.listTagsForStream(awsParams).promise();
  const existingTags = data.Tags.reduce((obj, { Key, Value }) => ({ ...obj, [Key]: Value }), {});
  const mergedTags = { ...existingTags, ...tags };
  awsParams = { StreamName: streamName, Tags: mergedTags };
  await client.addTagsToStream(awsParams).promise();
  logger.debug(`The stream tags have been updated.`);
}

class KinesisClient {
  constructor(options = {}) {
    const {
      createStreamIfNeeded = true,
      encryption,
      logger = {},
      shardCount = 1,
      streamName,
      tags,
      ...otherOptions
    } = options;

    Object.assign(internal(this), {
      createStreamIfNeeded,
      encryption,
      logger: {
        debug: typeof logger.debug === 'function' ? logger.debug.bind(logger) : noop,
        error: typeof logger.error === 'function' ? logger.error.bind(logger) : noop
      },
      shardCount,
      streamName,
      tags,
      options: otherOptions
    });
  }

  async connect() {
    const ctx = internal(this);
    const { encryption, tags, logger, options } = ctx;

    logger.debug('Trying to connect the client…');
    const client = new Kinesis(options);
    internal(this).client = client;

    await ensureStreamExists(ctx);
    if (encryption) await ensureStreamIsEncrypted(ctx);
    if (tags) await ensureStreamIsTagged(ctx);

    logger.debug('The client is now connected.');
  }
}

module.exports = KinesisClient;
