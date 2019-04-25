'use strict';

const projectName = require('project-name');
const { generate } = require('short-uuid');
const { hostname } = require('os');

const DynamoDbClient = require('./dynamodb-client');
const { confirmTableTags, ensureTableExists } = require('./table');
const { name: moduleName } = require('../package.json');

const appName = projectName(process.cwd());
const host = hostname();
const privateData = new WeakMap();
const { pid, uptime } = process;

/**
 * Provides access to the private data of the specified instance.
 *
 * @param {Object} instance - The private data's owner.
 * @returns {Object} The private data.
 */
function internal(instance) {
  if (!privateData.has(instance)) privateData.set(instance, {});
  return privateData.get(instance);
}

async function getStreamState(instance) {
  const privateProps = internal(instance);
  const { client, consumerGroup, streamName } = privateProps;
  const params = {
    ConsistentRead: true,
    Key: { consumerGroup, streamName }
  };
  const { Item } = await client.get(params);
  return Item;
}

async function initStreamState(instance) {
  const privateProps = internal(instance);
  const { client, consumerGroup, logger, streamCreatedOn, streamName } = privateProps;

  const Key = { consumerGroup, streamName };
  const { Item } = await client.get({ Key });
  if (Item && Item.streamCreatedOn !== streamCreatedOn) {
    await client.delete({ Key });
    logger.warn('Stream state has been reset. Non-matching stream creation timestamp.');
  }

  try {
    await client.put({
      ConditionExpression: 'attribute_not_exists(streamName)',
      Item: {
        consumerGroup,
        consumers: {},
        enhancedConsumers: {},
        shards: {},
        streamCreatedOn,
        streamName,
        version: generate()
      }
    });
    logger.debug('Initial state has been recorded for the stream.');
  } catch (err) {
    if (err.code !== 'ConditionalCheckFailedException') {
      logger.error(err);
      throw err;
    }
  }
}

/**
 * Class that encapsulates the DynamoDB table where the shared state for the stream is stored.
 */
class StateStore {
  /**
   * Initializes an instance of the state store.
   *
   * @param {Object} options - The initialization options.
   * @param {Object} options.dynamoDb - The initialization options passed to the Kinesis
   *        client module, specific for the DynamoDB state data table. This object can also
   *        contain any of the [`AWS.DynamoDB` options]{@link external:dynamoDbConstructor}.
   * @param {string} [options.dynamoDb.tableName=lifion-kinesis-state] - The name of the
   *        table where the shared state is stored.
   * @param {Object} [options.dynamoDb.tags={}] - If specified, the module will ensure
   *        the table has these tags during start.
   * @param {Object} options.logger - A logger instance.
   * @param {string} options.streamName - The name of the stream to keep state for.
   */
  constructor(options) {
    const {
      consumerGroup,
      consumerId,
      dynamoDb: { tableName, tags, ...awsOptions },
      logger,
      streamCreatedOn,
      streamName,
      useAutoShardAssignment
    } = options;

    const isStandalone = !useAutoShardAssignment;
    const shardsPath = isStandalone ? '#a0.#a1.#a2' : '#a';
    const shardsPathNames = isStandalone
      ? { '#a0': 'consumers', '#a1': consumerId, '#a2': 'shards' }
      : { '#a': 'shards' };

    Object.assign(internal(this), {
      awsOptions,
      consumerGroup,
      consumerId,
      isStandalone,
      logger,
      shardsPath,
      shardsPathNames,
      streamCreatedOn,
      streamName,
      tableName: tableName || `${moduleName}-state`,
      tags
    });
  }

  /**
   * Starts the state store by initializing a DynamoDB client and a document client. Then,
   * it will ensure the table exists, that is tagged as required, and there's an entry for
   * the stream state.
   */
  async start() {
    const privateProps = internal(this);
    const { tags } = privateProps;

    const client = new DynamoDbClient(privateProps);
    privateProps.client = client;

    privateProps.tableArn = await ensureTableExists(privateProps);
    if (tags) await confirmTableTags(privateProps);
    await initStreamState(this);
  }

  async clearOldConsumers(heartbeatFailureTimeout) {
    const privateProps = internal(this);
    const { client, consumerGroup, logger, streamName } = privateProps;

    const { consumers, enhancedConsumers, version } = await getStreamState(this);
    const consumerIds = Object.keys(consumers);

    const oldConsumers = consumerIds.filter(id => {
      const { heartbeat } = consumers[id];
      return Date.now() - new Date(heartbeat).getTime() > heartbeatFailureTimeout;
    });

    if (oldConsumers.length === 0) return;

    try {
      await client.update({
        ConditionExpression: `#b = :y`,
        ExpressionAttributeNames: {
          '#a': 'consumers',
          '#b': 'version',
          ...oldConsumers.reduce((obj, id, index) => ({ ...obj, [`#${index}`]: id }), {})
        },
        ExpressionAttributeValues: {
          ':x': generate(),
          ':y': version
        },
        Key: { consumerGroup, streamName },
        UpdateExpression: `REMOVE ${oldConsumers
          .map((id, index) => `#a.#${index}`)
          .join(', ')} SET #b = :x`
      });
      logger.debug(`Cleared ${oldConsumers.length} old consumer(s).`);
    } catch (err) {
      if (err.code !== 'ConditionalCheckFailedException') {
        logger.error(err);
        throw err;
      }
    }

    const usagesToClear = Object.keys(enhancedConsumers).filter(consumerName =>
      oldConsumers.includes(enhancedConsumers[consumerName].isUsedBy)
    );

    if (usagesToClear.length === 0) return;

    try {
      await client.update({
        ExpressionAttributeNames: {
          '#a': 'enhancedConsumers',
          '#b': 'isUsedBy',
          '#c': 'version',
          ...usagesToClear.reduce((obj, id, index) => ({ ...obj, [`#${index}`]: id }), {})
        },
        ExpressionAttributeValues: {
          ':z': null,
          ...usagesToClear.reduce((obj, id, index) => ({ ...obj, [`:${index}`]: generate() }), {})
        },
        Key: { consumerGroup, streamName },
        UpdateExpression: `SET ${usagesToClear
          .map((id, index) => `#a.#${index}.#b = :z, #a.#${index}.#c = :${index}`)
          .join(', ')}`
      });
      logger.debug(`Released usage of ${usagesToClear.length} enhanced consumer(s).`);
    } catch (err) {
      if (err.code !== 'ConditionalCheckFailedException') {
        logger.error(err);
        throw err;
      }
    }
  }

  async getEnhancedConsumers() {
    const { enhancedConsumers } = await getStreamState(this);
    return enhancedConsumers;
  }

  async registerConsumer() {
    const { client, consumerGroup, consumerId, isStandalone, logger, streamName } = internal(this);

    try {
      await client.update({
        ConditionExpression: 'attribute_not_exists(#a.#b)',
        ExpressionAttributeNames: {
          '#a': 'consumers',
          '#b': consumerId
        },
        ExpressionAttributeValues: {
          ':x': Object.assign(
            {
              appName,
              heartbeat: new Date().toISOString(),
              host,
              isActive: true,
              isStandalone,
              pid,
              startedOn: new Date(Date.now() - uptime() * 1000).toISOString()
            },
            isStandalone && { shards: {} }
          )
        },
        Key: { consumerGroup, streamName },
        UpdateExpression: 'SET #a.#b = :x'
      });
      logger.debug(`The consumer "${consumerId}" is now registered.`);
    } catch (err) {
      if (err.code === 'ConditionalCheckFailedException') {
        await client
          .update({
            ExpressionAttributeNames: {
              '#a': 'consumers',
              '#b': consumerId,
              '#c': 'heartbeat'
            },
            ExpressionAttributeValues: {
              ':x': new Date().toISOString()
            },
            Key: { consumerGroup, streamName },
            UpdateExpression: 'SET #a.#b.#c = :x'
          })
          .catch(() => {
            logger.debug(`Missed heartbeat for "${consumerId}".`);
          });
        return;
      }
      logger.error(err);
      throw err;
    }
  }

  async updateConsumerIsActive(isActive) {
    const { client, consumerGroup, consumerId, streamName } = internal(this);
    await client.update({
      ExpressionAttributeNames: {
        '#a': 'consumers',
        '#b': consumerId,
        '#c': 'isActive'
      },
      ExpressionAttributeValues: {
        ':z': isActive
      },
      Key: { consumerGroup, streamName },
      UpdateExpression: 'SET #a.#b.#c = :z'
    });
  }

  async registerEnhancedConsumer(name, arn) {
    const { client, consumerGroup, logger, streamName } = internal(this);

    try {
      await client.update({
        ConditionExpression: 'attribute_not_exists(#a.#b)',
        ExpressionAttributeNames: {
          '#a': 'enhancedConsumers',
          '#b': name
        },
        ExpressionAttributeValues: {
          ':x': {
            arn,
            isUsedBy: null,
            version: generate()
          }
        },
        Key: { consumerGroup, streamName },
        UpdateExpression: 'SET #a.#b = :x'
      });
      logger.debug(`The enhanced consumer "${name}" is now registered.`);
    } catch (err) {
      if (err.code !== 'ConditionalCheckFailedException') {
        logger.error(err);
        throw err;
      }
    }
  }

  async deregisterEnhancedConsumer(name) {
    const { client, consumerGroup, logger, streamName } = internal(this);

    try {
      await client.update({
        ConditionExpression: 'attribute_exists(#a.#b)',
        ExpressionAttributeNames: {
          '#a': 'enhancedConsumers',
          '#b': name
        },
        Key: { consumerGroup, streamName },
        UpdateExpression: 'REMOVE #a.#b'
      });
      logger.debug(`The enhanced consumer "${name}" is now de-registered.`);
    } catch (err) {
      if (err.code !== 'ConditionalCheckFailedException') {
        logger.error(err);
        throw err;
      }
    }
  }

  async ensureShardStateExists(shardId, shardData) {
    const privateProps = internal(this);
    const { client, consumerGroup, logger, shardsPath, shardsPathNames, streamName } = privateProps;
    const { parent } = shardData;

    try {
      await client.update({
        ConditionExpression: `attribute_not_exists(${shardsPath}.#b)`,
        ExpressionAttributeNames: { ...shardsPathNames, '#b': shardId },
        ExpressionAttributeValues: {
          ':x': {
            checkpoint: null,
            depleted: false,
            leaseExpiration: null,
            leaseOwner: null,
            parent,
            version: generate()
          }
        },
        Key: { consumerGroup, streamName },
        UpdateExpression: `SET ${shardsPath}.#b = :x`
      });
    } catch (err) {
      if (err.code !== 'ConditionalCheckFailedException') {
        logger.error(err);
        throw err;
      }
    }
  }

  async getShardAndStreamState(shardId, shardData) {
    const { consumerId, isStandalone } = internal(this);

    const getState = async () => {
      const streamState = await getStreamState(this);
      const { consumers } = streamState;
      const { shards } = isStandalone ? consumers[consumerId] : streamState;
      const shardState = shards[shardId];
      return { shardState, streamState };
    };

    const states = await getState();
    if (states.shardState !== undefined) return states;
    await this.ensureShardStateExists(shardId, shardData);
    return getState();
  }

  async lockShardLease(shardId, leaseTermTimeout, version) {
    const {
      client,
      consumerGroup,
      consumerId,
      logger,
      shardsPath,
      shardsPathNames,
      streamName
    } = internal(this);
    try {
      await client.update({
        ConditionExpression: `${shardsPath}.#b.#e = :z`,
        ExpressionAttributeNames: {
          ...shardsPathNames,
          '#b': shardId,
          '#c': 'leaseOwner',
          '#d': 'leaseExpiration',
          '#e': 'version'
        },
        ExpressionAttributeValues: {
          ':w': consumerId,
          ':x': new Date(Date.now() + leaseTermTimeout).toISOString(),
          ':y': generate(),
          ':z': version
        },
        Key: { consumerGroup, streamName },
        UpdateExpression: `SET ${[
          `${shardsPath}.#b.#c = :w`,
          `${shardsPath}.#b.#d = :x`,
          `${shardsPath}.#b.#e = :y`
        ].join(', ')}`
      });
      return true;
    } catch (err) {
      if (err.code !== 'ConditionalCheckFailedException') {
        logger.error(err);
        throw err;
      }
      return false;
    }
  }

  async lockStreamConsumer(consumerName, version) {
    const { client, consumerGroup, consumerId, logger, streamName } = internal(this);
    try {
      await client.update({
        ConditionExpression: `#a.#b.#d = :z`,
        ExpressionAttributeNames: {
          '#a': 'enhancedConsumers',
          '#b': consumerName,
          '#c': 'isUsedBy',
          '#d': 'version'
        },
        ExpressionAttributeValues: {
          ':x': consumerId,
          ':y': generate(),
          ':z': version
        },
        Key: { consumerGroup, streamName },
        UpdateExpression: `SET #a.#b.#c = :x, #a.#b.#d = :y`
      });
      return true;
    } catch (err) {
      if (err.code !== 'ConditionalCheckFailedException') {
        logger.error(err);
        throw err;
      }
      return false;
    }
  }

  async releaseShardLease(shardId, version) {
    const privateProps = internal(this);
    const { client, consumerGroup, logger, shardsPath, shardsPathNames, streamName } = privateProps;
    const releasedVersion = generate();

    try {
      await client.update({
        ConditionExpression: `${shardsPath}.#b.#e = :z`,
        ExpressionAttributeNames: {
          ...shardsPathNames,
          '#b': shardId,
          '#c': 'leaseOwner',
          '#d': 'leaseExpiration',
          '#e': 'version'
        },
        ExpressionAttributeValues: {
          ':w': null,
          ':x': null,
          ':y': releasedVersion,
          ':z': version
        },
        Key: { consumerGroup, streamName },
        UpdateExpression: `SET ${[
          `${shardsPath}.#b.#c = :w`,
          `${shardsPath}.#b.#d = :x`,
          `${shardsPath}.#b.#e = :y`
        ].join(', ')}`
      });
      return releasedVersion;
    } catch (err) {
      if (err.code !== 'ConditionalCheckFailedException') {
        logger.error(err);
        throw err;
      }
      return null;
    }
  }

  async storeShardCheckpoint(shardId, checkpoint) {
    if (typeof checkpoint !== 'string') throw new TypeError('The sequence number is required.');
    const { client, consumerGroup, shardsPath, shardsPathNames, streamName } = internal(this);

    await client.update({
      ExpressionAttributeNames: {
        ...shardsPathNames,
        '#b': shardId,
        '#c': 'checkpoint',
        '#d': 'version'
      },
      ExpressionAttributeValues: {
        ':x': checkpoint,
        ':y': generate()
      },
      Key: { consumerGroup, streamName },
      UpdateExpression: `SET ${shardsPath}.#b.#c = :x, ${shardsPath}.#b.#d = :y`
    });
  }

  async markShardAsDepleted(shardsData, parentShardId) {
    const { client, consumerGroup, shardsPath, shardsPathNames, streamName } = internal(this);

    const { shards } = await getStreamState(this);
    const parentShard = shards[parentShardId];

    const childrenShards = parentShard.checkpoint
      ? Object.keys(shardsData)
          .filter(shardId => shardsData[shardId].parent === parentShardId)
          .map(shardId => {
            const { startingSequenceNumber } = shardsData[shardId];
            return { shardId, startingSequenceNumber };
          })
      : [];

    await Promise.all(
      childrenShards.map(childrenShard => {
        return this.ensureShardStateExists(
          childrenShard.shardId,
          shardsData[childrenShard.shardId]
        );
      })
    );

    await client.update({
      ExpressionAttributeNames: Object.assign(
        {
          ...shardsPathNames,
          '#b': parentShardId,
          '#c': 'depleted',
          '#d': 'version'
        },
        childrenShards.length > 0 && { '#e': 'checkpoint' },
        childrenShards.reduce(
          (obj, childShard, index) => ({ ...obj, [`#${index}`]: childShard.shardId }),
          {}
        )
      ),
      ExpressionAttributeValues: {
        ':x': true,
        ':y': generate(),
        ...childrenShards.reduce(
          (obj, childShard, index) => ({
            ...obj,
            [`:${index * 2}`]: childShard.startingSequenceNumber,
            [`:${index * 2 + 1}`]: generate()
          }),
          {}
        )
      },
      Key: { consumerGroup, streamName },
      UpdateExpression: `SET ${[
        `${shardsPath}.#b.#c = :x`,
        `${shardsPath}.#b.#d = :y`,
        ...childrenShards.map((childShard, index) =>
          [
            `${shardsPath}.#${index}.#e = :${index * 2}`,
            `${shardsPath}.#${index}.#d = :${index * 2 + 1}`
          ].join(', ')
        )
      ].join(', ')}`
    });
  }

  async getOwnedShards() {
    const { consumerId, isStandalone } = internal(this);

    const streamState = await getStreamState(this);
    const { consumers } = streamState;
    const { shards } = isStandalone ? consumers[consumerId] : streamState;

    return Object.keys(shards)
      .filter(shardId => shards[shardId].leaseOwner === consumerId)
      .reduce((obj, shardId) => {
        const { checkpoint, depleted, leaseExpiration, version } = shards[shardId];
        if (new Date(leaseExpiration).getTime() - Date.now() > 0 && !depleted)
          return { ...obj, [shardId]: { checkpoint, leaseExpiration, version } };
        return obj;
      }, {});
  }

  async getAssignedEnhancedConsumer() {
    const { consumerId, logger } = internal(this);

    let consumerArn;
    let consumerName;

    const enhancedConsumers = await this.getEnhancedConsumers();
    const consumerNames = Object.keys(enhancedConsumers);

    // Find out an enhanced consumer was already assigned.
    consumerNames.find(name => {
      const { arn, isUsedBy } = enhancedConsumers[name];
      if (isUsedBy === consumerId) {
        consumerName = name;
        consumerArn = arn;
        return true;
      }
      return false;
    });

    // Try to assign an enhanced consumer from the available ones.
    if (!consumerArn) {
      const availableConsumers = consumerNames.filter(name => !enhancedConsumers[name].isUsedBy);
      for (let i = 0; i < availableConsumers.length; i += 1) {
        const name = availableConsumers[i];
        const { arn, version } = enhancedConsumers[name];
        if (await this.lockStreamConsumer(name, version)) {
          consumerArn = arn;
          consumerName = name;
          break;
        }
      }
    }

    if (!consumerArn) {
      logger.warn(`Couldn't lock an enhanced fan-out consumer.`);
      await this.updateConsumerIsActive(false);
      return null;
    }

    await this.updateConsumerIsActive(true);
    logger.debug(`Using the "${consumerName}" enhanced fan-out consumer.`);
    return consumerArn;
  }
}

/**
 * @external dynamoDbConstructor
 * @see https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#constructor-property
 */

module.exports = StateStore;
