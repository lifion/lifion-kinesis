/**
 * Module that maintains the state of the consumer in a DynamoDB table.
 *
 * @module state-store
 * @private
 */

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
 * @param {object} instance - The private data's owner.
 * @returns {object} The private data.
 * @private
 */
function internal(instance) {
  if (!privateData.has(instance)) privateData.set(instance, {});
  return privateData.get(instance);
}

/**
 * Retrieves the stream state.
 *
 * @param {object} instance - The instance of the state store to get the private data from.
 * @returns {Promise}
 * @private
 */
async function getStreamState(instance) {
  const privateProps = internal(instance);
  const { client, consumerGroup, streamName } = privateProps;
  const { Item } = await client.get({
    ConsistentRead: true,
    Key: { consumerGroup, streamName }
  });
  return Item;
}

/**
 * Ensures there's an entry for the current stream in the state database.
 *
 * @param {object} instance - The instance of the state store to get the private data from.
 * @returns {Promise}
 * @private
 */
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
 * Updates the "is active" flag of a consumer. This is useful for when a consumer is using
 * enhanced fan-out but is unable to use one of the registered enhanced consumers. When non
 * active, a consumer isn't used when calculating the maximum active number of leases.
 *
 * @param {object} instance - The instance of the state store to get the private data from.
 * @param {boolean} isActive - The value to set the "is active" flag to.
 * @fulfil {undefined}
 * @returns {Promise}
 */
async function updateConsumerIsActive(instance, isActive) {
  const { client, consumerGroup, consumerId, logger, streamName } = internal(instance);
  try {
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
  } catch (err) {
    logger.debug("Can't update the is consumer active flag.");
  }
}

/**
 * Tries to lock an enhanced fan-out consumer to this consumer.
 *
 * @param {object} instance - The instance of the state store to get the private data from.
 * @param {string} consumerName - The name of the enhanced fan-out consumer to lock.
 * @param {string} version - The known version  number of the enhanced consumer state entry.
 * @fulfil {boolean} - `true` if the enhanced consumer was locked, `false` otherwise.
 * @returns {Promise}
 */
async function lockEnhancedConsumer(instance, consumerName, version) {
  const {
    client,
    consumerGroup,
    consumerId,
    logger,
    streamName,
    useAutoShardAssignment
  } = internal(instance);

  try {
    await client.update({
      ConditionExpression: `#a.#b.#d = :z AND #a.#b.#c = :v`,
      ExpressionAttributeNames: {
        '#a': 'enhancedConsumers',
        '#b': consumerName,
        '#c': 'isUsedBy',
        '#d': 'version',
        ...(!useAutoShardAssignment && { '#e': 'shards' })
      },
      ExpressionAttributeValues: {
        ':v': null,
        ':x': consumerId,
        ':y': generate(),
        ':z': version,
        ...(!useAutoShardAssignment && { ':w': {} })
      },
      Key: { consumerGroup, streamName },
      UpdateExpression: `SET #a.#b.#c = :x, #a.#b.#d = :y${
        !useAutoShardAssignment ? ', #a.#b.#e = if_not_exists(#a.#b.#e, :w)' : ''
      }`
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

/**
 * Class that encapsulates the DynamoDB table where the shared state for the stream is stored.
 *
 * @alias module:state-store
 */
class StateStore {
  /**
   * Initializes an instance of the state store.
   *
   * @param {object} options - The initialization options.
   * @param {string} options.consumerGroup - The name of the group of consumers in which shards
   *        will be distributed and checkpoints will be shared.
   * @param {string} options.consumerId - An unique ID representing the instance of this consumer.
   * @param {object} options.dynamoDb - The initialization options passed to the Kinesis
   *        client module, specific for the DynamoDB state data table. This object can also
   *        contain any of the [`AWS.DynamoDB` options]{@link external:AwsJsSdkDynamoDb}.
   * @param {object} [options.dynamoDB.provisionedThroughput] - The provisioned throughput for the
   *        state table. If not provided, pay-per-request is used.
   * @param {string} [options.dynamoDb.tableName=lifion-kinesis-state] - The name of the
   *        table where the shared state is stored.
   * @param {object} [options.dynamoDb.tags={}] - If specified, the module will ensure
   *        the table has these tags during start.
   * @param {object} options.logger - An instance of a logger.
   * @param {string} options.streamCreatedOn - The creation timestamp for the stream. It's used
   *        to confirm the stored state corresponds to the same stream with the given name.
   * @param {string} options.streamName - The name of the stream to keep state for.
   * @param {boolean} options.useAutoShardAssignment - Wheter if the stream shards should be
   *        automatically assigned to the active consumers in the same group or not.
   * @param {boolean} options.useEnhancedFanOut - Whether if the consumer is using enhanced
   *        fan-out consumers or not.
   */
  constructor(options) {
    const {
      consumerGroup,
      consumerId,
      dynamoDb: { provisionedThroughput, tableName, tags, ...awsOptions },
      logger,
      streamCreatedOn,
      streamName,
      useAutoShardAssignment,
      useEnhancedFanOut
    } = options;

    Object.assign(internal(this), {
      awsOptions,
      consumerGroup,
      consumerId,
      logger,
      provisionedThroughput,
      streamCreatedOn,
      streamName,
      tableName: tableName || `${moduleName}-state`,
      tags,
      useAutoShardAssignment,
      useEnhancedFanOut
    });
  }

  /**
   * Clears out consumers that are considered to be gone as they have failed to record a
   * hearbeat in a given timeout period. In addition to clearing out the consumers, any shard
   * with an active lease for consumers that are gone will be released. Any enhanced fan-out
   * consumers in use by gone consumers will also be released.
   *
   * @param {number} heartbeatFailureTimeout - The number of milliseconds after a heartbeat when
   *        a consumer should be considered as gone.
   * @fulfil {undefined}
   * @returns {Promise}
   */
  async clearOldConsumers(heartbeatFailureTimeout) {
    const privateProps = internal(this);
    const { client, consumerGroup, logger, streamName } = privateProps;

    const { consumers, enhancedConsumers, version } = await getStreamState(this);
    const consumerIds = Object.keys(consumers);

    const oldConsumers = consumerIds.filter(id => {
      const { heartbeat } = consumers[id];
      return Date.now() - new Date(heartbeat).getTime() > heartbeatFailureTimeout;
    });

    if (oldConsumers.length > 0) {
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
    }

    const usagesToClear = Object.keys(enhancedConsumers).filter(consumerName => {
      const { isUsedBy } = enhancedConsumers[consumerName];
      if (oldConsumers.includes(isUsedBy)) {
        logger.debug(`Enhanced consumer "${consumerName}" can be released, missed heartbeat.`);
        return true;
      }
      if (!consumerIds.includes(isUsedBy)) {
        logger.debug(`Enhanced consumer "${consumerName}" can be released, unknown owner.`);
        return true;
      }
      return false;
    });

    await Promise.all(
      usagesToClear.map(async consumerName => {
        const { isUsedBy, version: ver } = enhancedConsumers[consumerName];
        try {
          await client.update({
            ConditionExpression: '#a.#b.#c = :w AND #a.#b.#d = :x',
            ExpressionAttributeNames: {
              '#a': 'enhancedConsumers',
              '#b': consumerName,
              '#c': 'isUsedBy',
              '#d': 'version'
            },
            ExpressionAttributeValues: {
              ':w': isUsedBy,
              ':x': ver,
              ':y': null,
              ':z': generate()
            },
            Key: { consumerGroup, streamName },
            UpdateExpression: 'SET #a.#b.#c = :y, #a.#b.#d = :z'
          });
          logger.debug(`Enhanced consumer "${consumerName}" has been released.`);
        } catch (err) {
          if (err.code !== 'ConditionalCheckFailedException') {
            logger.error(err);
            throw err;
          }
          logger.debug(`Enhanced consumer "${consumerName}" can't be released.`);
        }
      })
    );
  }

  /**
   * Removes an enhanced fan-out consumer from the stream state.
   *
   * @param {string} name - The name of the enhanced consumer to remove.
   * @fulfil {undefined}
   * @returns {Promise}
   */
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

  /**
   * Ensures there's an entry for the given shard ID and data in the stream state.
   *
   * @param {string} shardId - The ID of the stream shard.
   * @param {object} shardData - The data describing the shard as returned by the AWS Kinesis API.
   * @param {object} [streamState] - The current stream state, if known.
   * @fulfil {undefined}
   * @returns {Promise}
   */
  async ensureShardStateExists(shardId, shardData, streamState) {
    const privateProps = internal(this);
    const { shardsPath, shardsPathNames } = await this.getShardsData(streamState);
    const { client, consumerGroup, logger, streamName } = privateProps;
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

  /**
   * Returns the ARN of the enhanced fan-out consumer assigned to this consumer. It will try to
   * lock one if the consumer is using enhanced fan-out but hasn't been assigned one before.
   *
   * @fulfil {string} - The ARN of the assigned enhanced fan-out consumer, `null` otherwise.
   * @returns {Promise}
   */
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
        if (await lockEnhancedConsumer(this, name, version)) {
          consumerArn = arn;
          consumerName = name;
          break;
        }
      }
    }

    if (!consumerArn) {
      logger.warn(`All enhanced fan-out consumers are assigned. Waiting until one is available…`);
      await updateConsumerIsActive(this, false);
      return null;
    }

    await updateConsumerIsActive(this, true);
    logger.debug(`Using the "${consumerName}" enhanced fan-out consumer.`);
    return consumerArn;
  }

  /**
   * Returns the data for the enhanced fan-out consumers stored in the state.
   *
   * @fulfil {Object} - The enhanced consumers.
   * @returns {Promise}
   */
  async getEnhancedConsumers() {
    const { enhancedConsumers } = await getStreamState(this);
    return enhancedConsumers;
  }

  /**
   * Returns an object with the state of the shards for which this consumer has an active lease.
   *
   * @fulfil {Object} - An object with the state of the owned shards.
   * @returns {Promise}
   */
  async getOwnedShards() {
    const { consumerId } = internal(this);
    const streamState = await getStreamState(this);
    const { shards } = await this.getShardsData(streamState);

    return Object.keys(shards)
      .filter(shardId => shards[shardId].leaseOwner === consumerId)
      .reduce((obj, shardId) => {
        const { checkpoint, depleted, leaseExpiration, version } = shards[shardId];
        if (new Date(leaseExpiration).getTime() - Date.now() > 0 && !depleted)
          return { ...obj, [shardId]: { checkpoint, leaseExpiration, version } };
        return obj;
      }, {});
  }

  /**
   * Returns an object with the current states for a given shard and the entire stream.
   *
   * @param {string} shardId - The ID of the stream shard to get the state for.
   * @param {object} shardData - The data describing the shard as provided by the AWS Kinesis API.
   * @fulfil {Object} - An object containing `shardState` (the state of the shard) and
   *        `streamState` (the stream state).
   * @returns {Promise}
   */
  async getShardAndStreamState(shardId, shardData) {
    const getState = async () => {
      const streamState = await getStreamState(this);
      const { shards } = await this.getShardsData(streamState);
      const shardState = shards[shardId];
      return { shardState, streamState };
    };
    const states = await getState();
    if (states.shardState !== undefined) return states;
    await this.ensureShardStateExists(shardId, shardData, states.streamState);
    return getState();
  }

  /**
   * Returns the current state of the stream shards and pointers that can be used to update the
   * stream shard state in subsequent calls. This is useful as the shards state is stored in
   * different locations depending on the consumer usage scenario.
   *
   * - When using automatic shard distribution, the shards state is stored in `.shards`.
   * - When reading from all shards, the shards state is stored in `.consumers[].shards`.
   * - When reading from all the shards and using enhanced fan-out consumers, the shards state
   *   is stored in `.enhancedConsumers[].shards`.
   *
   * @param {object} [streamState] - The current state for the entire stream, if not provided,
   *        the stream state is fetched. This parameter is useful to avoid repeated calls for
   *        the stream state retrieval if that information is already present.
   * @fulfil {Object} - An object containing `shards` (the current shards state), `shardsPath`
   *        (a string pointing to the path to where the shards state is stored), and
   *        `shardsPathNames` (an object with the value for the path attributes that point to the
   *        place where the shards state is stored). Both `shardsPath` and `shardsPathNames` are
   *        to be used in `update` expressions.
   * @returns {Promise}
   */
  async getShardsData(streamState) {
    const { consumerId, useAutoShardAssignment, useEnhancedFanOut } = internal(this);
    const normStreamState = !streamState ? await getStreamState(this) : streamState;
    const { consumers, enhancedConsumers } = normStreamState;

    if (!useAutoShardAssignment) {
      if (useEnhancedFanOut) {
        const enhancedConsumerState = Object.entries(enhancedConsumers).find(
          ([, value]) => value.isUsedBy === consumerId
        );
        if (!enhancedConsumerState) {
          throw new Error('The enhanced consumer state is not where expected.');
        }
        const [consumerName, { shards }] = enhancedConsumerState;
        return {
          shards,
          shardsPath: '#a0.#a1.#a2',
          shardsPathNames: {
            '#a0': 'enhancedConsumers',
            '#a1': consumerName,
            '#a2': 'shards'
          }
        };
      }
      const { shards } = consumers[consumerId];
      return {
        shards,
        shardsPath: '#a0.#a1.#a2',
        shardsPathNames: {
          '#a0': 'consumers',
          '#a1': consumerId,
          '#a2': 'shards'
        }
      };
    }
    const { shards } = normStreamState;
    return {
      shards,
      shardsPath: '#a',
      shardsPathNames: {
        '#a': 'shards'
      }
    };
  }

  /**
   * Tries to lock the lease of a given shard.
   *
   * @param {string} shardId - The ID of the shard to lock a lease for.
   * @param {number} leaseTermTimeout - The duration of the lease in milliseconds.
   * @param {string} version - The known version number of the shard state entry.
   * @param {object} streamState - The known stream state.
   * @fulfil {boolean} - `true` if the lease was successfuly locked, `false` otherwise.
   * @returns {Promise}
   */
  async lockShardLease(shardId, leaseTermTimeout, version, streamState) {
    const { client, consumerGroup, consumerId, logger, streamName } = internal(this);
    const { shardsPath, shardsPathNames } = await this.getShardsData(streamState);
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

  /**
   * Marks a shard as depleted in the stream state so children shards can be leased.
   *
   * @param {object} shardsData - The current shards state.
   * @param {string} parentShardId - The ID of the shard to mark as depleted.
   * @fulfil {undefined}
   * @returns {Promise}
   */
  async markShardAsDepleted(shardsData, parentShardId) {
    const { client, consumerGroup, streamName } = internal(this);

    const streamState = await getStreamState(this);
    const { shards, shardsPath, shardsPathNames } = await this.getShardsData(streamState);
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
          shardsData[childrenShard.shardId],
          streamState
        );
      })
    );

    await client.update({
      ExpressionAttributeNames: {
        ...shardsPathNames,
        '#b': parentShardId,
        '#c': 'depleted',
        '#d': 'version',
        ...(childrenShards.length > 0 && { '#e': 'checkpoint' }),
        ...childrenShards.reduce(
          (obj, childShard, index) => ({ ...obj, [`#${index}`]: childShard.shardId }),
          {}
        )
      },
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

  /**
   * Registers the current consumer in the state if not present there yet. If present,
   * it updates the consumer hearbeat.
   *
   * @fulfil {undefined}
   * @returns {Promise}
   */
  async registerConsumer() {
    const {
      client,
      consumerGroup,
      consumerId,
      logger,
      streamName,
      useAutoShardAssignment,
      useEnhancedFanOut
    } = internal(this);

    try {
      await client.update({
        ConditionExpression: 'attribute_not_exists(#a.#b)',
        ExpressionAttributeNames: {
          '#a': 'consumers',
          '#b': consumerId
        },
        ExpressionAttributeValues: {
          ':x': {
            appName,
            heartbeat: new Date().toISOString(),
            host,
            isActive: true,
            isStandalone: !useAutoShardAssignment,
            pid,
            startedOn: new Date(Date.now() - uptime() * 1000).toISOString(),
            ...(!useAutoShardAssignment && !useEnhancedFanOut && { shards: {} })
          }
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

  /**
   * Makes sure that an enhanced fan-out consumer is present in the stream state.
   *
   * @param {string} name - The name of the enhanced fan-out consumer.
   * @param {string} arn - The ARN of the enhanced fan-out consumer as given by AWS.
   * @fulfil {undefined}
   * @returns {Promise}
   */
  async registerEnhancedConsumer(name, arn) {
    const { client, consumerGroup, logger, streamName, useAutoShardAssignment } = internal(this);

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
            isStandalone: !useAutoShardAssignment,
            isUsedBy: null,
            version: generate(),
            ...(!useAutoShardAssignment && { shards: {} })
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

  /**
   * Tries to release the lease of a shard.
   *
   * @param {string} shardId - The ID of the shard to release a lease for.
   * @param {string} version - The known version number of the shard state entry.
   * @param {object} streamState - The known stream state.
   * @fulfil {string} - The new version number if the lease is released, `null` otherwise.
   * @returns {Promise}
   */
  async releaseShardLease(shardId, version, streamState) {
    const privateProps = internal(this);
    const { client, consumerGroup, logger, streamName } = privateProps;
    const { shardsPath, shardsPathNames } = await this.getShardsData(streamState);
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

  /**
   * Starts the state store by initializing a DynamoDB client and a document client. Then,
   * it will ensure the table exists, that is tagged as required, and there's an entry for
   * the stream state.
   *
   * @fulfil {undefined}
   * @returns {Promise}
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

  /**
   * Store a shard checkpoint.
   *
   * @param {string} shardId - The ID of the shard to store a checkpoint for.
   * @param {string} checkpoint - The sequence number to store as the recovery point.
   * @param {string} shardsPath - The path pointing to where the shards state is stored.
   * @param {object} shardsPathNames - The values of the attribute names in the path.
   * @fulfil {undefined}
   * @returns {Promise}
   */
  async storeShardCheckpoint(shardId, checkpoint, shardsPath, shardsPathNames) {
    if (typeof checkpoint !== 'string') throw new TypeError('The sequence number is required.');
    const { client, consumerGroup, streamName } = internal(this);

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
}

/**
 * @external AwsJsSdkDynamoDb
 * @see https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#constructor-property
 */

module.exports = StateStore;
