'use strict';

const projectName = require('project-name');
const { DynamoDB } = require('aws-sdk');
const { generate } = require('short-uuid');
const { hostname } = require('os');
const { name } = require('../package.json');
const { checkIfTableExists, confirmTableTags, createTable } = require('./table');

const appName = projectName();
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

/**
 * Ensures that there's an entry for the stream in the state table.
 *
 * @param {Object} docClient - An instance of the DynamoDB.DocumentClient module.
 * @param {Object} logger - A logger instance.
 * @param {string} streamName - The name of the stream to ensure a state entry for.
 */
async function initializeStreamState(docClient, logger, streamName) {
  try {
    const params = {
      ConditionExpression: 'attribute_not_exists(streamName)',
      Item: { consumers: {}, streamName, version: generate() }
    };
    await docClient.put(params).promise();
    logger.debug('Initial state has been recorded for the stream.');
  } catch (err) {
    if (err.code !== 'ConditionalCheckFailedException') {
      logger.error(err);
      throw err;
    }
    logger.debug("There's existing state stored for the stream.");
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
   * @param {Object} options.dynamoDbOptions - The initialization options passed to the Kinesis
   *        client module, specific for the DynamoDB state data table. This object can also
   *        contain any of the [`AWS.DynamoDB` options]{@link external:dynamoDbConstructor}.
   * @param {string} [options.dynamoDbOptions.tableName=lifion-kinesis-state] - The name of the
   *        table where the shared state is stored.
   * @param {Object} [options.dynamoDbOptions.tags={}] - If specified, the module will ensure
   *        the table has these tags during start.
   * @param {Object} options.logger - A logger instance.
   * @param {string} options.streamName - The name of the stream to keep state for.
   */
  constructor({ consumerId, dynamoDbOptions, logger, streamName }) {
    const { tableName, tags, ...awsOptions } = dynamoDbOptions;
    Object.assign(internal(this), {
      awsOptions,
      consumerId,
      logger,
      streamName,
      tableName: tableName || `${name}-state`,
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
    const { awsOptions, logger, streamName, tableName, tags } = privateProps;

    const client = new DynamoDB(awsOptions);
    const docClient = new DynamoDB.DocumentClient({
      params: { TableName: tableName },
      service: client
    });

    privateProps.client = client;
    privateProps.docClient = docClient;

    let tableArn = await checkIfTableExists(client, logger, tableName);
    if (!tableArn) tableArn = await createTable();
    await confirmTableTags(client, logger, tableArn, tags);
    await initializeStreamState(docClient, logger, streamName);
  }

  async clearOldConsumers(oldRollCallAge) {
    const { docClient, logger, streamName } = internal(this);

    const Key = { streamName };
    let params = { Key, ConsistentRead: true };
    const { Item } = await docClient.get(params).promise();
    const { consumers, version } = Item;

    const oldConsumers = Object.keys(consumers).filter(id => {
      const { rollCalled } = consumers[id];
      return Date.now() - new Date(rollCalled).getTime() > oldRollCallAge;
    });

    if (oldConsumers.length === 0) return;

    params = {
      Key,
      UpdateExpression: `REMOVE ${oldConsumers
        .map((id, index) => `#a.#${index}`)
        .join(', ')} SET #b = :x`,
      ConditionExpression: `#b = :y`,
      ExpressionAttributeNames: {
        '#a': 'consumers',
        '#b': 'version',
        ...oldConsumers.reduce((obj, id, index) => ({ ...obj, [`#${index}`]: id }), {})
      },
      ExpressionAttributeValues: {
        ':x': generate(),
        ':y': version
      }
    };

    try {
      await docClient.update(params).promise();
      logger.debug(`Cleared old consumers: ${oldConsumers.join(', ')}`);
    } catch (err) {
      if (err.code !== 'ConditionalCheckFailedException') {
        logger.error(err);
        throw err;
      }
      logger.debug('Old consumers were cleared somewhere else.');
    }
  }

  async registerConsumer() {
    const { consumerId, docClient, logger, streamName } = internal(this);
    const Key = { streamName };
    const started = new Date(Date.now() - uptime() * 1000).toISOString();

    let params = {
      Key,
      UpdateExpression: `SET #a.#b = :x`,
      ConditionExpression: 'attribute_not_exists(#a.#b)',
      ExpressionAttributeNames: { '#a': 'consumers', '#b': consumerId },
      ExpressionAttributeValues: { ':x': { appName, host, pid, started } }
    };

    try {
      await docClient.update(params).promise();
      logger.debug(`The consumer "${consumerId}" is now registered.`);
    } catch (err) {
      if (err.code !== 'ConditionalCheckFailedException') {
        logger.error(err);
        throw err;
      }
    }

    const rollCalled = new Date().toISOString();

    params = {
      Key,
      UpdateExpression: 'set #a.#b.#c = :x',
      ExpressionAttributeNames: { '#a': 'consumers', '#b': consumerId, '#c': 'rollCalled' },
      ExpressionAttributeValues: { ':x': rollCalled }
    };

    await docClient.update(params).promise();
  }
}

/**
 * @external dynamoDbConstructor
 * @see https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#constructor-property
 */

module.exports = StateStore;
