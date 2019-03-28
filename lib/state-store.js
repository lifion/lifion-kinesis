'use strict';

const equal = require('fast-deep-equal');
const { DynamoDB } = require('aws-sdk');
const { generate } = require('short-uuid');
const { name, stateTableDefinition } = require('../package.json');

const privateData = new WeakMap();

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
 * Checks if the specified table exists and would wait until the table is deleted or activated
 * if the table is in the middle of an update.
 *
 * @param {Object} client - An instance of the DynamoDB client.
 * @param {Object} logger - A logger instance.
 * @param {string} tableName - The name of the table to check.
 * @returns {string} If the table exists, the ARN of the table, `null` otherwise.
 */
async function checkIfTableExists(client, logger, tableName) {
  try {
    const params = { TableName: tableName };
    const { Table } = await client.describeTable(params).promise();
    const { TableStatus } = Table;
    logger.debug(`The "${tableName}" table status is ${TableStatus}.`);
    if (TableStatus === 'DELETING') {
      logger.debug('Waiting for the table to complete deletion…');
      await client.waitFor('tableNotExists', params).promise();
      logger.debug('The table is gone.');
      return null;
    }
    if (TableStatus && TableStatus !== 'ACTIVE') {
      logger.debug('Waiting for the table to be active…');
      await client.waitFor('tableExists', params).promise();
      logger.debug('The table is now active.');
    }
    return Table.TableArn;
  } catch (err) {
    if (err.code !== 'ResourceNotFoundException') {
      logger.error(err);
      throw err;
    }
    return null;
  }
}

/**
 * Creates a table and waits until its activation.
 *
 * @param {Object} client - An instance of the DynamoDB client.
 * @param {Object} logger - A logger instance.
 * @param {string} tableName - The name of the table to create.
 * @returns {string} The ARN of the new table.
 */
async function createTable(client, logger, tableName) {
  logger.debug(`Trying to create the "${tableName}" table…`);
  const params = { TableName: tableName };
  await client.createTable({ ...params, ...stateTableDefinition }).promise();
  logger.debug('Waiting for the new table to be active…');
  const { Table } = await client.waitFor('tableExists', params).promise();
  logger.debug('The new table is now active.');
  return Table.TableArn;
}

/**
 * Ensures that the table is tagged as expected by reading the tags then updating them if needed.
 *
 * @param {Object} client - An instance of the DynamoDB client.
 * @param {Object} logger - A logger instance.
 * @param {string} tableArn - The ARN of the table to check the tags for.
 * @param {Object} tags - The tags that should be present in the table.
 * @returns {undefined}
 */
async function confirmTableTags(client, logger, tableArn, tags) {
  const params = { ResourceArn: tableArn };
  let { Tags } = await client.listTagsOfResource(params).promise();
  const existingTags = Tags.reduce((obj, { Key, Value }) => ({ ...obj, [Key]: Value }), {});
  const mergedTags = { ...existingTags, ...tags };
  if (!equal(existingTags, mergedTags)) {
    Tags = Object.entries(mergedTags).map(([Key, Value]) => ({ Key, Value }));
    await client.tagResource({ ...params, Tags }).promise();
    logger.debug('The table tags have been updated.');
  } else {
    logger.debug('The table is already tagged as required.');
  }
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
      Item: { streamName, version: generate() }
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
  constructor({ dynamoDbOptions, logger, streamName }) {
    const { tableName, tags, ...awsOptions } = dynamoDbOptions;
    Object.assign(internal(this), {
      awsOptions,
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
}

/**
 * @external dynamoDbConstructor
 * @see https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#constructor-property
 */

module.exports = StateStore;
