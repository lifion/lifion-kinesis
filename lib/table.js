'use strict';

const equal = require('fast-deep-equal');

/**
 * Checks if the specified table exists and would wait until the table is deleted or activated
 * if the table is in the middle of an update.
 *
 * @param {Object} client - An instance of the DynamoDB client.
 * @param {Object} logger - A logger instance.
 * @param {string} tableName - The name of the table to check.
 * @returns {string} If the table exists, the ARN of the table, `null` otherwise.
 */
async function checkIfTableExists({ client, logger, tableName }) {
  try {
    const params = { TableName: tableName };
    const { Table } = await client.describeTable(params);
    const { TableStatus } = Table;

    if (TableStatus === 'DELETING') {
      logger.debug('Waiting for the table to complete deletion…');
      await client.waitFor('tableNotExists', params);
      logger.debug('The table is now gone.');
      return null;
    }

    if (TableStatus && TableStatus !== 'ACTIVE') {
      logger.debug('Waiting for the table to be active…');
      await client.waitFor('tableExists', params);
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
 * Ensures that the table is tagged as expected by reading the tags then updating them if needed.
 *
 * @param {Object} client - An instance of the DynamoDB client.
 * @param {Object} logger - A logger instance.
 * @param {string} tableArn - The ARN of the table to check the tags for.
 * @param {Object} tags - The tags that should be present in the table.
 * @returns {undefined}
 */
async function confirmTableTags({ client, logger, tableArn, tags }) {
  const params = { ResourceArn: tableArn };
  let { Tags } = await client.listTagsOfResource(params);
  const existingTags = Tags.reduce((obj, { Key, Value }) => ({ ...obj, [Key]: Value }), {});
  const mergedTags = { ...existingTags, ...tags };

  if (!equal(existingTags, mergedTags)) {
    Tags = Object.entries(mergedTags).map(([Key, Value]) => ({ Key, Value }));
    await client.tagResource({ ...params, Tags });
    logger.debug('The table tags have been updated.');
  } else {
    logger.debug('The table is already tagged as required.');
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
async function ensureTableExists(props) {
  const { client, logger, tableName } = props;
  logger.debug(`Verifying the "${tableName}" table exists and it's active…`);

  const tableArn = await checkIfTableExists(props);

  if (tableArn === null) {
    logger.debug('Trying to create the table…');
    await client.createTable({
      AttributeDefinitions: [
        { AttributeName: 'consumerGroup', AttributeType: 'S' },
        { AttributeName: 'streamName', AttributeType: 'S' }
      ],
      BillingMode: 'PAY_PER_REQUEST',
      KeySchema: [
        { AttributeName: 'consumerGroup', KeyType: 'HASH' },
        { AttributeName: 'streamName', KeyType: 'RANGE' }
      ],
      SSESpecification: {
        Enabled: true
      },
      TableName: tableName
    });
    logger.debug('Waiting for the new table to be active…');
    const { Table } = await client.waitFor('tableExists', { TableName: tableName });
    logger.debug('The new table is now active.');
    return Table.TableArn;
  }

  logger.debug("The table exists and it's active.");
  return tableArn;
}

module.exports = {
  confirmTableTags,
  ensureTableExists
};
