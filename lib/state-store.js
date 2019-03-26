'use strict';

const equal = require('fast-deep-equal');
const { DynamoDB } = require('aws-sdk');
const { name } = require('../package.json');

const privateData = new WeakMap();

function internal(instance) {
  if (!privateData.has(instance)) privateData.set(instance, {});
  return privateData.get(instance);
}

class StateStore {
  constructor({ dynamoDbOptions, logger }) {
    const { tableName, tags, ...awsOptions } = dynamoDbOptions;
    Object.assign(internal(this), {
      awsOptions,
      logger,
      tableName: tableName || `${name}-state`,
      tags
    });
  }

  async start() {
    const privateProps = internal(this);
    const { awsOptions, logger, tableName: TableName, tags } = privateProps;

    const client = new DynamoDB(awsOptions);
    privateProps.client = client;

    let ResourceArn;
    let TableStatus;

    try {
      const { Table } = await client.describeTable({ TableName }).promise();
      ({ TableStatus } = Table);
      ResourceArn = Table.TableArn;
      logger.debug(`The "${TableName}" table status is ${TableStatus}.`);
      if (TableStatus === 'DELETING') {
        logger.debug('Waiting for the table to complete deletion…');
        await client.waitFor('tableNotExists', { TableName }).promise();
        TableStatus = undefined;
        logger.debug('The table is gone.');
      } else if (TableStatus && TableStatus !== 'ACTIVE') {
        logger.debug('Waiting for the table to be active…');
        await client.waitFor('tableExists', { TableName }).promise();
        logger.debug('The table is now active.');
      }
    } catch (err) {
      if (err.code !== 'ResourceNotFoundException') {
        logger.error(err);
        throw err;
      }
    }

    if (!TableStatus) {
      logger.debug(`Trying to create the "${TableName}" table…`);
      await client
        .createTable({
          AttributeDefinitions: [{ AttributeName: 'stream', AttributeType: 'S' }],
          BillingMode: 'PAY_PER_REQUEST',
          KeySchema: [{ AttributeName: 'stream', KeyType: 'HASH' }],
          SSESpecification: { Enabled: true },
          TableName
        })
        .promise();
      logger.debug('Waiting for the new table to be active…');
      const { Table } = await client.waitFor('tableExists', { TableName }).promise();
      ResourceArn = Table.TableArn;
      logger.debug('The new table is now active.');
    }

    let { Tags } = await client.listTagsOfResource({ ResourceArn }).promise();
    const existingTags = Tags.reduce((obj, { Key, Value }) => ({ ...obj, [Key]: Value }), {});
    const mergedTags = { ...existingTags, ...tags };

    if (!equal(existingTags, mergedTags)) {
      Tags = Object.entries(mergedTags).map(([Key, Value]) => ({ Key, Value }));
      await client.tagResource({ ResourceArn, Tags }).promise();
      logger.debug('The table tags have been updated.');
    } else {
      logger.debug('The table is already tagged as required.');
    }
  }
}

module.exports = StateStore;
