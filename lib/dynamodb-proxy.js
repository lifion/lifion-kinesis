'use strict';

const { DynamoDB } = require('aws-sdk');

const privateData = new WeakMap();

function internal(instance) {
  if (!privateData.has(instance)) privateData.set(instance, {});
  return privateData.get(instance);
}

class DynamoDBProxy {
  constructor({ awsOptions, tableName }) {
    const privateProps = internal(this);
    privateProps.client = new DynamoDB(awsOptions);
    privateProps.docClient = new DynamoDB.DocumentClient({
      params: { TableName: tableName },
      service: privateProps.client
    });
  }

  async describeTable(...args) {
    return internal(this)
      .client.describeTable(...args)
      .promise()
      .catch(err => {
        const error = new Error(err.message);
        error.code = err.code;
        throw error;
      });
  }

  async listTagsOfResource(...args) {
    return internal(this)
      .client.listTagsOfResource(...args)
      .promise()
      .catch(err => {
        const error = new Error(err.message);
        error.code = err.code;
        throw error;
      });
  }

  async put(...args) {
    return internal(this)
      .docClient.put(...args)
      .promise()
      .catch(err => {
        const error = new Error(err.message);
        error.code = err.code;
        throw error;
      });
  }

  async get(...args) {
    // console.trace('GET');
    return internal(this)
      .docClient.get(...args)
      .promise()
      .catch(err => {
        const error = new Error(err.message);
        error.code = err.code;
        throw error;
      });
  }

  async update(...args) {
    // console.trace('UPDATE');
    return internal(this)
      .docClient.update(...args)
      .promise()
      .catch(err => {
        const error = new Error(err.message);
        error.code = err.code;
        throw error;
      });
  }
}

module.exports = DynamoDBProxy;
