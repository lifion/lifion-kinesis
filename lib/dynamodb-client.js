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

  createTable(...args) {
    return internal(this)
      .client.createTable(...args)
      .promise()
      .catch(err => {
        const error = new Error(err.message);
        error.code = err.code;
        throw error;
      });
  }

  describeTable(...args) {
    return internal(this)
      .client.describeTable(...args)
      .promise()
      .catch(err => {
        const error = new Error(err.message);
        error.code = err.code;
        throw error;
      });
  }

  get(...args) {
    return internal(this)
      .docClient.get(...args)
      .promise()
      .catch(err => {
        const error = new Error(err.message);
        error.code = err.code;
        throw error;
      });
  }

  listTagsOfResource(...args) {
    return internal(this)
      .client.listTagsOfResource(...args)
      .promise()
      .catch(err => {
        const error = new Error(err.message);
        error.code = err.code;
        throw error;
      });
  }

  put(...args) {
    return internal(this)
      .docClient.put(...args)
      .promise()
      .catch(err => {
        const error = new Error(err.message);
        error.code = err.code;
        throw error;
      });
  }

  tagResource(...args) {
    return internal(this)
      .client.tagResource(...args)
      .promise()
      .catch(err => {
        const error = new Error(err.message);
        error.code = err.code;
        throw error;
      });
  }

  update(...args) {
    return internal(this)
      .docClient.update(...args)
      .promise()
      .catch(err => {
        const error = new Error(err.message);
        error.code = err.code;
        throw error;
      });
  }

  waitFor(...args) {
    return internal(this)
      .client.waitFor(...args)
      .promise()
      .catch(err => {
        const error = new Error(err.message);
        error.code = err.code;
        throw error;
      });
  }
}
module.exports = DynamoDBProxy;
