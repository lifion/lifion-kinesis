'use strict';

const retry = require('async-retry');
const { DynamoDB } = require('aws-sdk');

const { reportError, reportResponse } = require('./stats');
const { getStackObj, shouldBailRetry, transformErrorStack } = require('./utils');

const privateData = new WeakMap();
const statsSource = 'dynamoDb';

function internal(instance) {
  if (!privateData.has(instance)) privateData.set(instance, {});
  return privateData.get(instance);
}

async function sdkCall(client, methodName, ...args) {
  const stackObj = getStackObj(sdkCall);
  try {
    return client[methodName](...args)
      .promise()
      .then(response => {
        reportResponse(statsSource);
        return response;
      })
      .catch(err => {
        const error = transformErrorStack(err, stackObj);
        reportError(statsSource, error);
        throw error;
      });
  } catch (err) {
    const error = transformErrorStack(err, stackObj);
    reportError(statsSource, error);
    throw error;
  }
}

function retriableSdkCall(client, methodName, retryOpts, ...args) {
  const stackObj = getStackObj(retriableSdkCall);
  return retry(bail => {
    try {
      return client[methodName](...args)
        .promise()
        .then(response => {
          reportResponse(statsSource);
          return response;
        })
        .catch(err => {
          const error = transformErrorStack(err, stackObj);
          reportError(statsSource, error);
          if (!shouldBailRetry(err)) throw error;
          else bail(error);
        });
    } catch (err) {
      const error = transformErrorStack(err, stackObj);
      reportError(statsSource, error);
      bail(error);
      return undefined;
    }
  }, retryOpts);
}

class DynamoDbClient {
  constructor({ awsOptions, logger, tableName }) {
    const client = new DynamoDB(awsOptions);

    const docClient = new DynamoDB.DocumentClient({
      params: { TableName: tableName },
      service: client
    });

    const retryOpts = {
      maxTimeout: 5 * 60 * 1000,
      minTimeout: 1000,
      onRetry: err => {
        const { code, message, requestId, statusCode } = err;
        logger.warn(
          `Trying to recover from AWS.DynamoDB error…\n${[
            `\t- Message: ${message}`,
            `\t- Request ID: ${requestId}`,
            `\t- Code: ${code} (${statusCode})`,
            `\t- Table: ${tableName}`
          ].join('\n')}`
        );
      },
      randomize: true,
      retries: 100000
    };

    Object.assign(internal(this), { client, docClient, retryOpts });
  }

  createTable(...args) {
    const { client } = internal(this);
    return sdkCall(client, 'createTable', ...args);
  }

  describeTable(...args) {
    const { client, retryOpts } = internal(this);
    return retriableSdkCall(client, 'describeTable', retryOpts, ...args);
  }

  listTagsOfResource(...args) {
    const { client, retryOpts } = internal(this);
    return retriableSdkCall(client, 'listTagsOfResource', retryOpts, ...args);
  }

  tagResource(...args) {
    const { client } = internal(this);
    return sdkCall(client, 'tagResource', ...args);
  }

  waitFor(...args) {
    const { client, retryOpts } = internal(this);
    return retriableSdkCall(client, 'waitFor', retryOpts, ...args);
  }

  delete(...args) {
    const { docClient } = internal(this);
    return sdkCall(docClient, 'delete', ...args);
  }

  get(...args) {
    const { docClient, retryOpts } = internal(this);
    return retriableSdkCall(docClient, 'get', retryOpts, ...args);
  }

  put(...args) {
    const { docClient, retryOpts } = internal(this);
    return retriableSdkCall(docClient, 'put', retryOpts, ...args);
  }

  update(...args) {
    const { docClient, retryOpts } = internal(this);
    return retriableSdkCall(docClient, 'update', retryOpts, ...args);
  }
}
module.exports = DynamoDbClient;
