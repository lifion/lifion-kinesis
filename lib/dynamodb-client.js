'use strict';

const retry = require('async-retry');
const { DynamoDB } = require('aws-sdk');

const privateData = new WeakMap();

function internal(instance) {
  if (!privateData.has(instance)) privateData.set(instance, {});
  return privateData.get(instance);
}

function shouldBailRetry(err) {
  const { code } = err;
  return (
    code === 'MissingParameter' ||
    code === 'MissingRequiredParameter' ||
    code === 'MultipleValidationErrors' ||
    code === 'UnexpectedParameter' ||
    code === 'ValidationException'
  );
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
            `\t- Code: ${code} (${statusCode})`
          ].join('\n')}`
        );
      },
      randomize: true,
      retries: 100000
    };
    Object.assign(internal(this), { client, docClient, retryOpts });
  }

  createTable(...args) {
    return internal(this)
      .client.createTable(...args)
      .promise()
      .catch(err => {
        const { code, message } = err;
        const error = new Error(message);
        error.code = code;
        throw error;
      });
  }

  describeTable(...args) {
    const { client, retryOpts } = internal(this);
    return retry(
      bail =>
        client
          .describeTable(...args)
          .promise()
          .catch(err => {
            const { code } = err;
            if (code === 'ResourceNotFoundException' || shouldBailRetry(err)) bail();
            else throw err;
          }),
      retryOpts
    ).catch(err => {
      const { code, message } = err;
      const error = new Error(message);
      error.code = code;
      throw error;
    });
  }

  get(...args) {
    const { docClient, retryOpts } = internal(this);
    return retry(
      bail =>
        docClient
          .get(...args)
          .promise()
          .catch(err => {
            if (shouldBailRetry(err)) bail(err);
            else throw err;
          }),
      retryOpts
    ).catch(err => {
      const { code, message } = err;
      const error = new Error(message);
      error.code = code;
      throw error;
    });
  }

  listTagsOfResource(...args) {
    const { client, retryOpts } = internal(this);
    return retry(
      bail =>
        client
          .listTagsOfResource(...args)
          .promise()
          .catch(err => {
            if (shouldBailRetry(err)) bail(err);
            else throw err;
          }),
      retryOpts
    ).catch(err => {
      const { code, message } = err;
      const error = new Error(message);
      error.code = code;
      throw error;
    });
  }

  put(...args) {
    const { docClient, retryOpts } = internal(this);
    return retry(
      bail =>
        docClient
          .put(...args)
          .promise()
          .catch(err => {
            const { code } = err;
            if (code === 'ConditionalCheckFailedException' || shouldBailRetry(err)) bail(err);
            else throw err;
          }),
      retryOpts
    ).catch(err => {
      const { code, message } = err;
      const error = new Error(message);
      error.code = code;
      throw error;
    });
  }

  tagResource(...args) {
    return internal(this)
      .client.tagResource(...args)
      .promise()
      .catch(err => {
        const { code, message } = err;
        const error = new Error(message);
        error.code = code;
        throw error;
      });
  }

  update(...args) {
    const { docClient, retryOpts } = internal(this);
    return retry(
      bail =>
        docClient
          .update(...args)
          .promise()
          .catch(err => {
            const { code } = err;
            if (code === 'ConditionalCheckFailedException' || shouldBailRetry(err)) bail(err);
            else throw err;
          }),
      retryOpts
    ).catch(err => {
      const { code, message } = err;
      const error = new Error(message);
      error.code = code;
      throw error;
    });
  }

  waitFor(...args) {
    const { client, retryOpts } = internal(this);
    return retry(
      bail =>
        client
          .waitFor(...args)
          .promise()
          .catch(err => {
            if (shouldBailRetry(err)) bail(err);
            else throw err;
          }),
      retryOpts
    ).catch(err => {
      const { code, message } = err;
      const error = new Error(message);
      error.code = code;
      throw error;
    });
  }

  delete(...args) {
    return internal(this)
      .docClient.delete(...args)
      .promise()
      .catch(err => {
        const { code, message } = err;
        const error = new Error(message);
        error.code = code;
        throw error;
      });
  }
}
module.exports = DynamoDbClient;