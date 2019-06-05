/**
 * Module that wraps the calls to the AWS.DynamoDB library. Calls are wrapped so they can be
 * retried with a custom logic instead of the one provided by the AWS-SDK. In addition to retries,
 * calls are also promisified and the call stacks are preserved even in async/await calls by using
 * the `CAPTURE_STACK_TRACE` environment variable.
 *
 * @module dynamodb-client
 * @private
 */

'use strict';

const retry = require('async-retry');
const { DynamoDB } = require('aws-sdk');

const { reportError, reportResponse } = require('./stats');
const { getStackObj, shouldBailRetry, transformErrorStack } = require('./utils');

const privateData = new WeakMap();
const statsSource = 'dynamoDb';

/**
 * Provides access to the private data of the specified instance.
 *
 * @param {Object} instance - The private data's owner.
 * @returns {Object} The private data.
 * @private
 */
function internal(instance) {
  if (!privateData.has(instance)) privateData.set(instance, {});
  return privateData.get(instance);
}

/**
 * Calls a method on the given instance of AWS.DynamoDB. The call is promisified, the call stack
 * is preserved, and the results of the call are aggregated in the stats. Retries in this function
 * are the original ones provided by the AWS-SDK.
 *
 * @param {Object} client - An instance of AWS.DynamoDB.
 * @param {string} methodName - The name of the method to call.
 * @param {...*} args - The arguments of the method call.
 * @fulfil {*} - The original response from the AWS.DynamoDB call.
 * @reject {Error} - The error details from AWS.DynamoDB with a corrected error stack.
 * @returns {Promise}
 * @private
 *
 */
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

/**
 * Calls a method on the given instance of AWS.DynamoDB. The call is promisified, the call stack
 * is preserved, and the results of the call are aggregated in the stats. Retries in this function
 * are based on a custom logic replacing the one provided by the AWS-SDK.
 *
 * @param {Object} client - An instance of AWS.DynamoDB.
 * @param {string} methodName - The name of the method to call.
 * @param {Object} retryOpts - The [retry options as in async-retry]{@link external:AsyncRetry}.
 * @param {...*} args - The argument of the method call.
 * @fulfil {*} - The original response from the AWS.DynamoDB call.
 * @reject {Error} - The error details from AWS.DynamoDB with a corrected error stack.
 * @returns {Promise}
 * @private
 */
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

/**
 * A class that wraps AWS.DynamoDB.
 *
 * @alias module:dynamodb-client
 */
class DynamoDbClient {
  /**
   * Initializes the AWS.DynamoDB internal instance and prepares the retry logic.
   *
   * @param {Object} options - The initialization options.
   * @param {Object} options.awsOptions - The initialization options for AWS.DynamoDB.
   * @param {Object} options.logger - An instace of a logger.
   * @param {string} options.tableName - The name of the DynamoDB table.
   */
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

  /**
   * The CreateTable operation adds a new table to your account.
   *
   * @param {...*} args - The arguments.
   * @returns {Promise}
   */
  createTable(...args) {
    const { client } = internal(this);
    return sdkCall(client, 'createTable', ...args);
  }

  /**
   * Returns information about the table, including the current status of the table, when it was
   * created, the primary key schema, and any indexes on the table.
   *
   * @param {...*} args - The arguments.
   * @returns {Promise}
   */
  describeTable(...args) {
    const { client, retryOpts } = internal(this);
    return retriableSdkCall(client, 'describeTable', retryOpts, ...args);
  }

  /**
   * List all tags on an Amazon DynamoDB resource.
   *
   * @param {...*} args - The arguments.
   * @returns {Promise}
   */
  listTagsOfResource(...args) {
    const { client, retryOpts } = internal(this);
    return retriableSdkCall(client, 'listTagsOfResource', retryOpts, ...args);
  }

  /**
   * Associate a set of tags with an Amazon DynamoDB resource.
   *
   * @param {...*} args - The arguments.
   * @returns {Promise}
   */
  tagResource(...args) {
    const { client } = internal(this);
    return sdkCall(client, 'tagResource', ...args);
  }

  /**
   * Waits for a given DynamoDB resource.
   *
   * @param {...*} args - The arguments.
   * @returns {Promise}
   */
  waitFor(...args) {
    const { client, retryOpts } = internal(this);
    return retriableSdkCall(client, 'waitFor', retryOpts, ...args);
  }

  /**
   * Deletes a single item in a table by primary key by delegating to `AWS.DynamoDB.deleteItem()`.
   *
   * @param {...*} args - The arguments.
   * @returns {Promise}
   */
  delete(...args) {
    const { docClient } = internal(this);
    return sdkCall(docClient, 'delete', ...args);
  }

  /**
   * Returns a set of attributes for the item with the given primary key by delegating to
   * `AWS.DynamoDB.getItem()`.
   *
   * @param {...*} args - The arguments.
   * @returns {Promise}
   */
  get(...args) {
    const { docClient, retryOpts } = internal(this);
    return retriableSdkCall(docClient, 'get', retryOpts, ...args);
  }

  /**
   * Creates a new item, or replaces an old item with a new item by delegating to
   * `AWS.DynamoDB.putItem()`.
   *
   * @param {...*} args - The arguments.
   * @returns {Promise}
   */
  put(...args) {
    const { docClient, retryOpts } = internal(this);
    return retriableSdkCall(docClient, 'put', retryOpts, ...args);
  }

  /**
   * Edits an existing item's attributes, or adds a new item to the table if it does not already
   * exist by delegating to `AWS.DynamoDB.updateItem()`.
   *
   * @param {...*} args - The arguments.
   * @returns {Promise}
   */
  update(...args) {
    const { docClient, retryOpts } = internal(this);
    return retriableSdkCall(docClient, 'update', retryOpts, ...args);
  }
}

/**
 * @external AsyncRetry
 * @see https://github.com/zeit/async-retry#api
 */

module.exports = DynamoDbClient;
