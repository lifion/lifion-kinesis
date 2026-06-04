/**
 * Module that wraps the calls to the AWS DynamoDB client. Calls are wrapped so they can be
 * retried with a custom logic instead of the one provided by the AWS SDK. In addition to retries,
 * the call stacks are preserved even in async/await calls by using the `CAPTURE_STACK_TRACE`
 * environment variable.
 *
 * @module dynamodb-client
 * @private
 */

import retry from 'async-retry';
import { DynamoDB, waitUntilTableExists, waitUntilTableNotExists } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocument } from '@aws-sdk/lib-dynamodb';

import { reportError, reportResponse } from './stats.js';
import { getStackObj, shouldBailRetry, transformErrorStack } from './utils.js';
import { assertCredentialsSupported } from './aws-credentials.js';

const TABLE_WAIT_TIME_SECONDS = 300;
const statsSource = 'dynamoDb';
const stoppedClients = new WeakSet();

/**
 * Calls a method on the given DynamoDB client. The call stack is preserved, and the results of the
 * call are aggregated in the stats. Retries in this function are the original ones provided by the
 * AWS SDK.
 *
 * @param {Object} client - An instance of the AWS DynamoDB client.
 * @param {string} methodName - The name of the method to call.
 * @param {...*} args - The arguments of the method call.
 * @fulfil {*} - The original response from the AWS DynamoDB call.
 * @reject {Error} - The error details from AWS DynamoDB with a corrected error stack.
 * @returns {Promise}
 * @private
 */
async function sdkCall(client, methodName, ...args) {
  const stackObj = getStackObj(sdkCall);
  try {
    return client[methodName](...args)
      .then((response) => {
        reportResponse(statsSource);
        return response;
      })
      .catch((err) => {
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
 * Calls a method on the given DynamoDB client. The call stack is preserved, and the results of the
 * call are aggregated in the stats. Retries in this function are based on a custom logic replacing
 * the one provided by the AWS SDK.
 *
 * @param {Object} client - An instance of the AWS DynamoDB client.
 * @param {string} methodName - The name of the method to call.
 * @param {Object} retryOpts - The [retry options as in async-retry]{@link external:AsyncRetry}.
 * @param {...*} args - The argument of the method call.
 * @fulfil {*} - The original response from the AWS DynamoDB call.
 * @reject {Error} - The error details from AWS DynamoDB with a corrected error stack.
 * @returns {Promise}
 * @private
 */
function retriableSdkCall(client, methodName, retryOpts, ...args) {
  const stackObj = getStackObj(retriableSdkCall);
  return retry((bail) => {
    if (stoppedClients.has(client)) {
      bail(new Error('The DynamoDB client is stopped.'));
      return undefined;
    }
    try {
      return client[methodName](...args)
        .then((response) => {
          reportResponse(statsSource);
          return response;
        })
        .catch((err) => {
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
 * A class that wraps the AWS DynamoDB client.
 *
 * @alias module:dynamodb-client
 */
class DynamoDbClient {
  #data = {};

  /**
   * Initializes the AWS DynamoDB internal instance and prepares the retry logic.
   *
   * @param {Object} options - The initialization options.
   * @param {Object} options.awsOptions - The initialization options for the AWS DynamoDB client.
   * @param {Object} options.logger - An instace of a logger.
   * @param {string} options.tableName - The name of the DynamoDB table.
   */
  constructor({ awsOptions, logger, tableName }) {
    assertCredentialsSupported(awsOptions);
    const client = new DynamoDB(awsOptions);

    const docClient = DynamoDBDocument.from(client, {
      marshallOptions: { removeUndefinedValues: true }
    });

    const retryOpts = {
      forever: true,
      maxTimeout: 5 * 60 * 1000,
      minTimeout: 1000,
      onRetry: (err) => {
        const { code, message, requestId, statusCode } = err;
        logger.warn(
          `Trying to recover from AWS DynamoDB error…\n${[
            `\t- Message: ${message}`,
            `\t- Request ID: ${requestId}`,
            `\t- Code: ${code} (${statusCode})`,
            `\t- Table: ${tableName}`
          ].join('\n')}`
        );
      },
      randomize: true
    };

    Object.assign(this.#data, { client, docClient, logger, retryOpts, tableName });
  }

  /**
   * Stops the client. Pending and future retriable calls bail out instead of
   * retrying, so the client stops holding the event loop open.
   */
  stop() {
    const { client, docClient } = this.#data;
    stoppedClients.add(client);
    stoppedClients.add(docClient);
  }

  /**
   * The CreateTable operation adds a new table to your account.
   *
   * @param {...*} args - The arguments.
   * @returns {Promise}
   */
  async createTable(...args) {
    const { client, logger } = this.#data;
    try {
      await sdkCall(client, 'createTable', ...args);
      return undefined;
    } catch (err) {
      if (err.code === 'ResourceInUseException') {
        logger.debug('The table already exists.');
        return undefined;
      }
      throw err;
    }
  }

  /**
   * Returns information about the table, including the current status of the table, when it was
   * created, the primary key schema, and any indexes on the table.
   *
   * @param {...*} args - The arguments.
   * @returns {Promise}
   */
  describeTable(...args) {
    const { client, retryOpts } = this.#data;
    return retriableSdkCall(client, 'describeTable', retryOpts, ...args);
  }

  /**
   * List all tags on an Amazon DynamoDB resource.
   *
   * @param {...*} args - The arguments.
   * @returns {Promise}
   */
  listTagsOfResource(...args) {
    const { client, retryOpts } = this.#data;
    return retriableSdkCall(client, 'listTagsOfResource', retryOpts, ...args);
  }

  /**
   * Associate a set of tags with an Amazon DynamoDB resource.
   *
   * @param {...*} args - The arguments.
   * @returns {Promise}
   */
  async tagResource(...args) {
    const { client, logger } = this.#data;
    try {
      await sdkCall(client, 'tagResource', ...args);
      return undefined;
    } catch (err) {
      if (err.code === 'ResourceInUseException') {
        logger.debug('Ignoring concurrent modification of resource.');
        return undefined;
      }
      throw err;
    }
  }

  /**
   * Waits for the specified DynamoDB table to reach the given state.
   *
   * @param {string} state - The state to wait for, either `tableExists` or `tableNotExists`.
   * @param {Object} params - The parameters identifying the table (e.g. `{ TableName }`).
   * @fulfil {Object} - The last describe-table response observed by the waiter.
   * @returns {Promise}
   */
  async waitFor(state, params) {
    const { client } = this.#data;
    const stackObj = getStackObj(this.waitFor);
    const waiter = state === 'tableNotExists' ? waitUntilTableNotExists : waitUntilTableExists;
    try {
      const { reason } = await waiter({ client, maxWaitTime: TABLE_WAIT_TIME_SECONDS }, params);
      reportResponse(statsSource);
      return reason;
    } catch (err) {
      const error = transformErrorStack(err, stackObj);
      reportError(statsSource, error);
      throw error;
    }
  }

  /**
   * Deletes a single item in a table by primary key.
   *
   * @param {...*} args - The arguments.
   * @returns {Promise}
   */
  delete(...args) {
    const { docClient, tableName } = this.#data;
    const [params, ...rest] = args;
    return sdkCall(docClient, 'delete', { TableName: tableName, ...params }, ...rest);
  }

  /**
   * Returns a set of attributes for the item with the given primary key.
   *
   * @param {...*} args - The arguments.
   * @returns {Promise}
   */
  get(...args) {
    const { docClient, retryOpts, tableName } = this.#data;
    const [params, ...rest] = args;
    return retriableSdkCall(
      docClient,
      'get',
      retryOpts,
      { TableName: tableName, ...params },
      ...rest
    );
  }

  /**
   * Creates a new item, or replaces an old item with a new item.
   *
   * @param {...*} args - The arguments.
   * @returns {Promise}
   */
  put(...args) {
    const { docClient, retryOpts, tableName } = this.#data;
    const [params, ...rest] = args;
    return retriableSdkCall(
      docClient,
      'put',
      retryOpts,
      { TableName: tableName, ...params },
      ...rest
    );
  }

  /**
   * Edits an existing item's attributes, or adds a new item to the table if it does not already
   * exist.
   *
   * @param {...*} args - The arguments.
   * @returns {Promise}
   */
  update(...args) {
    const { docClient, retryOpts, tableName } = this.#data;
    const [params, ...rest] = args;
    return retriableSdkCall(
      docClient,
      'update',
      retryOpts,
      { TableName: tableName, ...params },
      ...rest
    );
  }
}

/**
 * @external AsyncRetry
 * @see https://github.com/zeit/async-retry#api
 */

export default DynamoDbClient;
