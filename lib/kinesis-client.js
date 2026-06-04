/**
 * Module that wraps the calls to the AWS Kinesis client. Calls are wrapped so they can be
 * retried with a custom logic instead of the one provided by the AWS SDK. In addition to retries,
 * the call stacks are preserved even in async/await calls by using the `CAPTURE_STACK_TRACE`
 * environment variable.
 *
 * @module kinesis-client
 * @private
 */

import retry from 'async-retry';
import { Kinesis, waitUntilStreamExists, waitUntilStreamNotExists } from '@aws-sdk/client-kinesis';

import { getStackObj, shouldBailRetry, transformErrorStack } from './utils.js';
import { reportError, reportRecordSent, reportResponse } from './stats.js';
import { assertCredentialsSupported } from './aws-credentials.js';

const RETRIABLE_PUT_ERRORS = new Set([
  'EADDRINUSE',
  'ECONNREFUSED',
  'ECONNRESET',
  'EPIPE',
  'ESOCKETTIMEDOUT',
  'ETIMEDOUT',
  'NetworkingError',
  'ProvisionedThroughputExceededException',
  'TimeoutError'
]);

const STREAM_WAIT_TIME_SECONDS = 180;
const statsSource = 'kinesis';
const stoppedClients = new WeakSet();

/**
 * Calls a method on the given Kinesis client. The call stack is preserved, and the results of the
 * call are aggregated in the stats. Retries in this function are the original ones provided by the
 * AWS SDK.
 *
 * @param {Object} client - An instance of the AWS Kinesis client.
 * @param {string} methodName - The name of the method to call.
 * @param {string} streamName - The name of the Kinesis stream for which the call relates to.
 * @param {...*} args - The arguments of the method call.
 * @fulfil {*} - The original response from the AWS Kinesis call.
 * @reject {Error} - The error details from AWS Kinesis with a corrected error stack.
 * @returns {Promise}
 * @private
 */
async function sdkCall(client, methodName, streamName, ...args) {
  const stackObj = getStackObj(sdkCall);
  try {
    return client[methodName](...args)
      .then((response) => {
        reportResponse(statsSource, streamName);
        return response;
      })
      .catch((err) => {
        const error = transformErrorStack(err, stackObj);
        reportError(statsSource, error, streamName);
        throw error;
      });
  } catch (err) {
    const error = transformErrorStack(err, stackObj);
    reportError(statsSource, error, streamName);
    throw error;
  }
}

/**
 * Calls a method on the given Kinesis client. The call stack is preserved, and the results of the
 * call are aggregated in the stats. Retries in this function are based on a custom logic replacing
 * the one provided by the AWS SDK.
 *
 * @param {Object} client - An instance of the AWS Kinesis client.
 * @param {string} methodName - The name of the method to call.
 * @param {string} streamName - The name of the Kinesis stream for which the call relates to.
 * @param {Object} retryOpts - The [retry options as in async-retry]{@link external:AsyncRetry}.
 * @param {...*} args - The argument of the method call.
 * @fulfil {*} - The original response from the AWS Kinesis call.
 * @reject {Error} - The error details from AWS Kinesis with a corrected error stack.
 * @returns {Promise}
 * @private
 */
function retriableSdkCall(client, methodName, streamName, retryOpts, ...args) {
  const stackObj = getStackObj(retriableSdkCall);
  return retry((bail) => {
    if (stoppedClients.has(client)) {
      bail(new Error('The Kinesis client is stopped.'));
      return undefined;
    }
    try {
      return client[methodName](...args)
        .then((response) => {
          reportResponse(statsSource, streamName);
          return response;
        })
        .catch((err) => {
          const error = transformErrorStack(err, stackObj);
          reportError(statsSource, error, streamName);
          if (!shouldBailRetry(err)) throw error;
          else bail(error);
        });
    } catch (err) {
      const error = transformErrorStack(err, stackObj);
      reportError(statsSource, error, streamName);
      bail(error);
      return undefined;
    }
  }, retryOpts);
}

/**
 * A class that wraps the AWS Kinesis client.
 *
 * @alias module:kinesis-client
 */
class KinesisClient {
  #data = {};

  /**
   * Initializes the AWS Kinesis internal instance and prepares the retry logic.
   *
   * @param {Object} options - The initialization options.
   * @param {Object} options.awsOptions - The initialization options for the AWS Kinesis client.
   * @param {Object} options.logger - An instace of a logger.
   * @param {Object} [options.retryOptions] - The [retry options as in async-retry]{@link
   *        external:AsyncRetry}, merged over the defaults (which retry forever with backoff).
   * @param {string} options.streamName - The name of the Kinesis stream for which calls relate to.
   * @param {boolean} options.supressThroughputWarnings - Flag indicating whether or not
   *        to supress ProvisionedThroughputExceededException warning logs.
   */
  constructor({ awsOptions, logger, retryOptions, streamName, supressThroughputWarnings }) {
    assertCredentialsSupported(awsOptions);
    const client = new Kinesis(awsOptions);

    const retryOpts = {
      forever: true,
      maxTimeout: 5 * 60 * 1000,
      minTimeout: 1000,
      onRetry: (err) => {
        const { code, message, requestId, statusCode } = err;
        const loggerMethod =
          supressThroughputWarnings && code === 'ProvisionedThroughputExceededException'
            ? 'debug'
            : 'warn';
        logger[loggerMethod](
          `Trying to recover from AWS Kinesis error…\n${[
            `\t- Message: ${message}`,
            `\t- Request ID: ${requestId}`,
            `\t- Code: ${code} (${statusCode})`,
            `\t- Stream: ${streamName}`
          ].join('\n')}`
        );
      },
      randomize: true,
      ...retryOptions
    };

    Object.assign(this.#data, {
      client,
      endpoint: awsOptions && awsOptions.endpoint,
      retryOpts,
      streamName
    });
  }

  /**
   * Stops the client so its pending and future retriable calls stop retrying.
   */
  stop() {
    const { client } = this.#data;
    stoppedClients.add(client);
  }

  /**
   * Resolves the AWS credentials the client is configured with. The enhanced fan-out signer uses
   * this so it signs requests with the same identity as the rest of the client.
   *
   * @fulfil {Object} - The resolved credentials.
   * @returns {Promise}
   */
  getCredentials() {
    const { client } = this.#data;
    return client.config.credentials();
  }

  /**
   * Adds or updates tags for the specified Kinesis data stream.
   *
   * @param {...*} args - The arguments.
   * @returns {Promise}
   */
  addTagsToStream(...args) {
    const { client, streamName } = this.#data;
    return sdkCall(client, 'addTagsToStream', streamName, ...args);
  }

  /**
   * Creates a Kinesis data stream.
   *
   * @param {...*} args - The arguments.
   * @returns {Promise}
   */
  createStream(...args) {
    const { client, streamName } = this.#data;
    return sdkCall(client, 'createStream', streamName, ...args).catch((err) => {
      if (err.code !== 'ResourceInUseException') throw err;
    });
  }

  /**
   * To deregister a consumer, provide its ARN.
   *
   * @param {...*} args - The arguments.
   * @returns {Promise}
   */
  deregisterStreamConsumer(...args) {
    const { client, streamName } = this.#data;
    return sdkCall(client, 'deregisterStreamConsumer', streamName, ...args);
  }

  /**
   * Describes the specified Kinesis data stream.
   *
   * @param {...*} args - The arguments.
   * @returns {Promise}
   */
  describeStream(...args) {
    const { client, retryOpts, streamName } = this.#data;
    return retriableSdkCall(client, 'describeStream', streamName, retryOpts, ...args);
  }

  /**
   * Summarizes the specified Kinesis data stream.
   *
   * @param {...*} args - The arguments.
   * @returns {Promise}
   */
  describeStreamSummary(...args) {
    const { client, retryOpts, streamName } = this.#data;

    return sdkCall(client, 'describeStreamSummary', streamName, ...args).catch((err) => {
      if (err.code !== 'UnknownOperationException') throw err;

      return retriableSdkCall(client, 'describeStream', streamName, retryOpts, ...args).then(
        (data) => {
          const { StreamDescription } = data;
          return { StreamDescriptionSummary: StreamDescription };
        }
      );
    });
  }

  /**
   * Gets data records from a Kinesis data stream's shard.
   *
   * @param {...*} args - The arguments.
   * @returns {Promise}
   */
  async getRecords(...args) {
    const { client, retryOpts, streamName } = this.#data;
    const response = await retriableSdkCall(client, 'getRecords', streamName, retryOpts, ...args);
    const { Records } = response;
    if (Records) {
      for (const record of Records) {
        if (record.Data) record.Data = Buffer.from(record.Data);
      }
    }
    return response;
  }

  /**
   * Gets an Amazon Kinesis shard iterator.
   *
   * @param {...*} args - The arguments.
   * @returns {Promise}
   */
  getShardIterator(...args) {
    const { client, retryOpts, streamName } = this.#data;
    return retriableSdkCall(client, 'getShardIterator', streamName, retryOpts, ...args);
  }

  /**
   * Tells whether the endpoint of the client is local or not.
   *
   * @returns {boolean} `true` if the endpoints is local, `false` otherwise.
   */
  isEndpointLocal() {
    const { endpoint } = this.#data;
    return (
      typeof endpoint === 'string' &&
      (endpoint.includes('localhost') || endpoint.includes('localstack'))
    );
  }

  /**
   * Lists the shards in a stream and provides information about each shard.
   *
   * @param {...*} args - The arguments.
   * @returns {Promise}
   */
  listShards(...args) {
    const { client, retryOpts, streamName } = this.#data;
    return retriableSdkCall(client, 'listShards', streamName, retryOpts, ...args);
  }

  /**
   * Lists the consumers registered to receive data from a stream using enhanced fan-out, and
   * provides information about each consumer.
   *
   * @param {...*} args - The arguments.
   * @returns {Promise}
   */
  listStreamConsumers(...args) {
    const { client, retryOpts, streamName } = this.#data;
    return retriableSdkCall(client, 'listStreamConsumers', streamName, retryOpts, ...args);
  }

  /**
   * Lists the tags for the specified Kinesis data stream.
   *
   * @param {...*} args - The arguments.
   * @returns {Promise}
   */
  listTagsForStream(...args) {
    const { client, retryOpts, streamName } = this.#data;
    return retriableSdkCall(client, 'listTagsForStream', streamName, retryOpts, ...args);
  }

  /**
   * Writes a single data record into an Amazon Kinesis data stream.
   *
   * @param {...*} args - The arguments.
   * @returns {Promise}
   */
  putRecord(...args) {
    const { client, retryOpts, streamName } = this.#data;
    const stackObj = getStackObj(retriableSdkCall);
    return retry((bail) => {
      try {
        return client
          .putRecord(...args)
          .then((result) => {
            reportResponse(statsSource, streamName);
            reportRecordSent(streamName);
            return result;
          })
          .catch((err) => {
            const error = transformErrorStack(err, stackObj);
            reportError(statsSource, error, streamName);
            if (RETRIABLE_PUT_ERRORS.has(err.code)) throw error;
            else bail(error);
          });
      } catch (err) {
        const error = transformErrorStack(err, stackObj);
        reportError(statsSource, error, streamName);
        bail(error);
        return undefined;
      }
    }, retryOpts);
  }

  /**
   * Writes multiple data records into a Kinesis data stream in a single call (also referred to as
   * a PutRecords request).
   *
   * @param {...*} args - The arguments.
   * @returns {Promise}
   */
  putRecords(...args) {
    const { client, retryOpts, streamName } = this.#data;
    const stackObj = getStackObj(retriableSdkCall);
    const [firstArg, ...restOfArgs] = args;
    let records = firstArg.Records;
    const results = [];
    return retry((bail) => {
      try {
        return client
          .putRecords({ ...firstArg, Records: records }, ...restOfArgs)
          .then((payload) => {
            const { EncryptionType, FailedRecordCount, Records } = payload;
            const failedCount = FailedRecordCount;
            const recordsCount = Records.length;
            const nextRecords = [];
            for (let i = 0; i < recordsCount; i += 1) {
              if (Records[i].ErrorCode) nextRecords.push(records[i]);
              else results.push(Records[i]);
            }
            reportResponse(statsSource, streamName);
            if (failedCount < records.length) {
              reportRecordSent(streamName);
            }
            if (failedCount === 0) {
              return { EncryptionType, Records: results };
            }
            records = nextRecords;
            const error = new Error(`Failed to write ${failedCount} of ${recordsCount} record(s).`);
            error.code = 'ProvisionedThroughputExceededException';
            throw error;
          })
          .catch((err) => {
            const error = transformErrorStack(err, stackObj);
            reportError(statsSource, error, streamName);
            if (RETRIABLE_PUT_ERRORS.has(err.code)) throw error;
            else bail(error);
          });
      } catch (err) {
        const error = transformErrorStack(err, stackObj);
        reportError(statsSource, error, streamName);
        bail(error);
        return undefined;
      }
    }, retryOpts);
  }

  /**
   * Registers a consumer with a Kinesis data stream.
   *
   * @param {...*} args - The arguments.
   * @returns {Promise}
   */
  registerStreamConsumer(...args) {
    const { client, streamName } = this.#data;
    return sdkCall(client, 'registerStreamConsumer', streamName, ...args);
  }

  /**
   * Enables or updates server-side encryption using an AWS KMS key for a specified stream.
   *
   * @param {...*} args - The arguments.
   * @returns {Promise}
   */
  startStreamEncryption(...args) {
    const { client, streamName } = this.#data;
    return sdkCall(client, 'startStreamEncryption', streamName, ...args).catch((err) => {
      const { code } = err;
      if (code !== 'UnknownOperationException' && code !== 'ResourceInUseException') throw err;
    });
  }

  /**
   * Waits for the specified Kinesis stream to reach the given state.
   *
   * @param {string} state - The state to wait for, either `streamExists` or `streamNotExists`.
   * @param {Object} params - The parameters identifying the stream (e.g. `{ StreamName }`).
   * @fulfil {Object} - The last describe-stream response observed by the waiter.
   * @returns {Promise}
   */
  async waitFor(state, params) {
    const { client, streamName } = this.#data;
    const stackObj = getStackObj(this.waitFor);
    const waiter = state === 'streamNotExists' ? waitUntilStreamNotExists : waitUntilStreamExists;
    try {
      const { reason } = await waiter({ client, maxWaitTime: STREAM_WAIT_TIME_SECONDS }, params);
      reportResponse(statsSource, streamName);
      return reason;
    } catch (err) {
      const error = transformErrorStack(err, stackObj);
      reportError(statsSource, error, streamName);
      throw error;
    }
  }
}

/**
 * @external AsyncRetry
 * @see https://github.com/zeit/async-retry#api
 */

export default KinesisClient;
