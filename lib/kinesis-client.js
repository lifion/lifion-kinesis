/**
 * Module that wraps the calls to the AWS.Kinesis library. Calls are wrapped so they can be
 * retried with a custom logic instead of the one provided by the AWS-SDK. In addition to retries,
 * calls are also promisified and the call stacks are preserved even in async/await calls by using
 * the `CAPTURE_STACK_TRACE` environment variable.
 *
 * @module kinesis-client
 * @private
 */

'use strict';

const retry = require('async-retry');
const { Kinesis } = require('aws-sdk');

const { getStackObj, shouldBailRetry, transformErrorStack } = require('./utils');
const { reportError, reportRecordSent, reportResponse } = require('./stats');

const RETRIABLE_PUT_ERRORS = [
  'EADDRINUSE',
  'ECONNREFUSED',
  'ECONNRESET',
  'EPIPE',
  'ESOCKETTIMEDOUT',
  'ETIMEDOUT',
  'NetworkingError',
  'ProvisionedThroughputExceededException'
];

const privateData = new WeakMap();
const statsSource = 'kinesis';

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
 * Calls a method on the given instance of AWS.Kinesis. The call is promisified, the call stack
 * is preserved, and the results of the call are aggregated in the stats. Retries in this function
 * are the original ones provided by the AWS-SDK.
 *
 * @param {Object} client - An instance of AWS.Kinesis.
 * @param {string} methodName - The name of the method to call.
 * @param {string} streamName - The name of the Kinesis stream for which the call relates to.
 * @param {...*} args - The arguments of the method call.
 * @fulfil {*} - The original response from the AWS.Kinesis call.
 * @reject {Error} - The error details from AWS.Kinesis with a corrected error stack.
 * @returns {Promise}
 * @private
 */
async function sdkCall(client, methodName, streamName, ...args) {
  const stackObj = getStackObj(sdkCall);
  try {
    return client[methodName](...args)
      .promise()
      .then(response => {
        reportResponse(statsSource, streamName);
        return response;
      })
      .catch(err => {
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
 * Calls a method on the given instance of AWS.Kinesis. The call is promisified, the call stack
 * is preserved, and the results of the call are aggregated in the stats. Retries in this function
 * are based on a custom logic replacing the one provided by the AWS-SDK.
 *
 * @param {Object} client - An instance of AWS.Kinesis.
 * @param {string} methodName - The name of the method to call.
 * @param {string} streamName - The name of the Kinesis stream for which the call relates to.
 * @param {Object} retryOpts - The [retry options as in async-retry]{@link external:AsyncRetry}.
 * @param {...*} args - The argument of the method call.
 * @fulfil {*} - The original response from the AWS.Kinesis call.
 * @reject {Error} - The error details from AWS.Kinesis with a corrected error stack.
 * @returns {Promise}
 * @private
 */
function retriableSdkCall(client, methodName, streamName, retryOpts, ...args) {
  const stackObj = getStackObj(retriableSdkCall);
  return retry(bail => {
    try {
      return client[methodName](...args)
        .promise()
        .then(response => {
          reportResponse(statsSource, streamName);
          return response;
        })
        .catch(err => {
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
 * A class that wraps AWS.Kinesis.
 *
 * @alias module:kinesis-client
 */
class KinesisClient {
  /**
   * Initializes the AWS.Kinesis internal instance and prepares the retry logic.
   *
   * @param {Object} options - The initialization options.
   * @param {Object} options.awsOptions - The initialization options for AWS.Kinesis.
   * @param {Object} options.logger - An instace of a logger.
   * @param {string} options.streamName - The name of the Kinesis stream for which calls relate to.
   */
  constructor({ awsOptions, logger, streamName }) {
    const client = new Kinesis(awsOptions);

    const retryOpts = {
      maxTimeout: 5 * 60 * 1000,
      minTimeout: 1000,
      onRetry: err => {
        const { code, message, requestId, statusCode } = err;
        logger.warn(
          `Trying to recover from AWS.Kinesis error…\n${[
            `\t- Message: ${message}`,
            `\t- Request ID: ${requestId}`,
            `\t- Code: ${code} (${statusCode})`,
            `\t- Stream: ${streamName}`
          ].join('\n')}`
        );
      },
      randomize: true,
      retries: 100000
    };

    Object.assign(internal(this), { client, retryOpts, streamName });
  }

  /**
   * Adds or updates tags for the specified Kinesis data stream.
   *
   * @param {...*} args - The arguments.
   * @returns {Promise}
   */
  addTagsToStream(...args) {
    const { client, streamName } = internal(this);
    return sdkCall(client, 'addTagsToStream', streamName, ...args);
  }

  /**
   * Creates a Kinesis data stream.
   *
   * @param {...*} args - The arguments.
   * @returns {Promise}
   */
  createStream(...args) {
    const { client, streamName } = internal(this);
    return sdkCall(client, 'createStream', streamName, ...args).catch(err => {
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
    const { client, streamName } = internal(this);
    return sdkCall(client, 'deregisterStreamConsumer', streamName, ...args);
  }

  /**
   * Describes the specified Kinesis data stream.
   *
   * @param {...*} args - The arguments.
   * @returns {Promise}
   */
  describeStream(...args) {
    const { client, retryOpts, streamName } = internal(this);
    return retriableSdkCall(client, 'describeStream', streamName, retryOpts, ...args);
  }

  /**
   * Gets data records from a Kinesis data stream's shard.
   *
   * @param {...*} args - The arguments.
   * @returns {Promise}
   */
  getRecords(...args) {
    const { client, retryOpts, streamName } = internal(this);
    return retriableSdkCall(client, 'getRecords', streamName, retryOpts, ...args);
  }

  /**
   * Gets an Amazon Kinesis shard iterator.
   *
   * @param {...*} args - The arguments.
   * @returns {Promise}
   */
  getShardIterator(...args) {
    const { client, retryOpts, streamName } = internal(this);
    return retriableSdkCall(client, 'getShardIterator', streamName, retryOpts, ...args);
  }

  /**
   * Tells whether the endpoint of the client is local or not.
   *
   * @returns {boolean} `true` if the endpoints is local, `false` otherwise.
   */
  isEndpointLocal() {
    const { client } = internal(this);
    const { host } = client.endpoint;
    return host.includes('localhost') || host.includes('localstack');
  }

  /**
   * Lists the shards in a stream and provides information about each shard.
   *
   * @param {...*} args - The arguments.
   * @returns {Promise}
   */
  listShards(...args) {
    const { client, retryOpts, streamName } = internal(this);
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
    const { client, retryOpts, streamName } = internal(this);
    return retriableSdkCall(client, 'listStreamConsumers', streamName, retryOpts, ...args);
  }

  /**
   * Lists the tags for the specified Kinesis data stream.
   *
   * @param {...*} args - The arguments.
   * @returns {Promise}
   */
  listTagsForStream(...args) {
    const { client, retryOpts, streamName } = internal(this);
    return retriableSdkCall(client, 'listTagsForStream', streamName, retryOpts, ...args);
  }

  /**
   * Writes a single data record into an Amazon Kinesis data stream.
   *
   * @param {...*} args - The arguments.
   * @returns {Promise}
   */
  putRecord(...args) {
    const { client, retryOpts, streamName } = internal(this);
    const stackObj = getStackObj(retriableSdkCall);
    return retry(bail => {
      try {
        return client
          .putRecord(...args)
          .promise()
          .then(result => {
            reportResponse(statsSource, streamName);
            reportRecordSent(streamName);
            return result;
          })
          .catch(err => {
            const error = transformErrorStack(err, stackObj);
            reportError(statsSource, error, streamName);
            if (RETRIABLE_PUT_ERRORS.includes(err.code)) throw error;
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
    const { client, retryOpts, streamName } = internal(this);
    const stackObj = getStackObj(retriableSdkCall);
    const [firstArg, ...restOfArgs] = args;
    let records = firstArg.Records;
    const results = [];
    return retry(bail => {
      try {
        return client
          .putRecords({ ...firstArg, Records: records }, ...restOfArgs)
          .promise()
          .then(payload => {
            const { EncryptionType, Records } = payload;
            const failedCount = payload.FailedRecordCount;
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
          .catch(err => {
            const error = transformErrorStack(err, stackObj);
            reportError(statsSource, error, streamName);
            if (RETRIABLE_PUT_ERRORS.includes(err.code)) throw error;
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
    const { client, streamName } = internal(this);
    return sdkCall(client, 'registerStreamConsumer', streamName, ...args);
  }

  /**
   * Enables or updates server-side encryption using an AWS KMS key for a specified stream.
   *
   * @param {...*} args - The arguments.
   * @returns {Promise}
   */
  startStreamEncryption(...args) {
    const { client, streamName } = internal(this);
    return sdkCall(client, 'startStreamEncryption', streamName, ...args).catch(err => {
      const { code } = err;
      if (code !== 'UnknownOperationException' && code !== 'ResourceInUseException') throw err;
    });
  }

  /**
   * Waits for a given Kinesis resource.
   *
   * @param {...*} args - The arguments.
   * @returns {Promise}
   */
  waitFor(...args) {
    const { client, retryOpts, streamName } = internal(this);
    return retriableSdkCall(client, 'waitFor', streamName, retryOpts, ...args);
  }
}

/**
 * @external AsyncRetry
 * @see https://github.com/zeit/async-retry#api
 */

module.exports = KinesisClient;
