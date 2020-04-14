/**
 * Module that wraps the calls to the AWS.S3 library. Calls are wrapped so they can be
 * retried with a custom logic instead of the one provided by the AWS-SDK. In addition to retries,
 * calls are also promisified and the call stacks are preserved even in async/await calls by using
 * the `CAPTURE_STACK_TRACE` environment variable.
 *
 * @module s3-client
 * @private
 */

'use strict';

const retry = require('async-retry');
const { S3 } = require('aws-sdk');

const { reportError, reportResponse } = require('./stats');
const { getStackObj, shouldBailRetry, transformErrorStack } = require('./utils');

const privateData = new WeakMap();
const statsSource = 's3';

/**
 * Provides access to the private data of the specified instance.
 *
 * @param {object} instance - The private data's owner.
 * @returns {object} The private data.
 * @private
 */
function internal(instance) {
  if (!privateData.has(instance)) privateData.set(instance, {});
  return privateData.get(instance);
}

/**
 * Calls a method on the given instance of AWS.S3. The call is promisified, the call stack
 * is preserved, and the results of the call are aggregated in the stats. Retries in this function
 * are the original ones provided by the AWS-SDK.
 *
 * @param {object} client - An instance of AWS.S3.
 * @param {string} methodName - The name of the method to call.
 * @param {...*} args - The arguments of the method call.
 * @fulfil {*} - The original response from the AWS.S3 call.
 * @reject {Error} - The error details from AWS.S3 with a corrected error stack.
 * @returns {Promise}
 * @private
 *
 */
async function sdkCall(client, methodName, ...args) {
  const stackObj = getStackObj(sdkCall);
  try {
    return client[methodName](...args)
      .promise()
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
 * Calls a method on the given instance of AWS.S3. The call is promisified, the call stack
 * is preserved, and the results of the call are aggregated in the stats. Retries in this function
 * are based on a custom logic replacing the one provided by the AWS-SDK.
 *
 * @param {object} client - An instance of AWS.S3.
 * @param {string} methodName - The name of the method to call.
 * @param {object} retryOpts - The [retry options as in async-retry]{@link external:AsyncRetry}.
 * @param {...*} args - The argument of the method call.
 * @fulfil {*} - The original response from the AWS.S3 call.
 * @reject {Error} - The error details from AWS.S3 with a corrected error stack.
 * @returns {Promise}
 * @private
 */
function retriableSdkCall(client, methodName, retryOpts, ...args) {
  const stackObj = getStackObj(retriableSdkCall);
  return retry((bail) => {
    try {
      return client[methodName](...args)
        .promise()
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
 * A class that wraps AWS.S3.
 *
 * @alias module:s3-client
 */
class S3Client {
  /**
   * Initializes the AWS.S3 internal instance and prepares the retry logic.
   *
   * @param {object} options - The initialization options.
   * @param {string} options.bucketName - The name of the S3 bucket.
   * @param {string} options.endpoint - The endpoint S3 service.
   * @param {number} [options.largeItemThreshold=900] - The size in KB above which an item
   *        should automatically be stored in s3.
   * @param {object} options.logger - An instace of a logger.
   * @param {Array<string>} [options.nonS3Keys=[]] - If the `useS3ForLargeItems` option is set to
   *        `true`, the `nonS3Keys` option lists the keys that will be sent normally on the kinesis record.
   * @param {string} [options.tags] - The tags that should be present in the s3 bucket.
   */
  constructor({ bucketName, largeItemThreshold, logger, nonS3Keys, tags, ...awsOptions }) {
    const client = new S3(awsOptions);

    const retryOpts = {
      maxTimeout: 5 * 60 * 1000,
      minTimeout: 1000,
      onRetry: (err) => {
        const { code, message, requestId, statusCode } = err;
        logger.warn(
          `Trying to recover from AWS.S3 error…\n${[
            `\t- Message: ${message}`,
            `\t- Request ID: ${requestId}`,
            `\t- Code: ${code} (${statusCode})`,
            `\t- bucket: ${bucketName}`
          ].join('\n')}`
        );
      },
      randomize: true,
      retries: 100000
    };

    Object.assign(internal(this), {
      bucketName,
      client,
      largeItemThreshold,
      nonS3Keys,
      retryOpts,
      tags
    });
  }

  /**
   * Creates a bucket.
   *
   * @param {...*} args - The arguments.
   * @returns {Promise}
   */
  createBucket(...args) {
    const { client } = internal(this);
    return sdkCall(client, 'createBucket', ...args);
  }

  /**
   * Sets the lifecycle configuration for a given bucket.
   *
   * @param {...*} args - The arguments.
   * @returns {Promise}
   */
  getBucketLifecycleConfiguration(...args) {
    const { client, retryOpts } = internal(this);
    return retriableSdkCall(client, 'getBucketLifecycleConfiguration', retryOpts, ...args).catch(
      (err) => {
        if (err.code === 'NoSuchLifecycleConfiguration') return { Rules: [] };
        throw err;
      }
    );
  }

  /**
   * Gets the tags for a given bucket.
   *
   * @param {...*} args - The arguments.
   * @returns {Promise}
   */
  getBucketTagging(...args) {
    const { client, retryOpts } = internal(this);
    return retriableSdkCall(client, 'getBucketTagging', retryOpts, ...args).catch((err) => {
      if (err.code === 'NoSuchTagSet') return { TagSet: [] };
      throw err;
    });
  }

  /**
   * Retrieves objects from Amazon S3.
   *
   * @param {...*} args - The arguments.
   * @returns {Promise}
   */
  getObject(...args) {
    const { client, retryOpts } = internal(this);
    return retriableSdkCall(client, 'getObject', retryOpts, ...args);
  }

  /**
   * This operation is useful to determine if a bucket exists and you have permission to access it.
   *
   * @param {...*} args - The arguments.
   * @returns {Promise}
   */
  headBucket(...args) {
    const { client, retryOpts } = internal(this);
    return retriableSdkCall(client, 'headBucket', retryOpts, ...args);
  }

  /**
   * Sets the lifecycle configuration for a given bucket.
   *
   * @param {...*} args - The arguments.
   * @returns {Promise}
   */
  putBucketLifecycleConfiguration(...args) {
    const { client, retryOpts } = internal(this);
    return retriableSdkCall(client, 'putBucketLifecycleConfiguration', retryOpts, ...args);
  }

  /**
   * Sets the tags for a given bucket.
   *
   * @param {...*} args - The arguments.
   * @returns {Promise}
   */
  putBucketTagging(...args) {
    const { client, retryOpts } = internal(this);
    return retriableSdkCall(client, 'putBucketTagging', retryOpts, ...args);
  }

  /**
   * Adds an object to a bucket.
   *
   * @param {...*} args - The arguments.
   * @returns {Promise}
   */
  putObject(...args) {
    const { client, retryOpts } = internal(this);
    return retriableSdkCall(client, 'putObject', retryOpts, ...args);
  }
}

/**
 * @external AsyncRetry
 * @see https://github.com/zeit/async-retry#api
 */

module.exports = S3Client;
