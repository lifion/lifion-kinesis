'use strict';

const retry = require('async-retry');
const { Kinesis } = require('aws-sdk');

const { CAPTURE_STACK_TRACE } = require('./constants');
const { reportError, reportRecordSent, reportResponse } = require('./stats');
const { shouldBailRetry, transformErrorStack } = require('./utils');

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

function internal(instance) {
  if (!privateData.has(instance)) privateData.set(instance, {});
  return privateData.get(instance);
}

function sdkCall(client, methodName, streamName, ...args) {
  let stackObj;
  if (CAPTURE_STACK_TRACE) {
    stackObj = {};
    Error.captureStackTrace(stackObj, sdkCall);
  }
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
    reportError(statsSource, error);
    throw error;
  }
}

function retriableSdkCall(client, methodName, streamName, retryOpts, ...args) {
  let stackObj;
  if (CAPTURE_STACK_TRACE) {
    stackObj = {};
    Error.captureStackTrace(stackObj, retriableSdkCall);
  }
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

class KinesisClient {
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
            `\t- Code: ${code} (${statusCode})`
          ].join('\n')}`
        );
      },
      randomize: true,
      retries: 100000
    };

    Object.assign(internal(this), { client, retryOpts, streamName });
  }

  createStream(...args) {
    const { client, streamName } = internal(this);
    return sdkCall(client, 'streamName', streamName, ...args).catch(err => {
      if (err.code !== 'ResourceInUseException') throw err;
    });
  }

  addTagsToStream(...args) {
    const { client, streamName } = internal(this);
    return sdkCall(client, 'addTagsToStream', streamName, ...args);
  }

  describeStream(...args) {
    const { client, retryOpts, streamName } = internal(this);
    return retriableSdkCall(client, 'describeStream', streamName, retryOpts, ...args);
  }

  listShards(...args) {
    const { client, retryOpts, streamName } = internal(this);
    return retriableSdkCall(client, 'listShards', streamName, retryOpts, ...args);
  }

  listStreamConsumers(...args) {
    const { client, retryOpts, streamName } = internal(this);
    return retriableSdkCall(client, 'listStreamConsumers', streamName, retryOpts, ...args);
  }

  listTagsForStream(...args) {
    const { client, retryOpts, streamName } = internal(this);
    return retriableSdkCall(client, 'listTagsForStream', streamName, retryOpts, ...args);
  }

  startStreamEncryption(...args) {
    const { client, streamName } = internal(this);
    return sdkCall(client, 'startStreamEncryption', streamName, ...args).catch(err => {
      const { code } = err;
      if (code !== 'UnknownOperationException' && code !== 'ResourceInUseException') throw err;
    });
  }

  waitFor(...args) {
    const { client, retryOpts, streamName } = internal(this);
    return retriableSdkCall(client, 'waitFor', streamName, retryOpts, ...args);
  }

  getShardIterator(...args) {
    const { client, retryOpts, streamName } = internal(this);
    return retriableSdkCall(client, 'getShardIterator', streamName, retryOpts, ...args);
  }

  getRecords(...args) {
    const { client, retryOpts, streamName } = internal(this);
    return retriableSdkCall(client, 'getRecords', streamName, retryOpts, ...args);
  }

  deregisterStreamConsumer(...args) {
    const { client, streamName } = internal(this);
    return sdkCall(client, 'deregisterStreamConsumer', streamName, ...args);
  }

  registerStreamConsumer(...args) {
    const { client, streamName } = internal(this);
    return sdkCall(client, 'registerStreamConsumer', streamName, ...args);
  }

  putRecord(...args) {
    const { client, retryOpts, streamName } = internal(this);
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
            const error = transformErrorStack(err);
            if (RETRIABLE_PUT_ERRORS.indexOf(err.code) > -1) {
              reportError(statsSource, error, streamName);
              throw error;
            } else bail(error);
          });
      } catch (err) {
        const error = transformErrorStack(err);
        reportError(statsSource, error, streamName);
        bail(error);
        return undefined;
      }
    }, retryOpts);
  }

  async putRecords(params) {
    const { client, retryOpts, streamName } = internal(this);
    const { Records, ...opts } = params;

    let records = Records;
    let results = [];
    let failedRecordCount = 0;

    return retry(bail => {
      try {
        return client
          .putRecords({ ...opts, Records: records })
          .promise()
          .then(payload => {
            failedRecordCount = payload.FailedRecordCount;
            results = payload.Records;

            if (failedRecordCount !== records.length) {
              reportResponse(statsSource, streamName);
              reportRecordSent(streamName);
            }

            if (failedRecordCount === 0) return;

            let code;
            let message;

            records = records.filter((record, i) => {
              const { ErrorCode, ErrorMessage } = results[i];
              if (ErrorCode && ErrorCode !== 'ProvisionedThroughputExceededException') {
                code = ErrorCode;
                message = ErrorMessage;
                return false;
              }
              if (ErrorCode && !code) code = code || ErrorCode;
              if (ErrorMessage && !message) message = ErrorMessage;
              return ErrorCode;
            });

            throw Object.assign(new Error(message), { code });
          })
          .catch(err => {
            const error = transformErrorStack(err);
            if (RETRIABLE_PUT_ERRORS.indexOf(err.code) > -1) {
              reportError(statsSource, error, streamName);
              throw error;
            } else bail(error);
          });
      } catch (err) {
        const error = transformErrorStack(err);
        reportError(statsSource, error, streamName);
        bail(error);
        return undefined;
      }
    }, retryOpts);
  }
}

module.exports = KinesisClient;
