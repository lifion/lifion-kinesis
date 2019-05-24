'use strict';

const retry = require('async-retry');
const { Kinesis } = require('aws-sdk');

const { reportError, reportRecordSent, reportResponse } = require('./stats');
const { getStackObj, shouldBailRetry, transformErrorStack } = require('./utils');

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
    reportError(statsSource, error);
    throw error;
  }
}

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

  createStream(...args) {
    const { client, streamName } = internal(this);
    return sdkCall(client, 'createStream', streamName, ...args).catch(err => {
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
            if (RETRIABLE_PUT_ERRORS.includes(err.code)) {
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
    let records = params.Records;
    const results = [];
    return retry(bail => {
      try {
        return client
          .putRecords({ ...params, Records: records })
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
            if (failedCount === 0) {
              return { EncryptionType, Records: results };
            }
            if (failedCount !== records.length) {
              reportResponse(statsSource, streamName);
              reportRecordSent(streamName);
            }
            records = nextRecords;
            const error = new Error(`Failed to write ${failedCount} of ${recordsCount} record(s).`);
            error.code = 'ProvisionedThroughputExceededException';
            throw error;
          })
          .catch(err => {
            const error = transformErrorStack(err);
            if (RETRIABLE_PUT_ERRORS.includes(err.code)) {
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

  isEndpointLocal() {
    const { client } = internal(this);
    const { host } = client.endpoint;
    return host.includes('localhost') || host.includes('localstack');
  }
}

module.exports = KinesisClient;
