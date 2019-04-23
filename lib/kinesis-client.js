'use strict';

const retry = require('async-retry');
const { Kinesis } = require('aws-sdk');

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

class KinesisClient {
  constructor({ awsOptions, logger }) {
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
    Object.assign(internal(this), { client, retryOpts });
  }

  createStream(...args) {
    return internal(this)
      .client.createStream(...args)
      .promise()
      .catch(err => {
        const { code, message } = err;
        if (code !== 'ResourceInUseException') {
          const error = new Error(message);
          error.code = code;
          throw error;
        }
      });
  }

  addTagsToStream(...args) {
    return internal(this)
      .client.addTagsToStream(...args)
      .promise()
      .catch(err => {
        const { code, message } = err;
        const error = new Error(message);
        error.code = code;
        throw error;
      });
  }

  describeStream(...args) {
    const { client, retryOpts } = internal(this);
    return retry(
      bail =>
        client
          .describeStream(...args)
          .promise()
          .catch(err => {
            const { code } = err;
            if (code === 'ResourceNotFoundException' || shouldBailRetry(err)) bail(err);
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

  listShards(...args) {
    const { client, retryOpts } = internal(this);
    return retry(
      bail =>
        client
          .listShards(...args)
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

  listStreamConsumers(...args) {
    const { client, retryOpts } = internal(this);
    return retry(
      bail =>
        client
          .listStreamConsumers(...args)
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

  listTagsForStream(...args) {
    const { client, retryOpts } = internal(this);
    return retry(
      bail =>
        client
          .listTagsForStream(...args)
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

  startStreamEncryption(...args) {
    return internal(this)
      .client.startStreamEncryption(...args)
      .promise()
      .catch(err => {
        const { code, message } = err;
        if (code !== 'UnknownOperationException' && code !== 'ResourceInUseException') {
          const error = new Error(message);
          error.code = code;
          throw error;
        }
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

  getShardIterator(...args) {
    const { client, retryOpts } = internal(this);
    return retry(
      bail =>
        client
          .getShardIterator(...args)
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

  getRecords(...args) {
    const { client, retryOpts } = internal(this);
    return retry(
      bail =>
        client
          .getRecords(...args)
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

  registerStreamConsumer(...args) {
    return internal(this)
      .client.registerStreamConsumer(...args)
      .promise()
      .catch(err => {
        const error = new Error(err.message);
        error.code = err.code;
        throw error;
      });
  }

  putRecord(...args) {
    const { client, retryOpts } = internal(this);
    return retry(
      bail =>
        client
          .putRecord(...args)
          .promise()
          .catch(err => {
            if (err.code !== 'ProvisionedThroughputExceededException') bail(err);
            else throw err;
          }),
      retryOpts
    ).catch(err => {
      const error = new Error(err.message);
      error.code = err.code;
      throw error;
    });
  }

  async putRecords(params) {
    const { client, retryOpts } = internal(this);
    const { Records, ...opts } = params;

    let records = Records;
    let results = [];
    let failedRecordCount = 0;

    return retry(
      bail =>
        client
          .putRecords({ ...opts, Records: records })
          .promise()
          .then(payload => {
            ({ FailedRecordCount: failedRecordCount, Records: results } = payload);
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

            const err = new Error(message);
            err.code = code;
            err.requestId = null;
            err.statusCode = null;
            throw err;
          })
          .catch(err => {
            if (err.code !== 'ProvisionedThroughputExceededException') bail(err);
            else throw err;
          }),
      retryOpts
    ).catch(err => {
      const error = new Error(err.message);
      error.code = err.code;
      throw error;
    });
  }
}

module.exports = KinesisClient;
