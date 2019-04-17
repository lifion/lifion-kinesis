'use strict';

const { Kinesis } = require('aws-sdk');

const privateData = new WeakMap();

function internal(instance) {
  if (!privateData.has(instance)) privateData.set(instance, {});
  return privateData.get(instance);
}

class KinesisProxy {
  constructor(options) {
    internal(this).client = new Kinesis(options);
  }

  createStream(...args) {
    return internal(this)
      .client.createStream(...args)
      .promise()
      .catch(err => {
        const error = new Error(err.message);
        error.code = err.code;
        throw error;
      });
  }

  addTagsToStream(...args) {
    return internal(this)
      .client.addTagsToStream(...args)
      .promise()
      .catch(err => {
        const error = new Error(err.message);
        error.code = err.code;
        throw error;
      });
  }

  describeStream(...args) {
    return internal(this)
      .client.describeStream(...args)
      .promise()
      .catch(err => {
        const error = new Error(err.message);
        error.code = err.code;
        throw error;
      });
  }

  listShards(...args) {
    return internal(this)
      .client.listShards(...args)
      .promise()
      .catch(err => {
        const error = new Error(err.message);
        error.code = err.code;
        throw error;
      });
  }

  listTagsForStream(...args) {
    return internal(this)
      .client.listTagsForStream(...args)
      .promise()
      .catch(err => {
        const error = new Error(err.message);
        error.code = err.code;
        throw error;
      });
  }

  startStreamEncryption(...args) {
    return internal(this)
      .client.startStreamEncryption(...args)
      .promise()
      .catch(err => {
        const { code, message } = err;
        if (code !== 'UnknownOperationException') {
          const error = new Error(message);
          error.code = code;
          throw error;
        }
      });
  }

  waitFor(...args) {
    return internal(this)
      .client.waitFor(...args)
      .promise()
      .catch(err => {
        const error = new Error(err.message);
        error.code = err.code;
        throw error;
      });
  }

  getShardIterator(...args) {
    return internal(this)
      .client.getShardIterator(...args)
      .promise()
      .catch(err => {
        const error = new Error(err.message);
        error.code = err.code;
        throw error;
      });
  }

  getRecords(...args) {
    return internal(this)
      .client.getRecords(...args)
      .promise()
      .catch(err => {
        const error = new Error(err.message);
        error.code = err.code;
        throw error;
      });
  }

  putRecord(...args) {
    return internal(this)
      .client.putRecord(...args)
      .promise()
      .catch(err => {
        const error = new Error(err.message);
        error.code = err.code;
        throw error;
      });
  }

  putRecords(...args) {
    return internal(this)
      .client.putRecords(...args)
      .promise()
      .catch(err => {
        const error = new Error(err.message);
        error.code = err.code;
        throw error;
      });
  }
}

module.exports = KinesisProxy;
