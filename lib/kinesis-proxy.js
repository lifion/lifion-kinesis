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

  async describeStream(...args) {
    return internal(this)
      .client.describeStream(...args)
      .promise()
      .catch(err => {
        const error = new Error(err.message);
        error.code = err.code;
        throw error;
      });
  }

  async listTagsForStream(...args) {
    return internal(this)
      .client.listTagsForStream(...args)
      .promise()
      .catch(err => {
        const error = new Error(err.message);
        error.code = err.code;
        throw error;
      });
  }

  async listShards(...args) {
    return internal(this)
      .client.listShards(...args)
      .promise()
      .catch(err => {
        const error = new Error(err.message);
        error.code = err.code;
        throw error;
      });
  }

  async waitFor(...args) {
    return internal(this)
      .client.waitFor(...args)
      .promise()
      .catch(err => {
        const error = new Error(err.message);
        error.code = err.code;
        throw error;
      });
  }
}

module.exports = KinesisProxy;
