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

  async describeStream(params) {
    const { client } = internal(this);
    return client.describeStream(params).promise();
  }

  async listTagsForStream(params) {
    const { client } = internal(this);
    return client.listTagsForStream(params).promise();
  }

  async listShards(params) {
    const { client } = internal(this);
    return client.listShards(params).promise();
  }
}

module.exports = KinesisProxy;
