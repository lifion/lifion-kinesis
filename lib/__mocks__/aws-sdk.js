'use strict';

function createResponseMock(data = {}) {
  return jest.fn(() => {
    return { promise: () => Promise.resolve(data) };
  });
}

const resolvePromise = jest.fn(() =>
  Promise.resolve({
    accessKeyId: 'resolved-access-key-id',
    secretAccessKey: 'resolved-secret-access-key',
    sessionToken: 'resolved-session-token'
  })
);

const CredentialProviderChain = jest.fn(() => ({ resolvePromise }));

const createTable = createResponseMock();
const deleteMock = createResponseMock();
const describeTable = createResponseMock();
const get = createResponseMock();
const listTagsOfResource = createResponseMock();
const put = createResponseMock();
const tagResource = createResponseMock();
const update = createResponseMock();
const waitFor = createResponseMock();

const DynamoDB = jest.fn(() => ({
  createTable,
  describeTable,
  listTagsOfResource,
  tagResource,
  waitFor
}));

DynamoDB.DocumentClient = jest.fn(() => ({
  delete: deleteMock,
  get,
  put,
  update
}));

const addTagsToStream = createResponseMock();
const createStream = createResponseMock();
const deregisterStreamConsumer = createResponseMock();
const describeStream = createResponseMock();
const describeStreamSummary = createResponseMock();
const getRecords = createResponseMock();
const getShardIterator = createResponseMock();
const isEndpointLocal = createResponseMock();
const listShards = createResponseMock();
const listStreamConsumers = createResponseMock();
const listTagsForStream = createResponseMock();
const putRecord = createResponseMock();
const putRecords = createResponseMock({ FailedRecordCount: 0, Records: [] });
const registerStreamConsumer = createResponseMock();
const startStreamEncryption = createResponseMock();

const Kinesis = jest.fn(({ endpoint } = {}) => {
  return {
    addTagsToStream,
    createStream,
    deregisterStreamConsumer,
    describeStream,
    describeStreamSummary,
    endpoint: new URL(endpoint || 'https://kinesis.amazonaws.com/'),
    getRecords,
    getShardIterator,
    isEndpointLocal,
    listShards,
    listStreamConsumers,
    listTagsForStream,
    putRecord,
    putRecords,
    registerStreamConsumer,
    startStreamEncryption,
    waitFor
  };
});

const constructor = createResponseMock();
const createBucket = createResponseMock();
const getBucketLifecycleConfiguration = createResponseMock();
const getBucketTagging = createResponseMock();
const getObject = createResponseMock();
const headBucket = createResponseMock();
const putBucketLifecycleConfiguration = createResponseMock();
const putBucketTagging = createResponseMock();
const putObject = createResponseMock();

const S3 = jest.fn(() => ({
  constructor,
  createBucket,
  getBucketLifecycleConfiguration,
  getBucketTagging,
  getObject,
  headBucket,
  putBucketLifecycleConfiguration,
  putBucketTagging,
  putObject
}));

function mockClear() {
  resolvePromise.mockClear();
  CredentialProviderChain.mockClear();

  createTable.mockClear();
  deleteMock.mockClear();
  describeTable.mockClear();
  get.mockClear();
  listTagsOfResource.mockClear();
  put.mockClear();
  tagResource.mockClear();
  update.mockClear();
  waitFor.mockClear();
  DynamoDB.mockClear();
  DynamoDB.DocumentClient.mockClear();

  addTagsToStream.mockClear();
  createStream.mockClear();
  deregisterStreamConsumer.mockClear();
  describeStream.mockClear();
  describeStreamSummary.mockClear();
  getRecords.mockClear();
  getShardIterator.mockClear();
  isEndpointLocal.mockClear();
  listShards.mockClear();
  listStreamConsumers.mockClear();
  listTagsForStream.mockClear();
  putRecord.mockClear();
  putRecords.mockClear();
  registerStreamConsumer.mockClear();
  startStreamEncryption.mockClear();
  Kinesis.mockClear();

  createBucket.mockClear();
  getBucketLifecycleConfiguration.mockClear();
  getBucketTagging.mockClear();
  getObject.mockClear();
  headBucket.mockClear();
  putBucketLifecycleConfiguration.mockClear();
  putBucketTagging.mockClear();
  putObject.mockClear();
  S3.mockClear();
}

module.exports = {
  CredentialProviderChain,
  DynamoDB,
  Kinesis,
  S3,
  mockClear
};
