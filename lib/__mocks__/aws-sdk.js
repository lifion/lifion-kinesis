'use strict';

function createResponseMock(data = {}) {
  return jest.fn(() => {
    return { promise: () => Promise.resolve(data) };
  });
}

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
    endpoint: { host: endpoint || '' },
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

function mockClear() {
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
}

module.exports = {
  DynamoDB,
  Kinesis,
  mockClear
};
