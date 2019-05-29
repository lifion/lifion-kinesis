'use strict';

function createResponseMock() {
  return jest.fn(() => {
    return { promise: () => Promise.resolve({}) };
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
}

module.exports = {
  DynamoDB,
  mockClear
};
