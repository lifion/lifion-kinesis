'use strict';

const createTable = jest.fn(() => {
  return { promise: () => Promise.resolve({}) };
});

const deleteMock = jest.fn(() => {
  return { promise: () => Promise.resolve({}) };
});

const describeTable = jest.fn(() => {
  return { promise: () => Promise.resolve({}) };
});

const get = jest.fn(() => {
  return { promise: () => Promise.resolve({}) };
});

const listTagsOfResource = jest.fn(() => {
  return { promise: () => Promise.resolve({}) };
});

const put = jest.fn(() => {
  return { promise: () => Promise.resolve({}) };
});

const tagResource = jest.fn(() => {
  return { promise: () => Promise.resolve({}) };
});

const update = jest.fn(() => {
  return { promise: () => Promise.resolve({}) };
});

const waitFor = jest.fn(() => {
  return { promise: () => Promise.resolve({}) };
});

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
