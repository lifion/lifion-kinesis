import { vi } from 'vitest';

function createResponseMock(data = {}) {
  return vi.fn(() => Promise.resolve(data));
}

const credentials = vi.fn(() =>
  Promise.resolve({
    accessKeyId: 'resolved-access-key-id',
    secretAccessKey: 'resolved-secret-access-key'
  })
);

const createTable = createResponseMock();
const describeTable = createResponseMock();
const listTagsOfResource = createResponseMock();
const tagResource = createResponseMock();

const DynamoDB = vi.fn(function () {
  return {
    config: { credentials },
    createTable,
    describeTable,
    listTagsOfResource,
    tagResource
  };
});

const waitUntilTableExists = vi.fn(() => Promise.resolve({ reason: {}, state: 'SUCCESS' }));
const waitUntilTableNotExists = vi.fn(() => Promise.resolve({ reason: {}, state: 'SUCCESS' }));

function mockClear() {
  credentials.mockClear();
  createTable.mockClear();
  describeTable.mockClear();
  listTagsOfResource.mockClear();
  tagResource.mockClear();
  DynamoDB.mockClear();
  waitUntilTableExists.mockClear();
  waitUntilTableNotExists.mockClear();
}

export { DynamoDB, mockClear, waitUntilTableExists, waitUntilTableNotExists };
