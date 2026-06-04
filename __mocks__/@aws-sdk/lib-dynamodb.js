import { vi } from 'vitest';

function createResponseMock(data = {}) {
  return vi.fn(() => Promise.resolve(data));
}

const deleteMock = createResponseMock();
const get = createResponseMock();
const put = createResponseMock();
const update = createResponseMock();

const DynamoDBDocument = {
  from: vi.fn(() => ({ delete: deleteMock, get, put, update }))
};

function mockClear() {
  deleteMock.mockClear();
  get.mockClear();
  put.mockClear();
  update.mockClear();
  DynamoDBDocument.from.mockClear();
}

export { DynamoDBDocument, mockClear };
