import { vi } from 'vitest';

function createResponseMock(data = {}) {
  return vi.fn(() => Promise.resolve(data));
}

const credentials = vi.fn(() =>
  Promise.resolve({
    accessKeyId: 'resolved-access-key-id',
    secretAccessKey: 'resolved-secret-access-key',
    sessionToken: 'resolved-session-token'
  })
);

const addTagsToStream = createResponseMock();
const createStream = createResponseMock();
const deregisterStreamConsumer = createResponseMock();
const describeStream = createResponseMock();
const describeStreamSummary = createResponseMock();
const getRecords = createResponseMock();
const getShardIterator = createResponseMock();
const listShards = createResponseMock();
const listStreamConsumers = createResponseMock();
const listTagsForStream = createResponseMock();
const putRecord = createResponseMock();
const putRecords = createResponseMock({ FailedRecordCount: 0, Records: [] });
const registerStreamConsumer = createResponseMock();
const startStreamEncryption = createResponseMock();

const Kinesis = vi.fn(function () {
  return {
    addTagsToStream,
    config: { credentials },
    createStream,
    deregisterStreamConsumer,
    describeStream,
    describeStreamSummary,
    getRecords,
    getShardIterator,
    listShards,
    listStreamConsumers,
    listTagsForStream,
    putRecord,
    putRecords,
    registerStreamConsumer,
    startStreamEncryption
  };
});

const waitUntilStreamExists = vi.fn(() => Promise.resolve({ reason: {}, state: 'SUCCESS' }));
const waitUntilStreamNotExists = vi.fn(() => Promise.resolve({ reason: {}, state: 'SUCCESS' }));

function mockClear() {
  credentials.mockClear();
  addTagsToStream.mockClear();
  createStream.mockClear();
  deregisterStreamConsumer.mockClear();
  describeStream.mockClear();
  describeStreamSummary.mockClear();
  getRecords.mockClear();
  getShardIterator.mockClear();
  listShards.mockClear();
  listStreamConsumers.mockClear();
  listTagsForStream.mockClear();
  putRecord.mockClear();
  putRecords.mockClear();
  registerStreamConsumer.mockClear();
  startStreamEncryption.mockClear();
  Kinesis.mockClear();
  waitUntilStreamExists.mockClear();
  waitUntilStreamNotExists.mockClear();
}

export { Kinesis, mockClear, waitUntilStreamExists, waitUntilStreamNotExists };
