'use strict';

const mockData = {};

function resetMockData() {
  mockData.Consumers = [];
  mockData.Streams = [];
}

const addTagsToStream = jest.fn(() => ({ promise: () => Promise.resolve() }));

const createStream = jest.fn(params => {
  const { StreamName } = params;
  const Stream = {
    StreamName,
    StreamStatus: 'CREATING',
    StreamARN: [
      'arn:aws:kinesis:us-east-1',
      Math.floor(Math.random() * 1e12),
      `stream/${StreamName}`
    ].join(':')
  };
  mockData.Streams.push(Stream);
  return { promise: () => Promise.resolve({}) };
});

const describeStream = jest.fn(params => {
  const { StreamName } = params;
  const StreamDescription = mockData.Streams.find(i => i.StreamName === StreamName);
  if (!StreamDescription) {
    const err = new Error("The stream doesn't exists.");
    err.code = 'ResourceNotFoundException';
    return { promise: () => Promise.reject(err) };
  }
  return { promise: () => Promise.resolve({ StreamDescription }) };
});

const listStreamConsumers = jest.fn(() => {
  const { Consumers } = mockData;
  return { promise: () => Promise.resolve({ Consumers }) };
});

const listTagsForStream = jest.fn(params => {
  const { StreamName } = params;
  const { Tags = [] } = mockData.Streams.find(i => i.StreamName === StreamName);
  return { promise: () => Promise.resolve({ Tags }) };
});

const registerStreamConsumer = jest.fn(params => {
  const { ConsumerName } = params;
  const Consumer = {
    ConsumerARN: [
      'arn:aws:kinesis:us-east-1',
      Math.floor(Math.random() * 1e12),
      `stream/test/consumer/${ConsumerName.toLowerCase()}`,
      Math.floor(Math.random() * 1e12)
    ].join(':'),
    ConsumerName,
    ConsumerStatus: 'ACTIVE'
  };
  mockData.Consumers.push(Consumer);
  return {
    promise: () => Promise.resolve({ Consumer: { ...Consumer, ConsumerStatus: 'CREATING' } })
  };
});

const startStreamEncryption = jest.fn(() => ({ promise: () => Promise.resolve({}) }));

const waitFor = jest.fn((state, { StreamName }) => {
  const StreamDescription = mockData.Streams.find(i => i.StreamName === StreamName);
  return { promise: () => Promise.resolve({ StreamDescription }) };
});

const Kinesis = jest.fn(() => ({
  addTagsToStream,
  createStream,
  describeStream,
  listStreamConsumers,
  listTagsForStream,
  registerStreamConsumer,
  startStreamEncryption,
  waitFor
}));

function mockClear() {
  addTagsToStream.mockClear();
  createStream.mockClear();
  describeStream.mockClear();
  listStreamConsumers.mockClear();
  listTagsForStream.mockClear();
  registerStreamConsumer.mockClear();
  startStreamEncryption.mockClear();
  waitFor.mockClear();
  Kinesis.mockClear();
  resetMockData();
}

function mockConsumers() {
  return mockData.Consumers;
}

function mockStreams() {
  return mockData.Streams;
}

resetMockData();

module.exports = {
  Kinesis,
  mockClear,
  mockConsumers,
  mockStreams
};
