'use strict';

//
// const mockData = {};
//
// function resetMockData() {
//   mockData.Consumers = [];
//   mockData.Streams = [];
// }
//
// const addTagsToStream = jest.fn(() => ({ promise: () => Promise.resolve() }));
//
// const createStream = jest.fn(params => {
//   const { StreamName } = params;
//   const Stream = {
//     StreamARN: [
//       'arn:aws:kinesis:us-east-1',
//       Math.floor(Math.random() * 1e12),
//       `stream/${StreamName}`
//     ].join(':'),
//     StreamName,
//     StreamStatus: 'CREATING'
//   };
//   mockData.Streams.push(Stream);
//   return { promise: () => Promise.resolve({}) };
// });
//
// const describeStream = jest.fn(params => {
//   const { StreamName } = params;
//   const StreamDescription = mockData.Streams.find(i => i.StreamName === StreamName);
//   if (!StreamDescription) {
//     const err = new Error("The stream doesn't exists.");
//     err.code = 'ResourceNotFoundException';
//     return { promise: () => Promise.reject(err) };
//   }
//   return { promise: () => Promise.resolve({ StreamDescription }) };
// });
//
// const listStreamConsumers = jest.fn(() => {
//   const { Consumers } = mockData;
//   return { promise: () => Promise.resolve({ Consumers }) };
// });
//
// const listTagsForStream = jest.fn(params => {
//   const { StreamName } = params;
//   const { Tags = [] } = mockData.Streams.find(i => i.StreamName === StreamName);
//   return { promise: () => Promise.resolve({ Tags }) };
// });
//
// const registerStreamConsumer = jest.fn(params => {
//   const { ConsumerName } = params;
//   const Consumer = {
//     ConsumerARN: [
//       'arn:aws:kinesis:us-east-1',
//       Math.floor(Math.random() * 1e12),
//       `stream/test/consumer/${ConsumerName.toLowerCase()}`,
//       Math.floor(Math.random() * 1e12)
//     ].join(':'),
//     ConsumerName,
//     ConsumerStatus: 'ACTIVE'
//   };
//   mockData.Consumers.push(Consumer);
//   return {
//     promise: () => Promise.resolve({ Consumer: { ...Consumer, ConsumerStatus: 'CREATING' } })
//   };
// });
//
// const startStreamEncryption = jest.fn(() => ({ promise: () => Promise.resolve({}) }));
//
// const waitFor = jest.fn((state, { StreamName }) => {
//   const StreamDescription = mockData.Streams.find(i => i.StreamName === StreamName);
//   return { promise: () => Promise.resolve({ StreamDescription }) };
// });
//
// const Kinesis = jest.fn(() => ({
//   addTagsToStream,
//   createStream,
//   describeStream,
//   listStreamConsumers,
//   listTagsForStream,
//   registerStreamConsumer,
//   startStreamEncryption,
//   waitFor
// }));
//
//

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

//
// function mockClear() {
//   addTagsToStream.mockClear();
//   createStream.mockClear();
//   describeStream.mockClear();
//   listStreamConsumers.mockClear();
//   listTagsForStream.mockClear();
//   registerStreamConsumer.mockClear();
//   startStreamEncryption.mockClear();
//   waitFor.mockClear();
//   Kinesis.mockClear();
//   resetMockData();
// createTable.mockClear();
// DynamoDB.DocumentClient.mockClear();
// DynamoDB.mockClear();
// }
//
// function mockConsumers() {
//   return mockData.Consumers;
// }
//
// function mockStreams() {
//   return mockData.Streams;
// }
//
// resetMockData();
//
module.exports = {
  DynamoDB
  //   Kinesis,
  // mockClear
  //   mockConsumers,
  //   mockStreams
};
