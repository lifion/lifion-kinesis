'use strict';

const { Kinesis, mockClear } = require('aws-sdk');
const { reportError, reportRecordSent, reportResponse } = require('./stats');
const KinesisClient = require('./kinesis-client');
const { getStackObj } = require('./utils');

jest.mock('./stats');

jest.mock('./utils', () => {
  const utils = jest.requireActual('./utils');
  return { ...utils, getStackObj: jest.fn((...args) => utils.getStackObj(...args)) };
});

describe('lib/kinesis-client', () => {
  const warn = jest.fn();
  const logger = { warn };

  let client;
  let error;
  let sdkClient;

  function recreateClients() {
    client = new KinesisClient({ logger, streamName: 'test-stream' });
    sdkClient = new Kinesis();
  }

  function throwErrorImplementation() {
    throw error;
  }

  function rejectedPromiseImplementation() {
    return { promise: () => Promise.reject(error) };
  }

  afterEach(() => {
    mockClear();
    reportError.mockClear();
    reportResponse.mockClear();
    warn.mockClear();
    reportRecordSent.mockClear();
  });

  test('the module exports the expected', () => {
    expect(KinesisClient).toEqual(expect.any(Function));
    expect(KinesisClient).toThrow('Class constructor');
    expect(Object.getOwnPropertyNames(KinesisClient.prototype)).toEqual([
      'constructor',
      'addTagsToStream',
      'createStream',
      'deregisterStreamConsumer',
      'describeStream',
      'getRecords',
      'getShardIterator',
      'isEndpointLocal',
      'listShards',
      'listStreamConsumers',
      'listTagsForStream',
      'putRecord',
      'putRecords',
      'registerStreamConsumer',
      'startStreamEncryption',
      'waitFor'
    ]);
  });

  test('new instances of the module wrap instances of AWS.Kinesis', () => {
    const awsOptions = { foo: 'bar' };
    client = new KinesisClient({ awsOptions });
    expect(client).toBeDefined();
    expect(Kinesis).toHaveBeenCalledWith(awsOptions);
  });

  describe.each`
    methodName                    | isRetriable
    ${'addTagsToStream'}          | ${false}
    ${'createStream'}             | ${false}
    ${'deregisterStreamConsumer'} | ${false}
    ${'describeStream'}           | ${true}
    ${'getRecords'}               | ${true}
    ${'getShardIterator'}         | ${true}
    ${'listShards'}               | ${true}
    ${'listStreamConsumers'}      | ${true}
    ${'listTagsForStream'}        | ${true}
    ${'putRecord'}                | ${true}
    ${'putRecords'}               | ${false}
    ${'registerStreamConsumer'}   | ${false}
    ${'startStreamEncryption'}    | ${false}
    ${'waitFor'}                  | ${true}
  `('$methodName', ({ isRetriable, methodName }) => {
    beforeAll(recreateClients);

    test(`${methodName} calls the wrapped AWS SDK method`, async () => {
      const params = methodName === 'putRecords' ? { Records: [] } : { foo: 'bar' };
      await client[methodName](params);
      expect(sdkClient[methodName]).toHaveBeenCalledWith(params);
      expect(reportResponse).toHaveBeenCalledWith('kinesis', 'test-stream');
    });

    test(`${methodName} throws exceptions from the wrapped SDK call`, async () => {
      error = Object.assign(new Error('foo'), { code: 'MissingRequiredParameter' });
      sdkClient[methodName].mockImplementationOnce(throwErrorImplementation);
      await expect(client[methodName]({})).rejects.toThrow(error);
      expect(reportError).toHaveBeenCalledWith('kinesis', error, 'test-stream');
      expect(warn).not.toHaveBeenCalled();
    });

    test(`${methodName} throws exceptions from the wrapped SDK promise`, async () => {
      error = Object.assign(new Error('foo'), { code: 'MissingRequiredParameter' });
      sdkClient[methodName].mockImplementationOnce(rejectedPromiseImplementation);
      await expect(client[methodName]({})).rejects.toThrow(error);
      expect(reportError).toHaveBeenCalledWith('kinesis', error, 'test-stream');
      expect(warn).not.toHaveBeenCalled();
    });

    test(`${methodName} throws exceptions with a debuggable stack trace`, async () => {
      error = Object.assign(new Error('foo'), { code: 'MissingRequiredParameter' });
      sdkClient[methodName].mockImplementationOnce(rejectedPromiseImplementation);
      const stackBefore = await client[methodName]({}).catch(err => err.stack);

      client = new KinesisClient({ logger, streamName: 'test-stream' });
      sdkClient = new Kinesis();

      sdkClient[methodName].mockImplementationOnce(rejectedPromiseImplementation);
      getStackObj.mockReturnValueOnce({ stack: '\n' });
      const stackAfter = await client[methodName]({}).catch(err => err.stack);

      expect(stackBefore).not.toEqual(stackAfter);
    });

    if (isRetriable) {
      test(`${methodName} retries errors from the wrapped SDK`, async () => {
        error = Object.assign(new Error('foo'), { code: 'ProvisionedThroughputExceededException' });
        sdkClient[methodName].mockImplementationOnce(rejectedPromiseImplementation);
        const promise = client[methodName]({});
        await expect(promise).resolves.toEqual({});
        expect(sdkClient[methodName]).toHaveBeenCalledTimes(2);
        expect(reportError).toHaveBeenCalledWith('kinesis', error, 'test-stream');
        expect(warn).toHaveBeenCalled();
      });
    }

    if (methodName === 'createStream' || methodName === 'startStreamEncryption') {
      test(`${methodName} should succeed if the stream is already getting updated`, async () => {
        error = Object.assign(new Error('foo'), { code: 'ResourceInUseException' });
        sdkClient[methodName].mockImplementationOnce(throwErrorImplementation);
        await expect(client[methodName]({})).resolves.toBeUndefined();
        expect(reportError).toHaveBeenCalledWith('kinesis', error, 'test-stream');
        expect(warn).not.toHaveBeenCalled();
      });
    }

    if (methodName === 'startStreamEncryption') {
      test(`${methodName} should succeed if the operation is not supported`, async () => {
        error = Object.assign(new Error('foo'), { code: 'UnknownOperationException' });
        sdkClient[methodName].mockImplementationOnce(throwErrorImplementation);
        await expect(client[methodName]({})).resolves.toBeUndefined();
        expect(reportError).toHaveBeenCalledWith('kinesis', error, 'test-stream');
        expect(warn).not.toHaveBeenCalled();
      });
    }

    if (methodName === 'putRecords') {
      test('putRecords should retry failed records until it succeeds', async () => {
        let mockedSeqNum = 0;
        function putHalfOfRecords(params) {
          return {
            promise: () => {
              let FailedRecordCount = 0;
              const Records = [];
              for (let i = 0; i < params.Records.length; i += 1) {
                if (i > 1) {
                  FailedRecordCount += 1;
                  Records.push({ ErrorCode: 'ProvisionedThroughputExceededException' });
                } else {
                  Records.push({ SequenceNumber: mockedSeqNum.toString() });
                  mockedSeqNum += 1;
                }
              }
              return Promise.resolve({ EncryptionType: 'foo', FailedRecordCount, Records });
            }
          };
        }
        sdkClient.putRecords.mockImplementationOnce(putHalfOfRecords);
        sdkClient.putRecords.mockImplementationOnce(putHalfOfRecords);
        await expect(
          client.putRecords({ Records: [{ Data: 'foo' }, { Data: 'bar' }, { Data: 'baz' }] })
        ).resolves.toEqual({
          EncryptionType: 'foo',
          Records: [{ SequenceNumber: '0' }, { SequenceNumber: '1' }, { SequenceNumber: '2' }]
        });
      });
    }
  });

  describe('isEndpointLocal', () => {
    test.each`
      endpoint        | expected | scenario
      ${undefined}    | ${false} | ${'no'}
      ${'localhost'}  | ${true}  | ${'a local'}
      ${'localstack'} | ${true}  | ${'a LocalStack'}
    `(
      'isEndpointLocal should return $expected with $scenario endpoint',
      ({ endpoint, expected }) => {
        client = new KinesisClient({ awsOptions: { endpoint } });
        expect(client.isEndpointLocal()).toBe(expected);
      }
    );
  });
});
