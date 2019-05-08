/* eslint-disable global-require */

'use strict';

jest.mock('./stats');
jest.useFakeTimers();

describe('lib/dynamodb-client', () => {
  const logger = { warn: jest.fn() };
  let DynamoDbClient;
  let awsSdk;
  let client;
  let error;
  let sdkClient;
  let stats;

  function recreateClients(isDocClient) {
    awsSdk = require('aws-sdk');
    DynamoDbClient = require('./dynamodb-client');
    client = new DynamoDbClient({ logger });
    sdkClient = isDocClient ? new awsSdk.DynamoDB.DocumentClient() : new awsSdk.DynamoDB();
  }

  function throwErrorImplementation() {
    throw error;
  }

  function rejectedPromiseImplementation() {
    return { promise: () => Promise.reject(error) };
  }

  beforeEach(() => {
    delete process.env.CAPTURE_STACK_TRACE;
    DynamoDbClient = require('./dynamodb-client');
    awsSdk = require('aws-sdk');
    stats = require('./stats');
  });

  afterEach(() => {
    jest.resetModules();
    logger.warn.mockClear();
  });

  test('the module exports the expected', () => {
    expect(DynamoDbClient).toEqual(expect.any(Function));
    expect(DynamoDbClient).toThrow('Class constructor');
    expect(Object.getOwnPropertyNames(DynamoDbClient.prototype)).toEqual([
      'constructor',
      'createTable',
      'describeTable',
      'listTagsOfResource',
      'tagResource',
      'waitFor',
      'delete',
      'get',
      'put',
      'update'
    ]);
  });

  test('new instances of the module wrap instances of AWS.DynamoDB', () => {
    const awsOptions = { foo: 'bar' };
    client = new DynamoDbClient({ awsOptions });
    expect(client).toBeDefined();
    expect(awsSdk.DynamoDB).toHaveBeenCalledWith(awsOptions);
  });

  describe.each`
    methodName              | isDocClient | isRetriable
    ${'createTable'}        | ${false}    | ${false}
    ${'delete'}             | ${true}     | ${false}
    ${'describeTable'}      | ${false}    | ${true}
    ${'get'}                | ${true}     | ${true}
    ${'listTagsOfResource'} | ${false}    | ${true}
    ${'put'}                | ${true}     | ${true}
    ${'tagResource'}        | ${false}    | ${false}
    ${'update'}             | ${true}     | ${true}
    ${'waitFor'}            | ${false}    | ${true}
  `('$methodName', ({ isDocClient, isRetriable, methodName }) => {
    beforeEach(() => {
      recreateClients(isDocClient);
    });

    test(`${methodName} calls the wrapped AWS SDK method`, async () => {
      const params = { foo: 'bar' };
      await client[methodName](params);
      expect(sdkClient[methodName]).toHaveBeenCalledWith(params);
      expect(stats.reportResponse).toHaveBeenCalledWith('dynamoDb');
    });

    test(`${methodName} throws exceptions from the wrapped SDK call`, async () => {
      error = Object.assign(new Error('foo'), { code: 'MissingRequiredParameter' });
      sdkClient[methodName].mockImplementationOnce(throwErrorImplementation);
      await expect(client[methodName]({})).rejects.toThrow(error);
      expect(stats.reportError).toHaveBeenCalledWith('dynamoDb', error);
      expect(logger.warn).not.toHaveBeenCalled();
    });

    test(`${methodName} throws exceptions from the wrapped SDK promise`, async () => {
      error = Object.assign(new Error('foo'), { code: 'MissingRequiredParameter' });
      sdkClient[methodName].mockImplementationOnce(rejectedPromiseImplementation);
      await expect(client[methodName]({})).rejects.toThrow(error);
      expect(stats.reportError).toHaveBeenCalledWith('dynamoDb', error);
      expect(logger.warn).not.toHaveBeenCalled();
    });

    test(`${methodName} throws exceptions with a debuggable stack trace`, async () => {
      error = Object.assign(new Error('foo'), { code: 'MissingRequiredParameter' });
      sdkClient[methodName].mockImplementationOnce(rejectedPromiseImplementation);
      const stackBefore = await client[methodName]({}).catch(err => err.stack);
      jest.resetModules();
      process.env.CAPTURE_STACK_TRACE = true;
      recreateClients(isDocClient);
      sdkClient[methodName].mockImplementationOnce(rejectedPromiseImplementation);
      const stackAfter = await client[methodName]({}).catch(err => err.stack);
      expect(stackBefore).not.toEqual(stackAfter);
    });

    if (isRetriable) {
      test('describeTable retries errors from the wrapped SDK', async () => {
        error = new Error('foo');
        sdkClient[methodName].mockImplementationOnce(rejectedPromiseImplementation);
        const promise = client[methodName]({});
        setImmediate(() => jest.runAllTimers());
        await expect(promise).resolves.toEqual({});
        expect(sdkClient[methodName]).toHaveBeenCalledTimes(2);
        expect(stats.reportError).toHaveBeenCalledWith('dynamoDb', error);
        expect(logger.warn).toHaveBeenCalled();
      });
    }
  });
});
