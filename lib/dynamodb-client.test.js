import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest';
import {
  DynamoDB,
  mockClear,
  waitUntilTableExists,
  waitUntilTableNotExists
} from '@aws-sdk/client-dynamodb';
import { DynamoDBDocument, mockClear as mockClearDoc } from '@aws-sdk/lib-dynamodb';

import { reportError, reportResponse } from './stats';
import DynamoDbClient from './dynamodb-client';
import { getStackObj } from './utils';

vi.mock('./stats');

vi.mock('./utils', async () => {
  const utils = await vi.importActual('./utils');
  return { ...utils, getStackObj: vi.fn((...args) => utils.getStackObj(...args)) };
});

describe('lib/dynamodb-client', () => {
  const debug = vi.fn();
  const warn = vi.fn();
  const logger = { debug, warn };
  let client;
  let error;
  let sdkClient;

  function recreateClients(isDocClient) {
    client = new DynamoDbClient({ logger, tableName: 'test-table' });
    sdkClient = isDocClient ? DynamoDBDocument.from() : new DynamoDB();
  }

  afterEach(() => {
    debug.mockClear();
    warn.mockClear();
    reportError.mockClear();
    reportResponse.mockClear();
    getStackObj.mockClear();
  });

  test('the module exports the expected', () => {
    expect(DynamoDbClient).toEqual(expect.any(Function));
    expect(DynamoDbClient).toThrow('Class constructor');
    expect(Object.getOwnPropertyNames(DynamoDbClient.prototype)).toEqual([
      'constructor',
      'stop',
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

  test('new instances of the module wrap instances of the AWS DynamoDB client', () => {
    const awsOptions = { foo: 'bar' };
    client = new DynamoDbClient({ awsOptions });
    expect(client).toBeDefined();
    expect(DynamoDB).toHaveBeenCalledWith(awsOptions);
  });

  test('stop makes retriable calls bail out instead of retrying', async () => {
    recreateClients(true);
    client.stop();
    await expect(client.get({})).rejects.toThrow('The DynamoDB client is stopped.');
    expect(sdkClient.get).not.toHaveBeenCalled();
    expect(warn).not.toHaveBeenCalled();
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
  `('$methodName', ({ isDocClient, isRetriable, methodName }) => {
    beforeEach(() => {
      mockClear();
      mockClearDoc();
      recreateClients(isDocClient);
    });

    test(`${methodName} calls the wrapped AWS SDK method`, async () => {
      const params = { foo: 'bar' };
      await client[methodName](params);
      const expected = isDocClient ? { TableName: 'test-table', ...params } : params;
      expect(sdkClient[methodName]).toHaveBeenCalledWith(expected);
      expect(reportResponse).toHaveBeenCalledWith('dynamoDb');
    });

    test(`${methodName} throws exceptions from the wrapped SDK call`, async () => {
      error = Object.assign(new Error('foo'), { code: 'MissingRequiredParameter' });
      sdkClient[methodName].mockImplementationOnce(() => {
        throw error;
      });
      await expect(client[methodName]({})).rejects.toThrow(error);
      expect(reportError).toHaveBeenCalledWith('dynamoDb', error);
      expect(warn).not.toHaveBeenCalled();
    });

    test(`${methodName} throws exceptions from the wrapped SDK promise`, async () => {
      error = Object.assign(new Error('foo'), { code: 'MissingRequiredParameter' });
      sdkClient[methodName].mockImplementationOnce(() => Promise.reject(error));
      await expect(client[methodName]({})).rejects.toThrow(error);
      expect(reportError).toHaveBeenCalledWith('dynamoDb', error);
      expect(warn).not.toHaveBeenCalled();
    });

    test(`${methodName} throws exceptions with a debuggable stack trace`, async () => {
      error = Object.assign(new Error('foo'), { code: 'MissingRequiredParameter' });
      sdkClient[methodName].mockImplementationOnce(() => Promise.reject(error));
      const stackBefore = await client[methodName]({}).catch((err) => err.stack);

      recreateClients(isDocClient);
      sdkClient[methodName].mockImplementationOnce(() => Promise.reject(error));
      getStackObj.mockReturnValueOnce({ stack: '\n' });
      const stackAfter = await client[methodName]({}).catch((err) => err.stack);

      expect(stackBefore).not.toEqual(stackAfter);
    });

    if (methodName === 'createTable' || methodName === 'tagResource') {
      test(`${methodName} ignores concurrent access errors`, async () => {
        error = Object.assign(new Error('foo'), { code: 'ResourceInUseException' });
        sdkClient[methodName].mockImplementationOnce(() => {
          throw error;
        });
        await expect(client[methodName]({})).resolves.toBeUndefined();
        expect(reportError).toHaveBeenCalledWith('dynamoDb', error);
        expect(debug).toHaveBeenCalledTimes(1);
        expect(debug).toHaveBeenCalledWith(
          methodName === 'createTable'
            ? 'The table already exists.'
            : 'Ignoring concurrent modification of resource.'
        );
      });
    }

    if (isRetriable) {
      test(`${methodName} retries errors from the wrapped SDK`, async () => {
        error = new Error('foo');
        sdkClient[methodName].mockImplementationOnce(() => Promise.reject(error));
        const promise = client[methodName]({});
        await expect(promise).resolves.toEqual({});
        expect(sdkClient[methodName]).toHaveBeenCalledTimes(2);
        expect(reportError).toHaveBeenCalledWith('dynamoDb', error);
        expect(warn).toHaveBeenCalled();
      });
    }
  });

  describe('waitFor', () => {
    beforeEach(() => {
      mockClear();
      mockClearDoc();
      client = new DynamoDbClient({ logger, tableName: 'test-table' });
    });

    test('waitFor(tableExists) resolves with the waiter reason and reports a response', async () => {
      waitUntilTableExists.mockResolvedValueOnce({
        reason: { Table: { TableArn: 'x' } },
        state: 'SUCCESS'
      });
      await expect(client.waitFor('tableExists', { TableName: 'test-table' })).resolves.toEqual({
        Table: { TableArn: 'x' }
      });
      expect(waitUntilTableExists).toHaveBeenCalledWith(
        { client: expect.anything(), maxWaitTime: expect.any(Number) },
        { TableName: 'test-table' }
      );
      expect(reportResponse).toHaveBeenCalledWith('dynamoDb');
    });

    test('waitFor(tableNotExists) uses the not-exists waiter', async () => {
      await client.waitFor('tableNotExists', { TableName: 'test-table' });
      expect(waitUntilTableNotExists).toHaveBeenCalled();
      expect(waitUntilTableExists).not.toHaveBeenCalled();
    });

    test('waitFor reports and rethrows waiter errors', async () => {
      error = Object.assign(new Error('boom'), { code: 'X' });
      waitUntilTableExists.mockRejectedValueOnce(error);
      await expect(client.waitFor('tableExists', {})).rejects.toThrow('boom');
      expect(reportError).toHaveBeenCalledWith('dynamoDb', error);
    });
  });
});
