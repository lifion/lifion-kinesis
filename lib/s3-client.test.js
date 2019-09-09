'use strict';

const { S3, mockClear } = require('aws-sdk');
const S3Client = require('./s3-client');
const { reportError, reportResponse } = require('./stats');
const { getStackObj } = require('./utils');

jest.mock('./stats');
jest.mock('./utils', () => {
  const utils = jest.requireActual('./utils');
  return { ...utils, getStackObj: jest.fn((...args) => utils.getStackObj(...args)) };
});

describe('lib/consumers-manager', () => {
  const warn = jest.fn();
  const logger = { warn };

  afterEach(() => {
    warn.mockClear();
  });

  test('the module exports the expected', () => {
    expect(S3Client).toEqual(expect.any(Function));
    expect(S3Client).toThrow('Class constructor');
    expect(Object.getOwnPropertyNames(S3Client.prototype)).toEqual([
      'constructor',
      'createBucket',
      'getBucketLifecycleConfiguration',
      'getBucketTagging',
      'getObject',
      'headBucket',
      'putBucketLifecycleConfiguration',
      'putBucketTagging',
      'putObject'
    ]);
  });

  test('new instances of the module wrap instances of AWS.S3', () => {
    const options = { bucketName: 'test bucket name', foo: 'bar', logger };
    const client = new S3Client(options);

    expect(client).toBeDefined();
    expect(S3).toHaveBeenCalledWith({ foo: 'bar' });
  });

  describe.each`
    methodName                           | isRetriable | expectedRetries | defaultValue      | notFoundErrorCode
    ${'createBucket'}                    | ${false}    | ${0}            | ${null}           | ${null}
    ${'getBucketLifecycleConfiguration'} | ${true}     | ${1}            | ${{ Rules: [] }}  | ${'NoSuchLifecycleConfiguration'}
    ${'getBucketTagging'}                | ${true}     | ${1}            | ${{ TagSet: [] }} | ${'NoSuchTagSet'}
    ${'getObject'}                       | ${true}     | ${2}            | ${null}           | ${null}
    ${'headBucket'}                      | ${true}     | ${2}            | ${null}           | ${null}
    ${'putBucketLifecycleConfiguration'} | ${true}     | ${2}            | ${null}           | ${null}
    ${'putBucketTagging'}                | ${true}     | ${2}            | ${null}           | ${null}
    ${'putObject'}                       | ${true}     | ${2}            | ${null}           | ${null}
  `(
    '$methodName',
    ({ defaultValue, expectedRetries, isRetriable, methodName, notFoundErrorCode }) => {
      const options = { bucketName: 'test bucket name', logger };
      const client = new S3Client(options);
      const sdkClient = new S3();
      const error = Object.assign(new Error('foo'), { code: 'MissingRequiredParameter' });

      beforeEach(() => {
        mockClear();
        sdkClient[methodName].mockClear();
      });

      test(`calls the wrapped AWS SDK method ${methodName}`, async () => {
        const params = { foo: 'bar' };
        await client[methodName](params);
        expect(sdkClient[methodName]).toHaveBeenCalledWith(params);
        expect(reportResponse).toHaveBeenCalledWith('s3');
      });

      test(`throws exceptions from the wrapped SDK call`, async () => {
        sdkClient[methodName].mockImplementationOnce(() => {
          throw error;
        });

        await expect(client[methodName]({})).rejects.toThrow(error);
        expect(reportError).toHaveBeenCalledWith('s3', error);
        expect(warn).not.toHaveBeenCalled();
      });

      test(`throws exceptions from the wrapped SDK promise`, async () => {
        sdkClient[methodName].mockImplementationOnce(() => ({
          promise: () => Promise.reject(error)
        }));

        await expect(client[methodName]({})).rejects.toThrow(error);
        expect(reportError).toHaveBeenCalledWith('s3', error);
        expect(warn).not.toHaveBeenCalled();
      });

      test(`throws exceptions with a debuggable stack trace`, async () => {
        const rejectedPromiseImplementation = () => ({
          promise: () => Promise.reject(error)
        });
        sdkClient[methodName]
          .mockImplementationOnce(rejectedPromiseImplementation)
          .mockImplementationOnce(rejectedPromiseImplementation);

        const stackBefore = await client[methodName]({}).catch(err => err.stack);
        getStackObj.mockReturnValueOnce({ stack: '\n' });
        const stackAfter = await client[methodName]({}).catch(err => err.stack);

        expect(stackBefore).not.toEqual(stackAfter);
      });

      if (defaultValue) {
        test('defaults to a given default value for specific error codes', async () => {
          const rejectedPromiseImplementation = () => ({
            promise: () =>
              Promise.reject(Object.assign(new Error('foo'), { code: notFoundErrorCode }))
          });
          sdkClient[methodName]
            .mockImplementationOnce(rejectedPromiseImplementation)
            .mockImplementationOnce(rejectedPromiseImplementation);

          await expect(client[methodName]()).resolves.toEqual(defaultValue);
        });
      }

      if (isRetriable) {
        test(`retries errors from the wrapped SDK`, async () => {
          sdkClient[methodName].mockImplementationOnce(() => ({
            promise: () =>
              Promise.reject(Object.assign(new Error('foo'), { code: notFoundErrorCode }))
          }));
          sdkClient[methodName].mockImplementationOnce(() => ({
            promise: () => Promise.resolve(defaultValue)
          }));

          const promise = client[methodName]({});

          await expect(promise).resolves.toEqual(defaultValue);
          expect(sdkClient[methodName]).toHaveBeenCalledTimes(expectedRetries);
          expect(reportError).toHaveBeenCalledWith('s3', error);
          expect(warn).toHaveBeenCalledTimes(expectedRetries - 1);
        });
      }
    }
  );
});
