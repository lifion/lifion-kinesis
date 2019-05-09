'use strict';

const utils = require('./utils');

jest.mock('./constants', () => ({
  BAIL_RETRY_LIST: ['FOO'],
  FORCED_RETRY_LIST: ['BAR']
}));

describe('lib/utils', () => {
  const { shouldBailRetry, transformErrorStack } = utils;

  test('the module exports the expected', () => {
    expect(utils).toEqual({
      shouldBailRetry: expect.any(Function),
      transformErrorStack: expect.any(Function)
    });
  });

  test.each`
    code            | expected | scenario
    ${null}         | ${false} | ${'with no code'}
    ${'FOO'}        | ${true}  | ${'in the bail retry list'}
    ${'BAR'}        | ${false} | ${'in the forced retry list'}
    ${'ETIMEDOUT'}  | ${false} | ${'that should be retried'}
    ${'OUT_OF_MEM'} | ${true}  | ${'that should not be retried'}
  `('shouldBailRetry should return $expected for errors $scenario', ({ code, expected }) => {
    const error = Object.assign(new Error(), code && { code });
    expect(shouldBailRetry(error)).toBe(expected);
  });

  test("transformErrorStack won't transform the stack if not provided with a replacement", () => {
    const error = new Error('foo');
    const beforeStack = error.stack.toString();
    const afterStack = transformErrorStack(error).stack.toString();
    expect(afterStack).toBe(beforeStack);
  });

  test('transformErrorStack will transform the stack with the provided one', () => {
    const stackObj = {};
    Error.captureStackTrace(stackObj);
    const error = new Error('foo');
    const beforeStack = error.stack.toString();
    const afterStack = transformErrorStack(error, stackObj).stack.toString();
    expect(beforeStack).not.toBe(afterStack);
  });

  test('transformErrorStack should maintain the error request ID in the transformed error', () => {
    const stackObj = {};
    Error.captureStackTrace(stackObj);
    const error = Object.assign(new Error('foo'), { requestId: 'bar' });
    const { requestId } = transformErrorStack(error, stackObj);
    expect(requestId).toBe('bar');
  });

  test('transformErrorStack should maintain the error status code in the transformed error', () => {
    const stackObj = {};
    Error.captureStackTrace(stackObj);
    const error = Object.assign(new Error('foo'), { statusCode: 'bar' });
    const { statusCode } = transformErrorStack(error, stackObj);
    expect(statusCode).toBe('bar');
  });
});
