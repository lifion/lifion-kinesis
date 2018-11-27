'use strict';

const utils = require('./utils');

const { isJson, noop, wait } = utils;

describe('lib/utils', () => {
  test('the module exports the expected', () => {
    expect(utils).toEqual({
      isJson: expect.any(Function),
      noop: expect.any(Function),
      wait: expect.any(Function)
    });
  });

  test('the isJson function returns true when called with a JSON', () => {
    expect(isJson(JSON.stringify({ foo: 'bar' }))).toBe(true);
  });

  test('the isJson function returns false when called with a non-JSON string', () => {
    expect(isJson('{')).toBe(false);
  });

  test('the noop function can be used to default functions in options', () => {
    const { foo = noop } = {};
    expect(() => foo()).not.toThrow();
  });

  test('the wait function can be used to delay the execution of the next statement', async () => {
    const before = new Date().getTime();
    await wait(32);
    const after = new Date().getTime();
    expect(after - before).toBeGreaterThanOrEqual(32);
  });
});
