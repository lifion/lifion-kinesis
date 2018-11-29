'use strict';

const utils = require('./utils');

const { isJson, noop, safeJsonParse, wait } = utils;

describe('lib/utils', () => {
  test('the module exports the expected', () => {
    expect(utils).toEqual({
      isJson: expect.any(Function),
      noop: expect.any(Function),
      safeJsonParse: expect.any(Function),
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

  test('the safeJsonParse function returns an object from a JSON string', () => {
    const json = JSON.stringify({ foo: 'bar' });
    expect(safeJsonParse(json)).toEqual({ foo: 'bar' });
  });

  test('the safeJsonParse function returns an empty object on invalid JSON strings', () => {
    expect(safeJsonParse('{foo}')).toEqual({});
  });

  test('the wait function can be used to delay the execution of the next statement', async () => {
    const before = new Date().getTime();
    await wait(32);
    const after = new Date().getTime();
    expect(after - before).toBeGreaterThanOrEqual(32);
  });
});
