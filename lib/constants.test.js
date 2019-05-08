/* eslint-disable global-require */

'use strict';

describe('lib/constants', () => {
  let constants;

  beforeEach(() => {
    delete process.env.CAPTURE_STACK_TRACE;
    jest.resetModules();
  });

  test('the module exports the expected', () => {
    constants = require('./constants');
    expect(constants).toEqual({
      BAIL_RETRY_LIST: expect.any(Array),
      CAPTURE_STACK_TRACE: false,
      FORCED_RETRY_LIST: expect.any(Array)
    });
  });

  test('the module uses the environment variables as expected', () => {
    process.env.CAPTURE_STACK_TRACE = true;
    const { CAPTURE_STACK_TRACE } = require('./constants');
    expect(CAPTURE_STACK_TRACE).toBe(true);
  });
});
