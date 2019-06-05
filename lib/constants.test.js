/* eslint-disable global-require */

'use strict';

const constants = require('./constants');

describe('lib/constants', () => {
  test('the module exports the expected', () => {
    expect(constants).toEqual({
      BAIL_RETRY_LIST: expect.any(Array),
      FORCED_RETRY_LIST: expect.any(Array)
    });
  });
});
