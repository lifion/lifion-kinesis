'use strict';

const stats = require('./stats');

describe('lib/stats', () => {
  test('the module exports the expected', () => {
    expect(stats).toEqual({
      getStats: expect.any(Function),
      reportError: expect.any(Function),
      reportRecordConsumed: expect.any(Function),
      reportRecordSent: expect.any(Function),
      reportResponse: expect.any(Function)
    });
  });
});
