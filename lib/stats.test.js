'use strict';

const stats = require('./stats');

describe('lib/stats', () => {
  test('the module exports the expected', () => {
    expect(stats).toEqual({
      getStats: expect.any(Function),
      reportException: expect.any(Function),
      reportRecordConsumed: expect.any(Function),
      reportRecordSent: expect.any(Function),
      reportSuccess: expect.any(Function),
      startStatsEmitter: expect.any(Function)
    });
  });
});
