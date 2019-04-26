'use strict';

const LeaseManager = require('./lease-manager');

describe('lib/lease-manager', () => {
  test('the module exports the expected', () => {
    expect(LeaseManager).toEqual(expect.any(Function));
    expect(LeaseManager).toThrow('Class constructor');
  });
});
