'use strict';

const HeartbeatManager = require('./heartbeat-manager');

describe('lib/heartbeat-manager', () => {
  test('the module exports the expected', () => {
    expect(HeartbeatManager).toEqual(expect.any(Function));
    expect(HeartbeatManager).toThrow('Class constructor');
  });
});
