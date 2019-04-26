'use strict';

const PollingConsumer = require('./polling-consumer');

describe('lib/polling-consumer', () => {
  test('the module exports the expected', () => {
    expect(PollingConsumer).toEqual(expect.any(Function));
    expect(PollingConsumer).toThrow('Class constructor');
  });
});
