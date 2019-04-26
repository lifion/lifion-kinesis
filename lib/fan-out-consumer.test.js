'use strict';

const FanOutConsumer = require('./fan-out-consumer');

describe('lib/fan-out-consumer', () => {
  test('the module exports the expected', () => {
    expect(FanOutConsumer).toEqual(expect.any(Function));
    expect(FanOutConsumer).toThrow('Class constructor');
  });
});
