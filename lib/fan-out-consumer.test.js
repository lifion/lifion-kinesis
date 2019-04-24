'use strict';

const fanOutConsumer = require('./fan-out-consumer');

describe('lib/fan-out-consumer', () => {
  test('the module exports the expected', () => {
    expect(fanOutConsumer).toEqual(expect.any(Function));
    expect(fanOutConsumer).toThrow('Class constructor');
  });
});
