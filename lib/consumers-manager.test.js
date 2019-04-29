'use strict';

const ConsumersManager = require('./consumers-manager');

describe('lib/consumers-manager', () => {
  test('the module exports the expected', () => {
    expect(ConsumersManager).toEqual(expect.any(Function));
    expect(ConsumersManager).toThrow('Class constructor');
  });
});
