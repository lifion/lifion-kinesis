'use strict';

const StateStore = require('./state-store');

describe('lib/state-store', () => {
  test('the module exports the expected', () => {
    expect(StateStore).toEqual(expect.any(Function));
    expect(StateStore).toThrow('Class constructor');
  });
});
