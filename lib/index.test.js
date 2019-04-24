'use strict';

const Kinesis = require('.');

describe('lib/index', () => {
  test('the module exports the expected', () => {
    expect(Kinesis).toEqual(expect.any(Function));
    expect(Kinesis).toThrow("Class constructor Kinesis cannot be invoked without 'new'");
  });
});
