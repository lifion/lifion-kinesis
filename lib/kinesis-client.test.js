'use strict';

const KinesisClient = require('./kinesis-client');

describe('lib/kinesis-client', () => {
  test('the module exports the expected', () => {
    expect(KinesisClient).toEqual(expect.any(Function));
    expect(KinesisClient).toThrow('Class constructor');
  });
});
