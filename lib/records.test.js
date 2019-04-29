'use strict';

const records = require('./records');

describe('lib/records', () => {
  test('the module exports the expected', () => {
    expect(records).toEqual({
      getRecordsDecoder: expect.any(Function),
      getRecordsEncoder: expect.any(Function),
      RecordsDecoder: expect.any(Function)
    });
    const { RecordsDecoder } = records;
    expect(RecordsDecoder).toThrow('Class constructor');
  });
});
