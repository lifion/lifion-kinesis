'use strict';

const records = require('./records');

describe('lib/records', () => {
  test('the module exports the expected', () => {
    expect(records).toEqual({
      RecordsDecoder: expect.any(Function),
      getRecordsDecoder: expect.any(Function),
      getRecordsEncoder: expect.any(Function)
    });
    const { RecordsDecoder } = records;
    expect(RecordsDecoder).toThrow('Class constructor');
  });
});
