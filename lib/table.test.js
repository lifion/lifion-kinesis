'use strict';

const table = require('./table');

describe('lib/table', () => {
  test('the module exports the expected', () => {
    expect(table).toEqual({
      confirmTableTags: expect.any(Function),
      ensureTableExists: expect.any(Function)
    });
  });
});
