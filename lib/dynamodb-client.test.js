'use strict';

const DynamoDbClient = require('./dynamodb-client');

describe('lib/dynamodb-client', () => {
  test('the module exports the expected', () => {
    expect(DynamoDbClient).toEqual(expect.any(Function));
    expect(DynamoDbClient).toThrow('Class constructor');
  });
});
