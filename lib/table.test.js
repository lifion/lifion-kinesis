'use strict';

const table = require('./table');

describe('lib/table', () => {
  const { confirmTableTags, ensureTableExists } = table;

  const createTable = jest.fn();
  const describeTable = jest.fn();
  const listTagsOfResource = jest.fn();
  const tagResource = jest.fn();
  const waitFor = jest.fn();
  const client = { createTable, describeTable, listTagsOfResource, tagResource, waitFor };

  const debug = jest.fn();
  const errorMock = jest.fn();
  const logger = { debug, error: errorMock };

  const expectedCreateTableParams = {
    AttributeDefinitions: [
      { AttributeName: 'consumerGroup', AttributeType: 'S' },
      { AttributeName: 'streamName', AttributeType: 'S' }
    ],
    KeySchema: [
      { AttributeName: 'consumerGroup', KeyType: 'HASH' },
      { AttributeName: 'streamName', KeyType: 'RANGE' }
    ],
    SSESpecification: { Enabled: true }
  };

  afterEach(() => {
    createTable.mockClear();
    describeTable.mockClear();
    listTagsOfResource.mockClear();
    tagResource.mockClear();
    waitFor.mockClear();

    debug.mockClear();
    errorMock.mockClear();
  });

  test('the module exports the expected', () => {
    expect(table).toEqual({
      confirmTableTags: expect.any(Function),
      ensureTableExists: expect.any(Function)
    });
  });

  test("confirmTableTags will update the table tags if they don't match the expected", async () => {
    listTagsOfResource.mockResolvedValueOnce({ Tags: [{ Key: 'foo', Value: 'bar' }] });
    await confirmTableTags({ client, logger, tableArn: 'foo', tags: { bar: 'baz' } });
    expect(tagResource).toHaveBeenCalledWith({
      ResourceArn: 'foo',
      Tags: [{ Key: 'foo', Value: 'bar' }, { Key: 'bar', Value: 'baz' }]
    });
    expect(debug).toHaveBeenCalledWith('The table tags have been updated.');
  });

  test("confirmTableTags shouldn't try to update the table tags if they match the expected", async () => {
    listTagsOfResource.mockResolvedValueOnce({ Tags: [{ Key: 'bar', Value: 'baz' }] });
    await confirmTableTags({ client, logger, tags: { bar: 'baz' } });
    expect(tagResource).not.toHaveBeenCalled();
    expect(debug).toHaveBeenCalledWith('The table is already tagged as required.');
  });

  test('ensureTableExists will create the table if it does not exist', async () => {
    describeTable.mockRejectedValueOnce(
      Object.assign(new Error('foo'), { code: 'ResourceNotFoundException' })
    );
    waitFor.mockResolvedValueOnce({ Table: { TableArn: 'baz' } });
    const tableArn = await ensureTableExists({ client, logger, tableName: 'bar' });
    expect(debug.mock.calls).toEqual([
      ['Verifying the "bar" table exists and it\'s active…'],
      ['Trying to create the table…'],
      ['Waiting for the new table to be active…'],
      ['The new table is now active.']
    ]);
    expect(createTable).toHaveBeenCalledWith({
      ...expectedCreateTableParams,
      BillingMode: 'PAY_PER_REQUEST',
      TableName: 'bar'
    });
    expect(waitFor).toHaveBeenCalledWith('tableExists', { TableName: 'bar' });
    expect(tableArn).toBe('baz');
  });

  test('ensureTableExists will use the provisioned throughput if provided', async () => {
    describeTable.mockRejectedValueOnce(
      Object.assign(new Error('foo'), { code: 'ResourceNotFoundException' })
    );
    waitFor.mockResolvedValueOnce({ Table: { TableArn: 'baz' } });
    const tableArn = await ensureTableExists({
      client,
      logger,
      provisionedThroughput: { readCapacityUnits: 5, writeCapacityUnits: 10 },
      tableName: 'bar'
    });
    expect(createTable).toHaveBeenCalledWith({
      ...expectedCreateTableParams,
      ProvisionedThroughput: { ReadCapacityUnits: 5, WriteCapacityUnits: 10 },
      TableName: 'bar'
    });
    expect(tableArn).toBe('baz');
  });

  test("ensureTableExists won't try to create the table if it already exists", async () => {
    describeTable.mockResolvedValueOnce({ Table: { TableArn: 'foo', TableStatus: 'ACTIVE' } });
    const tableArn = await ensureTableExists({ client, logger, tableName: 'bar' });
    expect(debug.mock.calls).toEqual([
      ['Verifying the "bar" table exists and it\'s active…'],
      ["The table exists and it's active."]
    ]);
    expect(createTable).not.toHaveBeenCalled();
    expect(waitFor).not.toHaveBeenCalled();
    expect(tableArn).toBe('foo');
  });

  test('ensureTableExists throws errors received from inner calls', async () => {
    const error = new Error('foo');
    describeTable.mockRejectedValueOnce(error);
    await expect(ensureTableExists({ client, logger, tableName: 'bar' })).rejects.toBe(error);
    expect(errorMock).toHaveBeenCalledWith(error);
  });

  test('ensureTableExists waits for a deleting table before trying to create it', async () => {
    describeTable.mockResolvedValueOnce({ Table: { TableArn: 'foo', TableStatus: 'DELETING' } });
    waitFor.mockResolvedValueOnce();
    waitFor.mockResolvedValueOnce({ Table: { TableArn: 'bar' } });
    const tableArn = await ensureTableExists({ client, logger, tableName: 'baz' });
    expect(debug.mock.calls).toEqual([
      ['Verifying the "baz" table exists and it\'s active…'],
      ['Waiting for the table to complete deletion…'],
      ['The table is now gone.'],
      ['Trying to create the table…'],
      ['Waiting for the new table to be active…'],
      ['The new table is now active.']
    ]);
    expect(createTable).toHaveBeenCalledWith({
      ...expectedCreateTableParams,
      BillingMode: 'PAY_PER_REQUEST',
      TableName: 'baz'
    });
    expect(waitFor.mock.calls).toEqual([
      ['tableNotExists', { TableName: 'baz' }],
      ['tableExists', { TableName: 'baz' }]
    ]);
    expect(tableArn).toBe('bar');
  });

  test('ensureTableExists waits for an updating table before trying to create it', async () => {
    describeTable.mockResolvedValueOnce({ Table: { TableArn: 'foo', TableStatus: 'UPDATING' } });
    waitFor.mockResolvedValueOnce();
    waitFor.mockResolvedValueOnce({ Table: { TableArn: 'bar' } });
    const tableArn = await ensureTableExists({ client, logger, tableName: 'baz' });
    expect(debug.mock.calls).toEqual([
      ['Verifying the "baz" table exists and it\'s active…'],
      ['Waiting for the table to be active…'],
      ['The table is now active.'],
      ["The table exists and it's active."]
    ]);
    expect(describeTable).toHaveBeenCalledWith({ TableName: 'baz' });
    expect(createTable).not.toHaveBeenCalled();
    expect(waitFor).toHaveBeenCalledWith('tableExists', { TableName: 'baz' });
    expect(tableArn).toBe('foo');
  });
});
