'use strict';

const { resetMockCounter } = require('short-uuid');

const DynamoDbClient = require('./dynamodb-client');
const StateStore = require('./state-store');
const { confirmTableTags, ensureTableExists } = require('./table');

jest.mock('./dynamodb-client', () => {
  const get = jest.fn(() => Promise.resolve({}));
  const put = jest.fn(() => Promise.resolve({}));
  const deleteMock = jest.fn(() => Promise.resolve({}));
  const update = jest.fn(() => Promise.resolve({}));
  return jest.fn(() => ({ delete: deleteMock, get, put, update }));
});

jest.mock('./table');

describe('lib/state-store', () => {
  const debug = jest.fn();
  const error = jest.fn();
  const warn = jest.fn();
  const logger = { debug, error, warn };

  const options = {
    consumerGroup: 'test-group',
    consumerId: 'test-id',
    dynamoDb: { tags: { foo: 'bar' } },
    logger,
    streamCreatedOn: '2019-01-01T00:00:00.000Z',
    streamName: 'test-stream',
    useAutoShardAssignment: true,
    useEnhancedFanOut: false
  };

  beforeAll(() => {
    ensureTableExists.mockResolvedValue('arn:table-arn');
  });

  afterEach(() => {
    debug.mockClear();
    ensureTableExists.mockClear();
    error.mockClear();
    warn.mockClear();
    resetMockCounter();

    const client = new DynamoDbClient();
    client.delete.mockClear();
    client.get.mockClear();
    client.put.mockClear();
    client.update.mockClear();

    DynamoDbClient.mockClear();
  });

  test('the module exports the expected', () => {
    expect(StateStore).toEqual(expect.any(Function));
    expect(StateStore).toThrow('Class constructor');
    expect(Object.getOwnPropertyNames(StateStore.prototype)).toEqual([
      'constructor',
      'clearOldConsumers',
      'deregisterEnhancedConsumer',
      'ensureShardStateExists',
      'getAssignedEnhancedConsumer',
      'getEnhancedConsumers',
      'getOwnedShards',
      'getShardAndStreamState',
      'getShardsData',
      'lockShardLease',
      'markShardAsDepleted',
      'registerConsumer',
      'registerEnhancedConsumer',
      'releaseShardLease',
      'start',
      'storeShardCheckpoint'
    ]);
  });

  test('starting the state store will create a DynamoDB table', async () => {
    const store = new StateStore(options);
    expect(store).toBeDefined();
    await expect(store.start()).resolves.toBeUndefined();
    expect(DynamoDbClient).toHaveBeenCalledWith(
      expect.objectContaining({
        awsOptions: {},
        logger,
        tableName: 'lifion-kinesis-state'
      })
    );
    const client = new DynamoDbClient();
    expect(ensureTableExists).toHaveBeenCalledWith(
      expect.objectContaining({
        client,
        logger,
        provisionedThroughput: undefined,
        tableName: 'lifion-kinesis-state'
      })
    );
    expect(confirmTableTags).toHaveBeenCalledWith(
      expect.objectContaining({
        client,
        logger,
        tableArn: 'arn:table-arn',
        tags: { foo: 'bar' }
      })
    );
    expect(client.get).toHaveBeenCalledWith({
      Key: { consumerGroup: 'test-group', streamName: 'test-stream' }
    });
    expect(client.put).toHaveBeenCalledWith({
      ConditionExpression: 'attribute_not_exists(streamName)',
      Item: {
        consumerGroup: 'test-group',
        consumers: {},
        enhancedConsumers: {},
        shards: {},
        streamCreatedOn: '2019-01-01T00:00:00.000Z',
        streamName: 'test-stream',
        version: '0000'
      }
    });
    expect(debug).toHaveBeenNthCalledWith(1, 'Initial state has been recorded for the stream.');
    expect(debug).toHaveBeenCalledTimes(1);
  });

  test("starting the state store shouldn't be a problem if the DynamoDB table exists", async () => {
    const store = new StateStore({ ...options, dynamoDb: { tags: undefined } });
    const client = new DynamoDbClient();
    client.get.mockResolvedValueOnce({ Item: { streamCreatedOn: '2019-01-01T00:00:00.000Z' } });
    client.put.mockRejectedValueOnce(
      Object.assign(new Error('foo'), { code: 'ConditionalCheckFailedException' })
    );
    await expect(store.start()).resolves.toBeUndefined();
    expect(client.delete).not.toHaveBeenCalled();
    expect(debug).not.toHaveBeenCalled();
  });

  test('starting the store recreates the table if stream created on differs', async () => {
    const store = new StateStore({ ...options, dynamoDb: { tags: undefined } });
    const client = new DynamoDbClient();
    client.get.mockResolvedValueOnce({ Item: { streamCreatedOn: '2018-12-31T00:00:00.000Z' } });
    await expect(store.start()).resolves.toBeUndefined();
    expect(client.delete).toHaveBeenCalledWith({
      Key: { consumerGroup: 'test-group', streamName: 'test-stream' }
    });
    expect(debug).toHaveBeenNthCalledWith(1, 'Initial state has been recorded for the stream.');
    expect(debug).toHaveBeenCalledTimes(1);
    expect(warn).toHaveBeenNthCalledWith(
      1,
      'Stream state has been reset. Non-matching stream creation timestamp.'
    );
    expect(warn).toHaveBeenCalledTimes(1);
  });

  test('starting the store throws if DynamoDB throws', async () => {
    const store = new StateStore({ ...options, dynamoDb: { tags: undefined } });
    const client = new DynamoDbClient();
    client.put.mockRejectedValueOnce(new Error('foo'));
    await expect(store.start()).rejects.toThrow('foo');
    expect(error.mock.calls).toEqual([[expect.objectContaining({ message: 'foo' })]]);
  });

  test('clearOldConsumers removes late-heartbeat consumers and references to them', async () => {
    const store = new StateStore(options);
    await store.start();

    const { get, update } = new DynamoDbClient();

    get.mockResolvedValueOnce({
      Item: {
        consumers: {
          'consumer-1': { heartbeat: new Date() },
          'consumer-2': { heartbeat: new Date('2019-01-01') },
          'consumer-3': { heartbeat: new Date('2020-01-01') }
        },
        enhancedConsumers: {
          'enhanced-consumer-1': { isUsedBy: 'consumer-2' },
          'enhanced-consumer-2': { isUsedBy: 'consumer-3' }
        },
        version: '0000'
      }
    });

    await expect(store.clearOldConsumers(1000)).resolves.toBeUndefined();

    expect(update).toHaveBeenNthCalledWith(1, {
      ConditionExpression: '#b = :y',
      ExpressionAttributeNames: {
        '#0': 'consumer-2',
        '#a': 'consumers',
        '#b': 'version'
      },
      ExpressionAttributeValues: {
        ':x': '0001',
        ':y': '0000'
      },
      Key: {
        consumerGroup: 'test-group',
        streamName: 'test-stream'
      },
      UpdateExpression: 'REMOVE #a.#0 SET #b = :x'
    });

    expect(update).toHaveBeenNthCalledWith(2, {
      ConditionExpression: '#a.#b.#c = :w AND #a.#b.#d = :x',
      ExpressionAttributeNames: {
        '#a': 'enhancedConsumers',
        '#b': 'enhanced-consumer-1',
        '#c': 'isUsedBy',
        '#d': 'version'
      },
      ExpressionAttributeValues: {
        ':w': 'consumer-2',
        ':x': undefined,
        ':y': null,
        ':z': '0002'
      },
      Key: {
        consumerGroup: 'test-group',
        streamName: 'test-stream'
      },
      UpdateExpression: 'SET #a.#b.#c = :y, #a.#b.#d = :z'
    });

    expect(update).toHaveBeenCalledTimes(2);

    expect(debug).toHaveBeenNthCalledWith(2, 'Cleared 1 old consumer(s).');
    expect(debug).toHaveBeenNthCalledWith(
      3,
      'Enhanced consumer "enhanced-consumer-1" can be released, missed heartbeat.'
    );
    expect(debug).toHaveBeenNthCalledWith(
      4,
      'Enhanced consumer "enhanced-consumer-1" has been released.'
    );
    expect(debug).toHaveBeenCalledTimes(4);
  });

  test('clearOldConsumers ignores conditional update mismatches', async () => {
    const store = new StateStore(options);
    await store.start();
    const { get, update } = new DynamoDbClient();
    get.mockResolvedValueOnce({
      Item: {
        consumers: {
          'consumer-1': { heartbeat: new Date('2019-01-01') }
        },
        enhancedConsumers: {},
        version: '0000'
      }
    });
    update.mockRejectedValueOnce(
      Object.assign(new Error('foo'), { code: 'ConditionalCheckFailedException' })
    );
    await expect(store.clearOldConsumers(1000)).resolves.toBeUndefined();
    expect(update).toHaveBeenNthCalledWith(1, {
      ConditionExpression: '#b = :y',
      ExpressionAttributeNames: { '#0': 'consumer-1', '#a': 'consumers', '#b': 'version' },
      ExpressionAttributeValues: { ':x': '0001', ':y': '0000' },
      Key: { consumerGroup: 'test-group', streamName: 'test-stream' },
      UpdateExpression: 'REMOVE #a.#0 SET #b = :x'
    });
    expect(update).toHaveBeenCalledTimes(1);
  });

  test('clearOldConsumers throws if the update to remove old consumers throws', async () => {
    const store = new StateStore(options);
    await store.start();
    const { get, update } = new DynamoDbClient();
    get.mockResolvedValueOnce({
      Item: {
        consumers: {
          'consumer-1': { heartbeat: new Date('2019-01-01') }
        },
        enhancedConsumers: {},
        version: '0000'
      }
    });
    update.mockRejectedValueOnce(new Error('foo'));
    await expect(store.clearOldConsumers(1000)).rejects.toThrow('foo');
    expect(error).toHaveBeenNthCalledWith(1, expect.objectContaining({ message: 'foo' }));
    expect(error).toHaveBeenCalledTimes(1);
  });

  test('clearOldConsumers ignores mismatches when releasing enhanced consumers', async () => {
    const store = new StateStore(options);
    await store.start();
    const { get, update } = new DynamoDbClient();
    get.mockResolvedValueOnce({
      Item: {
        consumers: {
          'consumer-1': { heartbeat: new Date() },
          'consumer-2': { heartbeat: new Date('2019-01-01') },
          'consumer-3': { heartbeat: new Date('2020-01-01') }
        },
        enhancedConsumers: {
          'enhanced-consumer-1': { isUsedBy: 'consumer-2' },
          'enhanced-consumer-2': { isUsedBy: 'consumer-3' }
        },
        version: '0000'
      }
    });
    update.mockResolvedValueOnce();
    update.mockRejectedValueOnce(
      Object.assign(new Error('foo'), { code: 'ConditionalCheckFailedException' })
    );
    await expect(store.clearOldConsumers(1000)).resolves.toBeUndefined();
    expect(debug).toHaveBeenCalledTimes(4);
    expect(debug).toHaveBeenNthCalledWith(
      4,
      'Enhanced consumer "enhanced-consumer-1" can\'t be released.'
    );
  });

  test('clearOldConsumers clears enhanced consumers when no consumers exist', async () => {
    const store = new StateStore(options);
    await store.start();

    const { get, update } = new DynamoDbClient();

    get.mockResolvedValueOnce({
      Item: {
        consumers: {},
        enhancedConsumers: {
          'enhanced-consumer-1': {
            isUsedBy: 'consumer-1'
          }
        },
        version: '0000'
      }
    });

    await expect(store.clearOldConsumers(1000)).resolves.toBeUndefined();

    expect(update).toHaveBeenNthCalledWith(1, {
      ConditionExpression: '#a.#b.#c = :w AND #a.#b.#d = :x',
      ExpressionAttributeNames: {
        '#a': 'enhancedConsumers',
        '#b': 'enhanced-consumer-1',
        '#c': 'isUsedBy',
        '#d': 'version'
      },
      ExpressionAttributeValues: {
        ':w': 'consumer-1',
        ':x': undefined,
        ':y': null,
        ':z': '0001'
      },
      Key: {
        consumerGroup: 'test-group',
        streamName: 'test-stream'
      },
      UpdateExpression: 'SET #a.#b.#c = :y, #a.#b.#d = :z'
    });

    expect(update).toHaveBeenCalledTimes(1);

    expect(debug).toHaveBeenNthCalledWith(
      2,
      'Enhanced consumer "enhanced-consumer-1" can be released, unknown owner.'
    );
    expect(debug).toHaveBeenNthCalledWith(
      3,
      'Enhanced consumer "enhanced-consumer-1" has been released.'
    );
    expect(debug).toHaveBeenCalledTimes(3);
  });

  test('clearOldConsumers throws if clearing enhanced consumers throws', async () => {
    const store = new StateStore(options);
    await store.start();

    const { get, update } = new DynamoDbClient();

    get.mockResolvedValueOnce({
      Item: {
        consumers: {},
        enhancedConsumers: {
          'enhanced-consumer-1': {
            isUsedBy: 'consumer-2'
          }
        },
        version: '0000'
      }
    });

    update.mockRejectedValueOnce(new Error('foo'));
    await expect(store.clearOldConsumers(1000)).rejects.toThrow('foo');
    expect(error).toHaveBeenNthCalledWith(1, expect.objectContaining({ message: 'foo' }));
    expect(error).toHaveBeenCalledTimes(1);
  });

  test('deregisterEnhancedConsumer removes enhanced fan-out consumers from the store', async () => {
    const store = new StateStore(options);
    await store.start();
    await expect(store.deregisterEnhancedConsumer('enhanced-consumer-1')).resolves.toBeUndefined();
    const { update } = new DynamoDbClient();
    expect(update).toHaveBeenCalledWith({
      ConditionExpression: 'attribute_exists(#a.#b)',
      ExpressionAttributeNames: { '#a': 'enhancedConsumers', '#b': 'enhanced-consumer-1' },
      Key: { consumerGroup: 'test-group', streamName: 'test-stream' },
      UpdateExpression: 'REMOVE #a.#b'
    });
    expect(debug).toHaveBeenNthCalledWith(
      2,
      'The enhanced consumer "enhanced-consumer-1" is now de-registered.'
    );
    expect(debug).toHaveBeenCalledTimes(2);
  });

  test('deregisterEnhancedConsumer ignores conditional update mismatches', async () => {
    const store = new StateStore(options);
    await store.start();
    const { update } = new DynamoDbClient();
    update.mockRejectedValueOnce(
      Object.assign(new Error('foo'), { code: 'ConditionalCheckFailedException' })
    );
    await expect(store.deregisterEnhancedConsumer('enhanced-consumer-1')).resolves.toBeUndefined();
    expect(error).not.toHaveBeenCalled();
  });

  test('deregisterEnhancedConsumer throws if DynamoDB throws', async () => {
    const store = new StateStore(options);
    await store.start();
    const { update } = new DynamoDbClient();
    update.mockRejectedValueOnce(new Error('foo'));
    await expect(store.deregisterEnhancedConsumer('enhanced-consumer-1')).rejects.toThrow('foo');
    expect(error).toHaveBeenNthCalledWith(1, expect.objectContaining({ message: 'foo' }));
    expect(error).toHaveBeenCalledTimes(1);
  });

  test('ensureShardStateExists makes a conditional update to insert the shard state', async () => {
    const store = new StateStore(options);
    await store.start();
    await expect(store.ensureShardStateExists('test-shard-id', {}, {})).resolves.toBeUndefined();
    const { update } = new DynamoDbClient();
    expect(update).toHaveBeenCalledWith({
      ConditionExpression: 'attribute_not_exists(#a.#b)',
      ExpressionAttributeNames: { '#a': 'shards', '#b': 'test-shard-id' },
      ExpressionAttributeValues: {
        ':x': {
          checkpoint: null,
          depleted: false,
          leaseExpiration: null,
          leaseOwner: null,
          parent: undefined,
          version: '0001'
        }
      },
      Key: { consumerGroup: 'test-group', streamName: 'test-stream' },
      UpdateExpression: 'SET #a.#b = :x'
    });
  });

  test('ensureShardStateExists ignores conditional update mismatches', async () => {
    const store = new StateStore(options);
    await store.start();
    const { update } = new DynamoDbClient();
    update.mockRejectedValueOnce(
      Object.assign(new Error('foo'), { code: 'ConditionalCheckFailedException' })
    );
    await expect(store.ensureShardStateExists('test-shard-id', {}, {})).resolves.toBeUndefined();
    expect(error).not.toHaveBeenCalled();
  });

  test('ensureShardStateExists throws if DynamoDB throws', async () => {
    const store = new StateStore(options);
    await store.start();
    const { update } = new DynamoDbClient();
    update.mockRejectedValueOnce(new Error('foo'));
    await expect(store.ensureShardStateExists('test-shard-id', {}, {})).rejects.toThrow('foo');
    expect(error).toHaveBeenNthCalledWith(1, expect.objectContaining({ message: 'foo' }));
    expect(error).toHaveBeenCalledTimes(1);
  });

  test('getAssignedEnhancedConsumer marks the consumer as inactive on no assignment', async () => {
    const store = new StateStore(options);
    await store.start();

    const { get, update } = new DynamoDbClient();
    update.mockRejectedValueOnce(new Error('foo'));
    get.mockResolvedValueOnce({ Item: { enhancedConsumers: {} } });

    await expect(store.getAssignedEnhancedConsumer()).resolves.toBe(null);

    expect(warn).toHaveBeenNthCalledWith(
      1,
      'All enhanced fan-out consumers are assigned. Waiting until one is available…'
    );
    expect(warn).toHaveBeenCalledTimes(1);

    expect(update).toHaveBeenNthCalledWith(1, {
      ExpressionAttributeNames: {
        '#a': 'consumers',
        '#b': 'test-id',
        '#c': 'isActive'
      },
      ExpressionAttributeValues: {
        ':z': false
      },
      Key: {
        consumerGroup: 'test-group',
        streamName: 'test-stream'
      },
      UpdateExpression: 'SET #a.#b.#c = :z'
    });
    expect(update).toHaveBeenCalledTimes(1);
  });

  test('getAssignedEnhancedConsumer returns the assigned enhanced consumer ARN', async () => {
    const store = new StateStore(options);
    await store.start();
    const { get, update } = new DynamoDbClient();
    get.mockResolvedValueOnce({
      Item: {
        enhancedConsumers: {
          'enhanced-consumer-0': {
            arn: 'arn:enhanced-consumer-0',
            isUsedBy: null
          },
          'enhanced-consumer-1': {
            arn: 'arn:enhanced-consumer-1',
            isUsedBy: 'test-id'
          }
        }
      }
    });
    await expect(store.getAssignedEnhancedConsumer()).resolves.toBe('arn:enhanced-consumer-1');
    expect(update).toHaveBeenCalledWith({
      ExpressionAttributeNames: { '#a': 'consumers', '#b': 'test-id', '#c': 'isActive' },
      ExpressionAttributeValues: { ':z': true },
      Key: { consumerGroup: 'test-group', streamName: 'test-stream' },
      UpdateExpression: 'SET #a.#b.#c = :z'
    });
    expect(debug).toHaveBeenNthCalledWith(
      2,
      'Using the "enhanced-consumer-1" enhanced fan-out consumer.'
    );
  });

  test('getAssignedEnhancedConsumer locks an enhanced consumer if not assigned yet', async () => {
    const store = new StateStore(options);
    await store.start();

    const { get, update } = new DynamoDbClient();
    get.mockResolvedValueOnce({
      Item: {
        enhancedConsumers: {
          'enhanced-consumer-0': {
            arn: 'arn:enhanced-consumer-0',
            isUsedBy: null,
            version: '0000'
          }
        }
      }
    });

    await expect(store.getAssignedEnhancedConsumer()).resolves.toBe('arn:enhanced-consumer-0');

    expect(update).toHaveBeenNthCalledWith(1, {
      ConditionExpression: '#a.#b.#d = :z AND #a.#b.#c = :v',
      ExpressionAttributeNames: {
        '#a': 'enhancedConsumers',
        '#b': 'enhanced-consumer-0',
        '#c': 'isUsedBy',
        '#d': 'version'
      },
      ExpressionAttributeValues: {
        ':v': null,
        ':x': 'test-id',
        ':y': '0001',
        ':z': '0000'
      },
      Key: {
        consumerGroup: 'test-group',
        streamName: 'test-stream'
      },
      UpdateExpression: 'SET #a.#b.#c = :x, #a.#b.#d = :y'
    });

    expect(update).toHaveBeenNthCalledWith(2, {
      ExpressionAttributeNames: {
        '#a': 'consumers',
        '#b': 'test-id',
        '#c': 'isActive'
      },
      ExpressionAttributeValues: {
        ':z': true
      },
      Key: {
        consumerGroup: 'test-group',
        streamName: 'test-stream'
      },
      UpdateExpression: 'SET #a.#b.#c = :z'
    });
    expect(update).toHaveBeenCalledTimes(2);

    expect(debug).toHaveBeenNthCalledWith(
      2,
      'Using the "enhanced-consumer-0" enhanced fan-out consumer.'
    );
  });

  test('getAssignedEnhancedConsumer works when auto shard assignment is off', async () => {
    const store = new StateStore({ ...options, useAutoShardAssignment: false });
    await store.start();

    const { get, update } = new DynamoDbClient();
    get.mockResolvedValueOnce({
      Item: {
        enhancedConsumers: {
          'enhanced-consumer-0': {
            arn: 'arn:enhanced-consumer-0',
            isUsedBy: null,
            version: '0000'
          }
        }
      }
    });

    await expect(store.getAssignedEnhancedConsumer()).resolves.toBe('arn:enhanced-consumer-0');

    expect(update).toHaveBeenNthCalledWith(1, {
      ConditionExpression: '#a.#b.#d = :z AND #a.#b.#c = :v',
      ExpressionAttributeNames: {
        '#a': 'enhancedConsumers',
        '#b': 'enhanced-consumer-0',
        '#c': 'isUsedBy',
        '#d': 'version',
        '#e': 'shards'
      },
      ExpressionAttributeValues: {
        ':v': null,
        ':w': {},
        ':x': 'test-id',
        ':y': '0001',
        ':z': '0000'
      },
      Key: {
        consumerGroup: 'test-group',
        streamName: 'test-stream'
      },
      UpdateExpression: 'SET #a.#b.#c = :x, #a.#b.#d = :y, #a.#b.#e = if_not_exists(#a.#b.#e, :w)'
    });

    expect(update).toHaveBeenNthCalledWith(2, {
      ExpressionAttributeNames: {
        '#a': 'consumers',
        '#b': 'test-id',
        '#c': 'isActive'
      },
      ExpressionAttributeValues: {
        ':z': true
      },
      Key: {
        consumerGroup: 'test-group',
        streamName: 'test-stream'
      },
      UpdateExpression: 'SET #a.#b.#c = :z'
    });

    expect(update).toHaveBeenCalledTimes(2);

    expect(debug).toHaveBeenNthCalledWith(
      2,
      'Using the "enhanced-consumer-0" enhanced fan-out consumer.'
    );
  });

  test('getAssignedEnhancedConsumer returns null if failed to lock an assignment', async () => {
    const store = new StateStore(options);
    await store.start();

    const { get, update } = new DynamoDbClient();
    get.mockResolvedValueOnce({
      Item: {
        enhancedConsumers: {
          'enhanced-consumer-0': {
            arn: 'arn:enhanced-consumer-0',
            isUsedBy: null,
            version: '0000'
          }
        }
      }
    });

    update.mockRejectedValueOnce(
      Object.assign(new Error('foo'), { code: 'ConditionalCheckFailedException' })
    );

    await expect(store.getAssignedEnhancedConsumer()).resolves.toBe(null);

    expect(update).toHaveBeenNthCalledWith(1, {
      ConditionExpression: '#a.#b.#d = :z AND #a.#b.#c = :v',
      ExpressionAttributeNames: {
        '#a': 'enhancedConsumers',
        '#b': 'enhanced-consumer-0',
        '#c': 'isUsedBy',
        '#d': 'version'
      },
      ExpressionAttributeValues: {
        ':v': null,
        ':x': 'test-id',
        ':y': '0001',
        ':z': '0000'
      },
      Key: {
        consumerGroup: 'test-group',
        streamName: 'test-stream'
      },
      UpdateExpression: 'SET #a.#b.#c = :x, #a.#b.#d = :y'
    });

    expect(update).toHaveBeenNthCalledWith(2, {
      ExpressionAttributeNames: {
        '#a': 'consumers',
        '#b': 'test-id',
        '#c': 'isActive'
      },
      ExpressionAttributeValues: {
        ':z': false
      },
      Key: {
        consumerGroup: 'test-group',
        streamName: 'test-stream'
      },
      UpdateExpression: 'SET #a.#b.#c = :z'
    });

    expect(update).toHaveBeenCalledTimes(2);

    expect(debug).toHaveBeenCalledTimes(1);
  });

  test('getAssignedEnhancedConsumer throws if DynamoDB throws', async () => {
    const store = new StateStore(options);
    await store.start();
    const { get, update } = new DynamoDbClient();
    get.mockResolvedValueOnce({
      Item: {
        enhancedConsumers: {
          'enhanced-consumer-0': {
            arn: 'arn:enhanced-consumer-0',
            isUsedBy: null,
            version: '0000'
          }
        }
      }
    });
    update.mockRejectedValueOnce(new Error('foo'));
    await expect(store.getAssignedEnhancedConsumer()).rejects.toThrow('foo');
    expect(update).toHaveBeenCalledTimes(1);
    expect(debug).toHaveBeenCalledTimes(1);
    expect(error).toHaveBeenNthCalledWith(1, expect.objectContaining({ message: 'foo' }));
    expect(error).toHaveBeenCalledTimes(1);
  });

  test('getOwnedShards returns the shards with active leases owned by the consumer', async () => {
    const state = new StateStore(options);
    await state.start();
    const { get } = new DynamoDbClient();
    const fiveMinsFromNow = new Date(Date.now() + 5 * 60 * 1000);
    get.mockResolvedValueOnce({
      Item: {
        shards: {
          'shard-0001': {
            checkpoint: '1',
            depleted: false,
            leaseExpiration: fiveMinsFromNow,
            leaseOwner: 'test-id',
            version: '0000'
          },
          'shard-0002': {
            checkpoint: '2',
            depleted: true,
            leaseExpiration: fiveMinsFromNow,
            leaseOwner: 'test-id',
            version: '0000'
          }
        }
      }
    });
    await expect(state.getOwnedShards()).resolves.toEqual({
      'shard-0001': {
        checkpoint: '1',
        leaseExpiration: fiveMinsFromNow,
        version: '0000'
      }
    });
  });

  test('getShardAndStreamState returns the stream and shard state', async () => {
    const state = new StateStore(options);
    await state.start();
    const { get } = new DynamoDbClient();
    get.mockResolvedValueOnce({
      Item: {
        consumers: { foo: 'bar' },
        shards: {
          'shard-0001': { baz: 'qux' },
          'shard-0002': { quux: 'quuz' }
        }
      }
    });
    await expect(state.getShardAndStreamState('shard-0001', {})).resolves.toEqual({
      shardState: { baz: 'qux' },
      streamState: {
        consumers: { foo: 'bar' },
        shards: {
          'shard-0001': { baz: 'qux' },
          'shard-0002': { quux: 'quuz' }
        }
      }
    });
  });

  test('getShardAndStreamState recreates the shard state and returns the expected', async () => {
    const state = new StateStore(options);
    await state.start();
    const { get, update } = new DynamoDbClient();
    const data = {
      Item: {
        consumers: { foo: 'bar' },
        shards: {
          'shard-0001': { baz: 'qux' }
        }
      }
    };
    get.mockResolvedValueOnce(data);
    get.mockResolvedValueOnce(data);
    update.mockImplementationOnce(params => {
      const {
        ExpressionAttributeNames: { '#b': shardId },
        ExpressionAttributeValues: { ':x': updateData }
      } = params;
      data.Item.shards[shardId] = updateData;
      return Promise.resolve({});
    });
    await expect(
      state.getShardAndStreamState('shard-0002', { parent: 'shard-0001' })
    ).resolves.toEqual({
      shardState: {
        checkpoint: null,
        depleted: false,
        leaseExpiration: null,
        leaseOwner: null,
        parent: 'shard-0001',
        version: '0001'
      },
      streamState: {
        consumers: { foo: 'bar' },
        shards: {
          'shard-0001': { baz: 'qux' },
          'shard-0002': {
            checkpoint: null,
            depleted: false,
            leaseExpiration: null,
            leaseOwner: null,
            parent: 'shard-0001',
            version: '0001'
          }
        }
      }
    });
  });

  test('getShardsData returns the expected when using automatic shard distribution', async () => {
    const store = new StateStore(options);
    await store.start();
    const { get } = new DynamoDbClient();
    get.mockResolvedValueOnce({ Item: { shards: { foo: 'bar' } } });
    await expect(store.getShardsData()).resolves.toEqual({
      shards: { foo: 'bar' },
      shardsPath: '#a',
      shardsPathNames: {
        '#a': 'shards'
      }
    });
  });

  test('getShardsData returns the expected when reading from all shards', async () => {
    const store = new StateStore({ ...options, useAutoShardAssignment: false });
    await store.start();
    await expect(
      store.getShardsData({ consumers: { 'test-id': { shards: { foo: 'bar' } } } })
    ).resolves.toEqual({
      shards: { foo: 'bar' },
      shardsPath: '#a0.#a1.#a2',
      shardsPathNames: {
        '#a0': 'consumers',
        '#a1': 'test-id',
        '#a2': 'shards'
      }
    });
  });

  test('getShardsData returns the expected with all shards and enhanced consumers', async () => {
    const store = new StateStore({
      ...options,
      useAutoShardAssignment: false,
      useEnhancedFanOut: true
    });
    await store.start();
    await expect(
      store.getShardsData({
        enhancedConsumers: {
          'enhanced-consumer-1': {
            isUsedBy: 'test-id',
            shards: { foo: 'bar' }
          }
        }
      })
    ).resolves.toEqual({
      shards: { foo: 'bar' },
      shardsPath: '#a0.#a1.#a2',
      shardsPathNames: {
        '#a0': 'enhancedConsumers',
        '#a1': 'enhanced-consumer-1',
        '#a2': 'shards'
      }
    });
  });

  test('getShardsData throws when the enhanced consumers state is missing', async () => {
    const store = new StateStore({
      ...options,
      useAutoShardAssignment: false,
      useEnhancedFanOut: true
    });
    await store.start();
    await expect(store.getShardsData({ enhancedConsumers: {} })).rejects.toThrow(
      'The enhanced consumer state is not where expected.'
    );
  });

  test('lockShardLease locks a shard lease and returns true', async () => {
    const store = new StateStore(options);
    await store.start();
    const spy = jest
      .spyOn(Date, 'now')
      .mockImplementationOnce(() => new Date('2019-01-01').getTime());
    await expect(store.lockShardLease('shard-0001', 10000, '0000', {})).resolves.toBe(true);
    const { update } = new DynamoDbClient();
    expect(update).toHaveBeenCalledWith({
      ConditionExpression: '#a.#b.#e = :z',
      ExpressionAttributeNames: {
        '#a': 'shards',
        '#b': 'shard-0001',
        '#c': 'leaseOwner',
        '#d': 'leaseExpiration',
        '#e': 'version'
      },
      ExpressionAttributeValues: {
        ':w': 'test-id',
        ':x': '2019-01-01T00:00:10.000Z',
        ':y': '0001',
        ':z': '0000'
      },
      Key: { consumerGroup: 'test-group', streamName: 'test-stream' },
      UpdateExpression: 'SET #a.#b.#c = :w, #a.#b.#d = :x, #a.#b.#e = :y'
    });
    spy.mockRestore();
  });

  test('lockShardLease returns false if unable to acquire the lock', async () => {
    const store = new StateStore(options);
    await store.start();
    const { update } = new DynamoDbClient();
    update.mockRejectedValueOnce(
      Object.assign(new Error('foo'), { code: 'ConditionalCheckFailedException' })
    );
    await expect(store.lockShardLease('shard-0001', 10000, '0000', {})).resolves.toBe(false);
    expect(error).not.toHaveBeenCalled();
  });

  test('lockShardLease throws if DynamoDB throws', async () => {
    const store = new StateStore(options);
    await store.start();
    const { update } = new DynamoDbClient();
    update.mockRejectedValueOnce(new Error('foo'));
    await expect(store.lockShardLease('shard-0001', 10000, '0000', {})).rejects.toThrow('foo');
    expect(error).toHaveBeenNthCalledWith(1, expect.objectContaining({ message: 'foo' }));
    expect(error).toHaveBeenCalledTimes(1);
  });

  test("markShardAsDepleted marks a depletion and updates the children's checkpoints", async () => {
    const store = new StateStore(options);
    await store.start();
    const { get, update } = new DynamoDbClient();
    get.mockResolvedValueOnce({
      Item: { shards: { 'shard-0000': { checkpoint: '1' } } }
    });
    await expect(
      store.markShardAsDepleted(
        {
          'shard-0001': {
            parent: 'shard-0000',
            startingSequenceNumber: '2'
          }
        },
        'shard-0000'
      )
    ).resolves.toBeUndefined();
    expect(update).toHaveBeenNthCalledWith(1, {
      ConditionExpression: 'attribute_not_exists(#a.#b)',
      ExpressionAttributeNames: { '#a': 'shards', '#b': 'shard-0001' },
      ExpressionAttributeValues: {
        ':x': {
          checkpoint: null,
          depleted: false,
          leaseExpiration: null,
          leaseOwner: null,
          parent: 'shard-0000',
          version: '0001'
        }
      },
      Key: { consumerGroup: 'test-group', streamName: 'test-stream' },
      UpdateExpression: 'SET #a.#b = :x'
    });
    expect(update).toHaveBeenNthCalledWith(2, {
      ExpressionAttributeNames: {
        '#0': 'shard-0001',
        '#a': 'shards',
        '#b': 'shard-0000',
        '#c': 'depleted',
        '#d': 'version',
        '#e': 'checkpoint'
      },
      ExpressionAttributeValues: {
        ':0': '2',
        ':1': '0003',
        ':x': true,
        ':y': '0002'
      },
      Key: { consumerGroup: 'test-group', streamName: 'test-stream' },
      UpdateExpression: 'SET #a.#b.#c = :x, #a.#b.#d = :y, #a.#0.#e = :0, #a.#0.#d = :1'
    });
    expect(update).toHaveBeenCalledTimes(2);
  });

  test('markShardAsDepleted marks a depletion without passing an unset checkpoint', async () => {
    const store = new StateStore(options);
    await store.start();
    const { get, update } = new DynamoDbClient();
    get.mockResolvedValueOnce({
      Item: { shards: { 'shard-0000': { checkpoint: undefined } } }
    });
    await expect(
      store.markShardAsDepleted(
        {
          'shard-0001': {
            parent: 'shard-0000',
            startingSequenceNumber: '2'
          }
        },
        'shard-0000'
      )
    ).resolves.toBeUndefined();
    expect(update).toHaveBeenNthCalledWith(1, {
      ExpressionAttributeNames: {
        '#a': 'shards',
        '#b': 'shard-0000',
        '#c': 'depleted',
        '#d': 'version'
      },
      ExpressionAttributeValues: {
        ':x': true,
        ':y': '0001'
      },
      Key: { consumerGroup: 'test-group', streamName: 'test-stream' },
      UpdateExpression: 'SET #a.#b.#c = :x, #a.#b.#d = :y'
    });
    expect(update).toHaveBeenCalledTimes(1);
  });

  test('registerConsumer makes an update to add the current consumer to the state', async () => {
    const store = new StateStore(options);
    await store.start();
    await expect(store.registerConsumer()).resolves.toBeUndefined();
    const { update } = new DynamoDbClient();
    expect(update).toHaveBeenNthCalledWith(1, {
      ConditionExpression: 'attribute_not_exists(#a.#b)',
      ExpressionAttributeNames: {
        '#a': 'consumers',
        '#b': 'test-id'
      },
      ExpressionAttributeValues: {
        ':x': {
          appName: 'lifion-kinesis',
          heartbeat: expect.stringMatching(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/),
          host: expect.any(String),
          isActive: true,
          isStandalone: false,
          pid: expect.any(Number),
          startedOn: expect.stringMatching(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/)
        }
      },
      Key: {
        consumerGroup: 'test-group',
        streamName: 'test-stream'
      },
      UpdateExpression: 'SET #a.#b = :x'
    });
    expect(update).toHaveBeenCalledTimes(1);
    expect(debug).toHaveBeenNthCalledWith(2, 'The consumer "test-id" is now registered.');
    expect(debug).toHaveBeenCalledTimes(2);
  });

  test('registerConsumer makes an update to add a standalone consumer to the state', async () => {
    const store = new StateStore({ ...options, useAutoShardAssignment: false });
    await store.start();
    await expect(store.registerConsumer()).resolves.toBeUndefined();
    const { update } = new DynamoDbClient();
    expect(update).toHaveBeenNthCalledWith(1, {
      ConditionExpression: 'attribute_not_exists(#a.#b)',
      ExpressionAttributeNames: {
        '#a': 'consumers',
        '#b': 'test-id'
      },
      ExpressionAttributeValues: {
        ':x': {
          appName: 'lifion-kinesis',
          heartbeat: expect.stringMatching(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/),
          host: expect.any(String),
          isActive: true,
          isStandalone: true,
          pid: expect.any(Number),
          shards: {},
          startedOn: expect.stringMatching(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/)
        }
      },
      Key: {
        consumerGroup: 'test-group',
        streamName: 'test-stream'
      },
      UpdateExpression: 'SET #a.#b = :x'
    });
  });

  test('registerConsumer updates the hearbeat if the consumer is already registered', async () => {
    const store = new StateStore(options);
    await store.start();
    const { update } = new DynamoDbClient();
    update.mockRejectedValueOnce(
      Object.assign(new Error('foo'), { code: 'ConditionalCheckFailedException' })
    );
    update.mockRejectedValueOnce(
      Object.assign(new Error('foo'), { code: 'ConditionalCheckFailedException' })
    );
    await expect(store.registerConsumer()).resolves.toBeUndefined();
    expect(update).toHaveBeenNthCalledWith(2, {
      ExpressionAttributeNames: {
        '#a': 'consumers',
        '#b': 'test-id',
        '#c': 'heartbeat'
      },
      ExpressionAttributeValues: {
        ':x': expect.stringMatching(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/)
      },
      Key: {
        consumerGroup: 'test-group',
        streamName: 'test-stream'
      },
      UpdateExpression: 'SET #a.#b.#c = :x'
    });
    expect(update).toHaveBeenCalledTimes(2);
    expect(debug).toHaveBeenNthCalledWith(2, 'Missed heartbeat for "test-id".');
    expect(debug).toHaveBeenCalledTimes(2);
  });

  test('registerConsumer throws if DynamoDB throws', async () => {
    const store = new StateStore(options);
    await store.start();
    const { update } = new DynamoDbClient();
    update.mockRejectedValueOnce(new Error('foo'));
    await expect(store.registerConsumer()).rejects.toThrow('foo');
    expect(error).toHaveBeenNthCalledWith(1, expect.objectContaining({ message: 'foo' }));
    expect(error).toHaveBeenCalledTimes(1);
  });

  test('registerEnhancedConsumer registers enhanced consumers in the state', async () => {
    const store = new StateStore(options);
    await store.start();
    await expect(
      store.registerEnhancedConsumer('test-enhanced-consumer', 'arn:test-enhanced-consumer')
    ).resolves.toBeUndefined();
    const { update } = new DynamoDbClient();
    expect(update).toHaveBeenNthCalledWith(1, {
      ConditionExpression: 'attribute_not_exists(#a.#b)',
      ExpressionAttributeNames: { '#a': 'enhancedConsumers', '#b': 'test-enhanced-consumer' },
      ExpressionAttributeValues: {
        ':x': {
          arn: 'arn:test-enhanced-consumer',
          isStandalone: false,
          isUsedBy: null,
          version: '0001'
        }
      },
      Key: { consumerGroup: 'test-group', streamName: 'test-stream' },
      UpdateExpression: 'SET #a.#b = :x'
    });
    expect(update).toHaveBeenCalledTimes(1);
    expect(debug).toHaveBeenNthCalledWith(
      2,
      'The enhanced consumer "test-enhanced-consumer" is now registered.'
    );
    expect(debug).toHaveBeenCalledTimes(2);
  });

  test('registerEnhancedConsumer registers standalone enhanced consumers', async () => {
    const store = new StateStore({ ...options, useAutoShardAssignment: false });
    await store.start();
    await expect(
      store.registerEnhancedConsumer('test-enhanced-consumer', 'arn:test-enhanced-consumer')
    ).resolves.toBeUndefined();
    const { update } = new DynamoDbClient();
    expect(update).toHaveBeenNthCalledWith(1, {
      ConditionExpression: 'attribute_not_exists(#a.#b)',
      ExpressionAttributeNames: { '#a': 'enhancedConsumers', '#b': 'test-enhanced-consumer' },
      ExpressionAttributeValues: {
        ':x': {
          arn: 'arn:test-enhanced-consumer',
          isStandalone: true,
          isUsedBy: null,
          shards: {},
          version: '0001'
        }
      },
      Key: { consumerGroup: 'test-group', streamName: 'test-stream' },
      UpdateExpression: 'SET #a.#b = :x'
    });
    expect(update).toHaveBeenCalledTimes(1);
    expect(debug).toHaveBeenNthCalledWith(
      2,
      'The enhanced consumer "test-enhanced-consumer" is now registered.'
    );
    expect(debug).toHaveBeenCalledTimes(2);
  });

  test('registerEnhancedConsumer ignores conditional update mismatches', async () => {
    const store = new StateStore(options);
    await store.start();
    const { update } = new DynamoDbClient();
    update.mockRejectedValueOnce(
      Object.assign(new Error('foo'), { code: 'ConditionalCheckFailedException' })
    );
    await expect(
      store.registerEnhancedConsumer('test-enhanced-consumer', 'arn:test-enhanced-consumer')
    ).resolves.toBeUndefined();
    expect(error).not.toHaveBeenCalled();
  });

  test('registerEnhancedConsumer throws if DynamoDB throws', async () => {
    const store = new StateStore(options);
    await store.start();
    const { update } = new DynamoDbClient();
    update.mockRejectedValueOnce(new Error('foo'));
    await expect(
      store.registerEnhancedConsumer('test-enhanced-consumer', 'arn:test-enhanced-consumer')
    ).rejects.toThrow('foo');
    expect(error).toHaveBeenNthCalledWith(1, expect.objectContaining({ message: 'foo' }));
    expect(error).toHaveBeenCalledTimes(1);
  });

  test('releaseShardLease makes an update to release a shard lease', async () => {
    const store = new StateStore(options);
    await store.start();
    const { get, update } = new DynamoDbClient();
    get.mockResolvedValueOnce({ Item: {} });
    await expect(store.releaseShardLease('shard-0001', '0000', {})).resolves.toBe('0001');
    expect(update).toHaveBeenNthCalledWith(1, {
      ConditionExpression: '#a.#b.#e = :z',
      ExpressionAttributeNames: {
        '#a': 'shards',
        '#b': 'shard-0001',
        '#c': 'leaseOwner',
        '#d': 'leaseExpiration',
        '#e': 'version'
      },
      ExpressionAttributeValues: {
        ':w': null,
        ':x': null,
        ':y': '0001',
        ':z': '0000'
      },
      Key: {
        consumerGroup: 'test-group',
        streamName: 'test-stream'
      },
      UpdateExpression: 'SET #a.#b.#c = :w, #a.#b.#d = :x, #a.#b.#e = :y'
    });
    expect(update).toHaveBeenCalledTimes(1);
  });

  test('releaseShardLease ignores conditional update mismatches', async () => {
    const store = new StateStore(options);
    await store.start();
    const { get, update } = new DynamoDbClient();
    get.mockResolvedValueOnce({ Item: {} });
    update.mockRejectedValueOnce(
      Object.assign(new Error('foo'), { code: 'ConditionalCheckFailedException' })
    );
    await expect(store.releaseShardLease('shard-0001', '0000', {})).resolves.toBe(null);
    expect(update).toHaveBeenCalledTimes(1);
    expect(error).not.toHaveBeenCalled();
  });

  test('releaseShardLease throws when DynamoDB throws', async () => {
    const store = new StateStore(options);
    await store.start();
    const { get, update } = new DynamoDbClient();
    get.mockResolvedValueOnce({ Item: {} });
    update.mockRejectedValueOnce(new Error('foo'));
    await expect(store.releaseShardLease('shard-0001', '0000', {})).rejects.toThrow('foo');
    expect(update).toHaveBeenCalledTimes(1);
    expect(error).toHaveBeenCalledWith(expect.objectContaining({ message: 'foo' }));
  });

  test('storeShardCheckpoint throws if called with no checkpoint', async () => {
    const store = new StateStore(options);
    await store.start();
    await expect(
      store.storeShardCheckpoint('shard-0001', null, '#a', { '#a': 'a' })
    ).rejects.toThrow('The sequence number is required.');
  });

  test('storeShardCheckpoint updates the checkpoint of a shard', async () => {
    const store = new StateStore(options);
    await store.start();
    await expect(
      store.storeShardCheckpoint('shard-0001', '1', '#a', { '#a': 'a' })
    ).resolves.toBeUndefined();
    const { update } = new DynamoDbClient();
    expect(update).toHaveBeenNthCalledWith(1, {
      ExpressionAttributeNames: {
        '#a': 'a',
        '#b': 'shard-0001',
        '#c': 'checkpoint',
        '#d': 'version'
      },
      ExpressionAttributeValues: {
        ':x': '1',
        ':y': '0001'
      },
      Key: {
        consumerGroup: 'test-group',
        streamName: 'test-stream'
      },
      UpdateExpression: 'SET #a.#b.#c = :x, #a.#b.#d = :y'
    });
    expect(update).toHaveBeenCalledTimes(1);
  });

  test('storeShardCheckpoint throws if DynamoDB throws', async () => {
    const store = new StateStore(options);
    await store.start();
    const { update } = new DynamoDbClient();
    update.mockRejectedValueOnce(new Error('foo'));
    await expect(
      store.storeShardCheckpoint('shard-0001', '1', '#a', { '#a': 'a' })
    ).rejects.toThrow('foo');
  });
});
