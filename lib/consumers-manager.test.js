'use strict';

const ConsumersManager = require('./consumers-manager');
const FanOutConsumer = require('./fan-out-consumer');
const PollingConsumer = require('./polling-consumer');

jest.mock('./fan-out-consumer', () => {
  const start = jest.fn();
  const stop = jest.fn();
  const MockFanOutConsumer = jest.fn(() => ({ start, stop }));
  return Object.assign(MockFanOutConsumer, {
    clearMocks: () => {
      start.mockClear();
      stop.mockClear();
      MockFanOutConsumer.mockClear();
    },
    getMocks: () => ({ start, stop })
  });
});

jest.mock('./polling-consumer', () => {
  const start = jest.fn();
  const stop = jest.fn();
  const updateLeaseExpiration = jest.fn();
  const MockPollingConsumer = jest.fn(() => ({ start, stop, updateLeaseExpiration }));
  return Object.assign(MockPollingConsumer, {
    clearMocks: () => {
      start.mockClear();
      stop.mockClear();
      updateLeaseExpiration.mockClear();
      MockPollingConsumer.mockClear();
    },
    getMocks: () => ({ start, stop, updateLeaseExpiration })
  });
});

describe('lib/consumers-manager', () => {
  let assignedEnhancedConsumer;
  let manager;
  let ownedShards;

  const logger = { debug: jest.fn() };
  const stateStore = {
    getAssignedEnhancedConsumer: jest.fn(() => assignedEnhancedConsumer),
    getOwnedShards: jest.fn(() => ownedShards)
  };

  beforeEach(() => {
    assignedEnhancedConsumer = 'foo';
    ownedShards = { foo: { leaseExpiration: null } };
  });

  afterEach(() => {
    FanOutConsumer.clearMocks();
    PollingConsumer.clearMocks();
    logger.debug.mockClear();
    stateStore.getAssignedEnhancedConsumer.mockClear();
    stateStore.getOwnedShards.mockClear();
  });

  test('the module exports the expected', () => {
    expect(ConsumersManager).toEqual(expect.any(Function));
    expect(ConsumersManager).toThrow('Class constructor');
  });

  test('the manager is able to instantiate and start polling consumers as needed', async () => {
    manager = new ConsumersManager({ logger, stateStore });
    await manager.reconcile();

    expect(PollingConsumer).toHaveBeenCalledWith(expect.objectContaining({ shardId: 'foo' }));
    expect(PollingConsumer.getMocks().start).toHaveBeenCalled();

    expect(logger.debug.mock.calls).toEqual([
      ['Reconciling shard consumers…'],
      ['Starting a consumer for "foo"…']
    ]);
  });

  test('the lease expiration is updated if a polling consumer is already running', async () => {
    manager = new ConsumersManager({ logger, stateStore });
    ownedShards.foo.leaseExpiration = 1;
    await manager.reconcile();

    expect(PollingConsumer).toHaveBeenCalledWith(expect.objectContaining({ leaseExpiration: 1 }));
    expect(PollingConsumer.getMocks().start).toHaveBeenCalled();

    PollingConsumer.clearMocks();
    ownedShards.foo.leaseExpiration = 2;
    await manager.reconcile();

    expect(PollingConsumer).not.toHaveBeenCalled();
    expect(PollingConsumer.getMocks().start).not.toHaveBeenCalled();
    expect(PollingConsumer.getMocks().updateLeaseExpiration).toHaveBeenCalledWith(2);

    expect(logger.debug.mock.calls).toEqual([
      ['Reconciling shard consumers…'],
      ['Starting a consumer for "foo"…'],
      ['Reconciling shard consumers…']
    ]);
  });

  test('the manager should stop consumers for no longer owned shards', async () => {
    manager = new ConsumersManager({ logger, stateStore });
    await manager.reconcile();
    PollingConsumer.clearMocks();
    ownedShards = {};
    await manager.reconcile();
    await manager.reconcile();

    expect(PollingConsumer).not.toHaveBeenCalled();
    expect(PollingConsumer.getMocks().stop).toHaveBeenCalled();

    expect(logger.debug.mock.calls).toEqual([
      ['Reconciling shard consumers…'],
      ['Starting a consumer for "foo"…'],
      ['Reconciling shard consumers…'],
      ['Stopping the consumer for "foo"…'],
      ['Reconciling shard consumers…']
    ]);
  });

  test('the manager is able to instantiate and start fan-out consumers as needed', async () => {
    manager = new ConsumersManager({ logger, stateStore, useEnhancedFanOut: true });
    await manager.reconcile();

    expect(FanOutConsumer).toHaveBeenCalledWith(expect.objectContaining({ shardId: 'foo' }));
    expect(FanOutConsumer.getMocks().start).toHaveBeenCalled();

    expect(logger.debug.mock.calls).toEqual([
      ['Reconciling shard consumers…'],
      ['Starting a consumer for "foo"…']
    ]);
  });

  test('all fan-out consumers should stop if the assigned enhanced consumer is lost', async () => {
    manager = new ConsumersManager({ logger, stateStore, useEnhancedFanOut: true });
    await manager.reconcile();
    FanOutConsumer.clearMocks();
    assignedEnhancedConsumer = null;
    await manager.reconcile();

    expect(FanOutConsumer).not.toHaveBeenCalled();
    expect(FanOutConsumer.getMocks().stop).toHaveBeenCalled();

    expect(logger.debug.mock.calls).toEqual([
      ['Reconciling shard consumers…'],
      ['Starting a consumer for "foo"…'],
      ['Reconciling shard consumers…'],
      ['Stopping the consumer for "foo"…']
    ]);
  });
});
