import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest';

import ConsumersManager from './consumers-manager';
import FanOutConsumer from './fan-out-consumer';
import PollingConsumer from './polling-consumer';

vi.mock('./fan-out-consumer', () => {
  const start = vi.fn();
  const stop = vi.fn();
  const MockFanOutConsumer = vi.fn(function () {
    return { start, stop };
  });
  return {
    default: Object.assign(MockFanOutConsumer, {
      clearMocks: () => {
        start.mockClear();
        stop.mockClear();
        MockFanOutConsumer.mockClear();
      },
      getMocks: () => ({ start, stop })
    })
  };
});

vi.mock('./polling-consumer', () => {
  const start = vi.fn();
  const stop = vi.fn();
  const updateLeaseExpiration = vi.fn();
  const MockPollingConsumer = vi.fn(function () {
    return { start, stop, updateLeaseExpiration };
  });
  return {
    default: Object.assign(MockPollingConsumer, {
      clearMocks: () => {
        start.mockClear();
        stop.mockClear();
        updateLeaseExpiration.mockClear();
        MockPollingConsumer.mockClear();
      },
      getMocks: () => ({ start, stop, updateLeaseExpiration })
    })
  };
});

describe('lib/consumers-manager', () => {
  let assignedEnhancedConsumer;
  let manager;
  let ownedShards;

  const logger = { debug: vi.fn(), error: vi.fn() };
  const stateStore = {
    getAssignedEnhancedConsumer: vi.fn(() => assignedEnhancedConsumer),
    getOwnedShards: vi.fn(() => ownedShards)
  };

  beforeEach(() => {
    assignedEnhancedConsumer = 'foo';
    ownedShards = { foo: { leaseExpiration: null } };
  });

  afterEach(() => {
    FanOutConsumer.clearMocks();
    PollingConsumer.clearMocks();
    logger.debug.mockClear();
    logger.error.mockClear();
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

    const { debug } = logger;
    expect(debug).toHaveBeenNthCalledWith(1, 'Reconciling shard consumers…');
    expect(debug).toHaveBeenNthCalledWith(2, 'Starting a consumer for "foo"…');
    expect(debug).toHaveBeenCalledTimes(2);
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

    const { debug } = logger;
    expect(debug).toHaveBeenNthCalledWith(1, 'Reconciling shard consumers…');
    expect(debug).toHaveBeenNthCalledWith(2, 'Starting a consumer for "foo"…');
    expect(debug).toHaveBeenNthCalledWith(3, 'Reconciling shard consumers…');
    expect(debug).toHaveBeenCalledTimes(3);
  });

  test('the manager stops consumers for no longer owned shards', async () => {
    manager = new ConsumersManager({ logger, stateStore });
    await manager.reconcile();
    PollingConsumer.clearMocks();
    ownedShards = {};
    await manager.reconcile();
    await manager.reconcile();

    expect(PollingConsumer).not.toHaveBeenCalled();
    expect(PollingConsumer.getMocks().stop).toHaveBeenCalled();

    const { debug } = logger;
    expect(debug).toHaveBeenNthCalledWith(1, 'Reconciling shard consumers…');
    expect(debug).toHaveBeenNthCalledWith(2, 'Starting a consumer for "foo"…');
    expect(debug).toHaveBeenNthCalledWith(3, 'Reconciling shard consumers…');
    expect(debug).toHaveBeenNthCalledWith(4, 'Stopping the consumer for "foo"…');
    expect(debug).toHaveBeenNthCalledWith(5, 'Reconciling shard consumers…');
    expect(debug).toHaveBeenCalledTimes(5);
  });

  test('the manager recovers from errors when stopping consumers', async () => {
    manager = new ConsumersManager({ logger, stateStore });
    await manager.reconcile();
    PollingConsumer.clearMocks();
    ownedShards = {};

    PollingConsumer.getMocks().stop.mockImplementationOnce(() => {
      throw new Error('foo');
    });

    await expect(manager.reconcile()).resolves.toBeUndefined();

    const { error } = logger;
    expect(error).toHaveBeenNthCalledWith(
      1,
      'Unexpected recoverable failure when trying to stop a consumer:',
      expect.objectContaining({ message: 'foo' })
    );
    expect(error).toHaveBeenCalledTimes(1);
  });

  test('reconcile throws when starting consumers throws', async () => {
    manager = new ConsumersManager({ logger, stateStore });

    PollingConsumer.getMocks().start.mockImplementationOnce(() => {
      throw new Error('foo');
    });

    await expect(manager.reconcile()).rejects.toEqual(expect.objectContaining({ message: 'foo' }));

    const { error } = logger;
    expect(error).toHaveBeenNthCalledWith(
      1,
      'Unexpected recoverable error when trying to start a consumer:',
      expect.objectContaining({ message: 'foo' })
    );
    expect(error).toHaveBeenCalledTimes(1);
  });

  test('the manager is able to instantiate and start fan-out consumers as needed', async () => {
    manager = new ConsumersManager({ logger, stateStore, useEnhancedFanOut: true });
    await manager.reconcile();

    expect(FanOutConsumer).toHaveBeenCalledWith(expect.objectContaining({ shardId: 'foo' }));
    expect(FanOutConsumer.getMocks().start).toHaveBeenCalled();

    const { debug } = logger;
    expect(debug).toHaveBeenNthCalledWith(1, 'Reconciling shard consumers…');
    expect(debug).toHaveBeenNthCalledWith(2, 'Starting a consumer for "foo"…');
    expect(debug).toHaveBeenCalledTimes(2);
  });

  test('all fan-out consumers stop if the assigned enhanced consumer is lost', async () => {
    manager = new ConsumersManager({ logger, stateStore, useEnhancedFanOut: true });
    await manager.reconcile();
    FanOutConsumer.clearMocks();
    assignedEnhancedConsumer = null;
    await manager.reconcile();

    expect(FanOutConsumer).not.toHaveBeenCalled();
    expect(FanOutConsumer.getMocks().stop).toHaveBeenCalled();

    const { debug } = logger;
    expect(debug).toHaveBeenNthCalledWith(1, 'Reconciling shard consumers…');
    expect(debug).toHaveBeenNthCalledWith(2, 'Starting a consumer for "foo"…');
    expect(debug).toHaveBeenNthCalledWith(3, 'Reconciling shard consumers…');
    expect(debug).toHaveBeenNthCalledWith(4, 'Stopping the consumer for "foo"…');
    expect(debug).toHaveBeenCalledTimes(4);
  });

  test('the manager stops all consumers when asked to stop', async () => {
    manager = new ConsumersManager({ logger, stateStore });
    await manager.reconcile();
    manager.stop();
    expect(PollingConsumer.getMocks().stop).toHaveBeenCalled();
  });

  test('stopping ignores shard slots already cleared by a hand-off', async () => {
    manager = new ConsumersManager({ logger, stateStore });
    await manager.reconcile();
    ownedShards = {};
    await manager.reconcile();
    expect(() => manager.stop()).not.toThrow();
  });
});
