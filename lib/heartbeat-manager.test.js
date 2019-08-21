'use strict';

const HeartbeatManager = require('./heartbeat-manager');

describe('lib/heartbeat-manager', () => {
  const debug = jest.fn();
  const error = jest.fn();
  const logger = { debug, error };
  const clearOldConsumers = jest.fn();
  const registerConsumer = jest.fn();
  const stateStore = { clearOldConsumers, registerConsumer };

  function nextTickWait() {
    return new Promise(resolve => setImmediate(resolve));
  }

  beforeEach(() => {
    jest.useFakeTimers();
  });

  afterEach(() => {
    debug.mockClear();
    error.mockClear();
    clearOldConsumers.mockClear();
    registerConsumer.mockClear();
  });

  test('the module exports the expected', () => {
    expect(HeartbeatManager).toEqual(expect.any(Function));
    expect(HeartbeatManager).toThrow('Class constructor');
  });

  test('the heartbeat manager does the expected after starting it', async () => {
    const manager = new HeartbeatManager({ logger, stateStore });
    await manager.start();
    jest.runOnlyPendingTimers();
    await nextTickWait();
    expect(clearOldConsumers).toHaveBeenCalledWith(expect.any(Number));
    expect(clearOldConsumers).toHaveBeenCalledTimes(2);
    expect(registerConsumer).toHaveBeenCalled();
    expect(registerConsumer).toHaveBeenCalledTimes(2);
    expect(debug).toHaveBeenCalled();
    expect(debug).toHaveBeenCalledTimes(2);
    manager.stop();
  });

  test('the heartbeat manager can be started multiple times without problems', async () => {
    const manager = new HeartbeatManager({ logger, stateStore });
    await manager.start();
    await manager.start();
    jest.runOnlyPendingTimers();
    await nextTickWait();
    expect(clearOldConsumers).toHaveBeenCalledTimes(2);
    expect(registerConsumer).toHaveBeenCalledTimes(2);
    expect(debug).toHaveBeenCalledTimes(2);
    manager.stop();
  });

  test('the heartbeat manager can be stopped', async () => {
    const manager = new HeartbeatManager({ logger, stateStore });
    await manager.start();
    manager.stop();
    jest.runOnlyPendingTimers();
    await nextTickWait();
    expect(clearOldConsumers).toHaveBeenCalledTimes(1);
    expect(registerConsumer).toHaveBeenCalledTimes(1);
    expect(debug).toHaveBeenCalledTimes(1);
  });

  test('the heartbeat manager can be stopped multiple times without problems', async () => {
    const manager = new HeartbeatManager({ logger, stateStore });
    await manager.start();
    manager.stop();
    manager.stop();
    jest.runOnlyPendingTimers();
    await nextTickWait();
    expect(clearOldConsumers).toHaveBeenCalledTimes(1);
    expect(registerConsumer).toHaveBeenCalledTimes(1);
    expect(debug).toHaveBeenCalledTimes(1);
  });

  test('the heartbeat manager can resume after being stopped', async () => {
    const manager = new HeartbeatManager({ logger, stateStore });
    await manager.start();
    manager.stop();
    await manager.start();
    jest.runOnlyPendingTimers();
    await nextTickWait();
    expect(clearOldConsumers).toHaveBeenCalledTimes(3);
    expect(registerConsumer).toHaveBeenCalledTimes(3);
    expect(debug).toHaveBeenCalledTimes(3);
    manager.stop();
  });

  test('the heartbeat manager can recover from thrown errors', async () => {
    const manager = new HeartbeatManager({ logger, stateStore });
    registerConsumer.mockRejectedValueOnce(new Error('foo'));
    await expect(manager.start()).resolves.toBeUndefined();
    expect(error).toHaveBeenNthCalledWith(
      1,
      'Unexpected recoverable failure when trying to send a hearbeat:',
      expect.objectContaining({ message: 'foo' })
    );
    expect(error).toHaveBeenCalledTimes(1);
  });
});
