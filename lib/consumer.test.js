'use strict';

const { Kinesis, mockClear, mockConsumers } = require('aws-sdk');
const consumer = require('./consumer');

describe('lib/consumer', () => {
  let client;
  let logger;
  let ctx;

  beforeEach(() => {
    jest.useFakeTimers();
    client = new Kinesis();
    logger = { debug: jest.fn(), error: jest.fn() };
    ctx = {
      client,
      consumerName: 'foo',
      logger,
      streamArn: 'bar',
      streamName: 'baz'
    };
  });

  afterEach(() => {
    mockClear();
  });

  test('the module exports the expected', () => {
    expect(consumer).toEqual({
      activate: expect.any(Function)
    });
  });

  test("activate registers a consumer and return its ARN if it doesn't exists", async () => {
    await expect(consumer.activate(ctx)).resolves.toMatch(/^arn:aws:kinesis/);
    expect(client.registerStreamConsumer).toHaveBeenCalledWith({
      ConsumerName: 'foo',
      StreamARN: 'bar'
    });
    expect(logger.debug.mock.calls).toEqual([
      ['Checking if the "foo" consumer for "baz" exists…'],
      ['Trying to register the consumer…'],
      ['Waiting until the stream consumer is active…'],
      ['The stream consumer is now active.']
    ]);
  });

  test("activate doesn't tries to register for an already active consumer", async () => {
    const mockConsumer = {
      ConsumerARN: 'qux',
      ConsumerName: 'foo',
      ConsumerStatus: 'ACTIVE'
    };
    mockConsumers().push(mockConsumer);
    setTimeout.mockImplementationOnce(callback => {
      mockConsumer.ConsumerStatus = 'ACTIVE';
      callback();
    });
    await expect(consumer.activate(ctx)).resolves.toBe('qux');
    expect(client.registerStreamConsumer).not.toBeCalled();
    expect(logger.debug.mock.calls).toEqual([
      ['Checking if the "foo" consumer for "baz" exists…'],
      ['The stream consumer exists already and is active.']
    ]);
  });

  test("activate waits for a consumer if it's in creating state", async () => {
    const mockConsumer = {
      ConsumerARN: 'qux',
      ConsumerName: 'foo',
      ConsumerStatus: 'CREATING'
    };
    mockConsumers().push(mockConsumer);
    setTimeout.mockImplementationOnce(callback => {
      mockConsumer.ConsumerStatus = 'ACTIVE';
      callback();
    });
    await expect(consumer.activate(ctx)).resolves.toBe('qux');
    expect(client.registerStreamConsumer).not.toBeCalled();
    expect(logger.debug.mock.calls).toEqual([
      ['Checking if the "foo" consumer for "baz" exists…'],
      ['Waiting until the stream consumer is active…'],
      ['The stream consumer is now active.']
    ]);
  });

  test('activate throws if waiting too long for a consumer that is in creating state', async () => {
    const mockConsumer = {
      ConsumerARN: 'qux',
      ConsumerName: 'foo',
      ConsumerStatus: 'CREATING'
    };
    mockConsumers().push(mockConsumer);
    setTimeout.mockImplementation(callback => callback());
    await expect(consumer.activate(ctx)).rejects.toThrow(
      'Consumer "foo" exceeded the maximum wait time for activation.'
    );
    expect(logger.debug.mock.calls).toEqual([
      ['Checking if the "foo" consumer for "baz" exists…'],
      ['Waiting until the stream consumer is active…']
    ]);
    expect(logger.error.mock.calls).toEqual([
      ['Consumer "foo" exceeded the maximum wait time for activation.']
    ]);
  });

  test("activate waits for a consumer if it's in deleting state before creating one", async () => {
    const mockConsumer = {
      ConsumerARN: 'qux',
      ConsumerName: 'foo',
      ConsumerStatus: 'DELETING'
    };
    mockConsumers().push(mockConsumer);
    setTimeout.mockImplementationOnce(callback => {
      mockConsumers().length = 0;
      callback();
    });
    await expect(consumer.activate(ctx)).resolves.toMatch(/^arn:aws:kinesis/);
    expect(client.registerStreamConsumer).toHaveBeenCalledWith({
      ConsumerName: 'foo',
      StreamARN: 'bar'
    });
    expect(logger.debug.mock.calls).toEqual([
      ['Checking if the "foo" consumer for "baz" exists…'],
      ['Waiting for the stream consumer to complete deletion…'],
      ['The stream consumer is now gone.'],
      ['Trying to register the consumer…'],
      ['Waiting until the stream consumer is active…'],
      ['The stream consumer is now active.']
    ]);
  });

  test('activate throws if waiting too long for a consumer that is in deleting state', async () => {
    const mockConsumer = {
      ConsumerARN: 'qux',
      ConsumerName: 'foo',
      ConsumerStatus: 'DELETING'
    };
    mockConsumers().push(mockConsumer);
    setTimeout.mockImplementation(callback => callback());
    await expect(consumer.activate(ctx)).rejects.toThrow(
      'Consumer "foo" exceeded the maximum wait time for deletion.'
    );
    expect(logger.debug.mock.calls).toEqual([
      ['Checking if the "foo" consumer for "baz" exists…'],
      ['Waiting for the stream consumer to complete deletion…']
    ]);
    expect(logger.error.mock.calls).toEqual([
      ['Consumer "foo" exceeded the maximum wait time for deletion.']
    ]);
  });
});
