'use strict';

const { Kinesis: AwsKinesis } = require('aws-sdk');
const Kinesis = require('.');
const consumer = require('./consumer');
const stream = require('./stream');
const utils = require('./utils');

jest.mock('./consumer');
jest.mock('./stream');
jest.mock('./utils');
jest.mock('./shard-subscriber');

describe('lib/index', () => {
  const consumerName = 'foo';
  const streamName = 'bar';

  afterEach(() => {
    AwsKinesis.mockClear();
    consumer.activate.mockClear();
    stream.activate.mockClear();
    stream.encrypt.mockClear();
    stream.getShards.mockClear();
    stream.tag.mockClear();
    utils.noop.mockClear();
  });

  test('the module exports the expected', () => {
    expect(Kinesis).toEqual(expect.any(Function));
    expect(Kinesis).toThrow("Class constructor Kinesis cannot be invoked without 'new'");
  });

  test('the constructor should throw if called without a "consumerName"', () => {
    expect(() => new Kinesis()).toThrow('The "consumerName" option is required.');
  });

  test('the constructor should throw if called without a "streamName"', () => {
    expect(() => new Kinesis({ consumerName })).toThrow('The "streamName" option is required.');
  });

  test('the constructor should be able to initialize an instance', () => {
    let instance;
    expect(() => {
      instance = new Kinesis({ consumerName, streamName });
    }).not.toThrow();
    expect(instance).toEqual(expect.any(Kinesis));
  });

  test('connect should return a promise', async () => {
    const kinesis = new Kinesis({ consumerName, streamName });
    const promise = kinesis.connect();
    expect(promise).toEqual(expect.any(Promise));
    await expect(promise).resolves.not.toBeDefined();
  });

  test('connect should instantiate an AWS SDK Kinesis object', async () => {
    const kinesis = new Kinesis({ consumerName, streamName, foo: 'bar' });
    await kinesis.connect();
    expect(AwsKinesis).toHaveBeenCalledWith({ foo: 'bar' });
  });

  test('connect should activate a stream', async () => {
    const kinesis = new Kinesis({ consumerName, streamName });
    await kinesis.connect();
    expect(stream.activate).toHaveBeenCalledWith({
      client: expect.any(Object),
      compression: undefined,
      consumerArn: undefined,
      consumerName,
      createStreamIfNeeded: true,
      encryption: undefined,
      logger: expect.any(Object),
      options: {},
      shardCount: 1,
      shards: [],
      streamArn: undefined,
      streamName: 'bar',
      tags: undefined
    });
  });

  test('connect should use a noop logger when not provided one', async () => {
    const kinesis = new Kinesis({ consumerName, streamName });
    await kinesis.connect();
    const [[{ logger }]] = stream.activate.mock.calls;
    const { noop } = utils;
    expect(logger).toEqual({ debug: noop, error: noop, warn: noop });
    expect(noop.mock.calls).toEqual([
      ['Trying to connect the client…'],
      ['Creating subscribers for the stream shards using "foo"…'],
      ['The client is now connected.']
    ]);
  });

  test('connect should use the provided logger', async () => {
    const logger = { debug: jest.fn(), error: jest.fn(), warn: jest.fn() };
    await new Kinesis({ consumerName, streamName, logger }).connect();
    expect(utils.noop).not.toBeCalled();
    expect(logger.debug.mock.calls).toEqual([
      ['Trying to connect the client…'],
      ['Creating subscribers for the stream shards using "foo"…'],
      ['The client is now connected.']
    ]);
    expect(() => new Kinesis({ logger })).toThrow();
    expect(logger.error.mock.calls[0][0]).toEqual('The "consumerName" option is required.');
  });

  test('connect should encrypt a stream if provided with the encryption options', async () => {
    await new Kinesis({ consumerName, streamName, encryption: { foo: 'bar' } }).connect();
    expect(stream.encrypt).toHaveBeenCalled();
    const [[{ encryption }]] = stream.encrypt.mock.calls;
    expect(encryption).toEqual({ foo: 'bar' });
  });

  test('connect should tag a stream if provided with the tags option', async () => {
    await new Kinesis({ consumerName, streamName, tags: { foo: 'bar' } }).connect();
    expect(stream.tag).toHaveBeenCalled();
    const [[{ tags }]] = stream.tag.mock.calls;
    expect(tags).toEqual({ foo: 'bar' });
  });
});
