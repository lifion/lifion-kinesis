'use strict';

const { Kinesis, mockClear, mockStreams } = require('aws-sdk');
const stream = require('./stream');

describe('lib/stream', () => {
  let client;
  let logger;
  let ctx;

  beforeEach(() => {
    client = new Kinesis();
    logger = { debug: jest.fn(), error: jest.fn() };
    ctx = {
      createStreamIfNeeded: true,
      client,
      logger,
      shardCount: 1,
      streamName: 'foo'
    };
  });

  afterEach(() => {
    mockClear();
  });

  test('the module exports the expected', () => {
    expect(stream).toEqual({
      activate: expect.any(Function),
      encrypt: expect.any(Function),
      tag: expect.any(Function)
    });
  });

  test("activate creates a stream if it's doesn't exists and auto-create is on", async () => {
    await expect(stream.activate(ctx)).resolves.toMatch(/^arn:aws:kinesis/);
    expect(client.createStream).toBeCalledWith({ ShardCount: 1, StreamName: 'foo' });
    expect(logger.debug.mock.calls).toEqual([
      ['Checking if the stream "foo" exists…'],
      ['Trying to create the stream…'],
      ['Waiting for the new stream to be active…'],
      ['The new stream is now active.']
    ]);
  });

  test("activate throws if a stream doesn't exists and auto-create is off", async () => {
    ctx.createStreamIfNeeded = false;
    await expect(stream.activate(ctx)).rejects.toThrow("The stream doesn't exists.");
    expect(client.createStream).not.toBeCalledWith();
    expect(logger.debug.mock.calls).toEqual([['Checking if the stream "foo" exists…']]);
    const [[{ code, message }]] = logger.error.mock.calls;
    expect(code).toBe('ResourceNotFoundException');
    expect(message).toBe("The stream doesn't exists.");
  });

  test("activate won't try to create a stream if it exists already", async () => {
    const mockStream = { StreamARN: 'bar', StreamName: 'foo', StreamStatus: 'ACTIVE' };
    mockStreams().push(mockStream);
    await expect(stream.activate(ctx)).resolves.toBe('bar');
    expect(client.createStream).not.toBeCalledWith();
    expect(logger.debug.mock.calls).toEqual([
      ['Checking if the stream "foo" exists…'],
      ['The stream status is ACTIVE.']
    ]);
  });

  test("activate waits for a stream if it's in creating state", async () => {
    const mockStream = { StreamARN: 'bar', StreamName: 'foo', StreamStatus: 'CREATING' };
    mockStreams().push(mockStream);
    await expect(stream.activate(ctx)).resolves.toMatch('bar');
    expect(client.createStream).not.toBeCalled();
    expect(logger.debug.mock.calls).toEqual([
      ['Checking if the stream "foo" exists…'],
      ['The stream status is CREATING.'],
      ['Waiting for the stream to be active…'],
      ['The stream is now active.']
    ]);
  });

  test('activate waits for a stream in deleting state before trying to create it', async () => {
    const mockStream = { StreamARN: 'bar', StreamName: 'foo', StreamStatus: 'DELETING' };
    mockStreams().push(mockStream);
    await expect(stream.activate(ctx)).resolves.toMatch('bar');
    expect(client.createStream).toBeCalledWith({ ShardCount: 1, StreamName: 'foo' });
    expect(logger.debug.mock.calls).toEqual([
      ['Checking if the stream "foo" exists…'],
      ['The stream status is DELETING.'],
      ['Waiting for the stream to complete deletion…'],
      ['The stream is now gone.'],
      ['Trying to create the stream…'],
      ['Waiting for the new stream to be active…'],
      ['The new stream is now active.']
    ]);
  });

  test('encrypt will start the stream encryption if not previously encrypted', async () => {
    ctx.encryption = { type: 'baz', keyId: 'qux' };
    const mockStream = { StreamName: 'foo', StreamStatus: 'ACTIVE', EncryptionType: 'NONE' };
    mockStreams().push(mockStream);
    await expect(stream.encrypt(ctx)).resolves.not.toBeDefined();
    expect(client.startStreamEncryption).toBeCalledWith({
      EncryptionType: 'baz',
      KeyId: 'qux',
      StreamName: 'foo'
    });
    expect(logger.debug.mock.calls).toEqual([
      ['Checking if the stream "foo" is encrypted…'],
      ['Trying to encrypt the stream…'],
      ['Waiting for the stream to update…'],
      ['The stream is now encrypted.']
    ]);
  });

  test("encrypt won't try to  encrypt the stream if it's already encrypted", async () => {
    ctx.encryption = { type: 'baz', keyId: 'qux' };
    const mockStream = { StreamName: 'foo', StreamStatus: 'ACTIVE', EncryptionType: 'KMS' };
    mockStreams().push(mockStream);
    await expect(stream.encrypt(ctx)).resolves.not.toBeDefined();
    expect(client.startStreamEncryption).not.toBeCalled();
    expect(logger.debug.mock.calls).toEqual([
      ['Checking if the stream "foo" is encrypted…'],
      ['The stream is encrypted.']
    ]);
  });

  test("tag updates the stream if it's not already tagged", async () => {
    ctx.tags = { baz: 'qux', quux: 'quuz', corge: 'grault' };
    const mockStream = {
      StreamName: 'foo',
      StreamStatus: 'ACTIVE'
    };
    mockStreams().push(mockStream);
    await expect(stream.tag(ctx)).resolves.not.toBeDefined();
    expect(client.addTagsToStream).toBeCalledWith({
      StreamName: 'foo',
      Tags: { baz: 'qux', corge: 'grault', quux: 'quuz' }
    });
    expect(logger.debug.mock.calls).toEqual([
      ['Checking if the stream "foo" is already tagged…'],
      ['The stream tags have been updated.']
    ]);
  });

  test('tag updates the stream with the previous and new tags', async () => {
    ctx.tags = { corge: 'grault' };
    const mockStream = {
      StreamName: 'foo',
      StreamStatus: 'ACTIVE',
      Tags: [{ Key: 'baz', Value: 'qux' }]
    };
    mockStreams().push(mockStream);
    await stream.tag(ctx);
    expect(client.addTagsToStream).toBeCalledWith({
      StreamName: 'foo',
      Tags: { baz: 'qux', corge: 'grault' }
    });
    expect(logger.debug.mock.calls).toEqual([
      ['Checking if the stream "foo" is already tagged…'],
      ['The stream tags have been updated.']
    ]);
  });

  test("tag won't update the stream if the previous and new tags are the same", async () => {
    ctx.tags = { baz: 'qux', quux: 'quuz', corge: 'grault' };
    const mockStream = {
      StreamName: 'foo',
      StreamStatus: 'ACTIVE',
      Tags: [
        { Key: 'baz', Value: 'qux' },
        { Key: 'quux', Value: 'quuz' },
        { Key: 'corge', Value: 'grault' }
      ]
    };
    mockStreams().push(mockStream);
    await stream.tag(ctx);
    expect(client.addTagsToStream).not.toBeCalled();
    expect(logger.debug.mock.calls).toEqual([
      ['Checking if the stream "foo" is already tagged…'],
      ['The stream is already tagged as required.']
    ]);
  });
});
