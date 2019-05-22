'use strict';

const stream = require('./stream');

describe('lib/stream', () => {
  const {
    checkIfStreamExists,
    confirmStreamTags,
    ensureStreamEncription,
    ensureStreamExists
  } = stream;

  const debug = jest.fn();
  const errorMock = jest.fn();
  const logger = { debug, error: errorMock };

  const addTagsToStream = jest.fn();
  const createStream = jest.fn();
  const describeStream = jest.fn();
  const listStreamConsumers = jest.fn();
  const listTagsForStream = jest.fn();
  const startStreamEncryption = jest.fn();
  const waitFor = jest.fn();
  const client = {
    addTagsToStream,
    createStream,
    describeStream,
    listStreamConsumers,
    listTagsForStream,
    startStreamEncryption,
    waitFor
  };

  const commonParams = {
    client,
    createStreamIfNeeded: true,
    logger,
    shardCount: 1,
    streamName: 'foo'
  };

  afterEach(() => {
    addTagsToStream.mockClear();
    createStream.mockClear();
    debug.mockClear();
    describeStream.mockClear();
    errorMock.mockClear();
    listTagsForStream.mockClear();
    startStreamEncryption.mockClear();
    waitFor.mockClear();
  });

  test('the module exports the expected', () => {
    expect(stream).toEqual({
      checkIfStreamExists: expect.any(Function),
      confirmStreamTags: expect.any(Function),
      ensureStreamEncription: expect.any(Function),
      ensureStreamExists: expect.any(Function),
      getEnhancedConsumers: expect.any(Function),
      getStreamShards: expect.any(Function),
      registerEnhancedConsumer: expect.any(Function)
    });
  });

  test('checkIfStreamExists returns the ARN and the creation timestamp of a stream', async () => {
    describeStream.mockResolvedValueOnce({
      StreamDescription: {
        StreamARN: 'bar',
        StreamCreationTimestamp: new Date('2019-01-01'),
        StreamStatus: 'ACTIVE'
      }
    });
    await expect(checkIfStreamExists(commonParams)).resolves.toEqual({
      streamArn: 'bar',
      streamCreatedOn: '2019-01-01T00:00:00.000Z'
    });
    expect(describeStream).toHaveBeenCalledWith({ StreamName: 'foo' });
  });

  test("checkIfStreamExists returns a null ARN if the stream doesn't exist", async () => {
    const error = Object.assign(new Error('foo'), { code: 'ResourceNotFoundException' });
    describeStream.mockRejectedValueOnce(error);
    await expect(checkIfStreamExists(commonParams)).resolves.toEqual({ streamArn: null });
    expect(errorMock).not.toHaveBeenCalled();
  });

  test('checkIfStreamExists throws errors from the internal calls', async () => {
    const error = new Error('foo');
    describeStream.mockRejectedValueOnce(error);
    await expect(checkIfStreamExists(commonParams)).rejects.toBe(error);
    expect(errorMock).toHaveBeenCalledWith(error);
  });

  test('checkIfStreamExists waits until the stream is deleted before resolving', async () => {
    describeStream.mockResolvedValueOnce({
      StreamDescription: {
        StreamARN: 'bar',
        StreamCreationTimestamp: new Date('2019-01-01'),
        StreamStatus: 'DELETING'
      }
    });
    await expect(checkIfStreamExists(commonParams)).resolves.toEqual({ streamArn: null });
    expect(waitFor).toHaveBeenCalledWith('streamNotExists', { StreamName: 'foo' });
    expect(debug.mock.calls).toEqual([
      ['Waiting for the stream to complete deletion…'],
      ['The stream is now gone.']
    ]);
  });

  test('checkIfStreamExists resolves until the stream finishes updating', async () => {
    describeStream.mockResolvedValueOnce({
      StreamDescription: {
        StreamARN: 'bar',
        StreamCreationTimestamp: new Date('2019-01-01'),
        StreamStatus: 'UPDATING'
      }
    });
    await expect(checkIfStreamExists(commonParams)).resolves.toEqual({
      streamArn: 'bar',
      streamCreatedOn: '2019-01-01T00:00:00.000Z'
    });
    expect(waitFor).toHaveBeenCalledWith('streamExists', { StreamName: 'foo' });
    expect(debug.mock.calls).toEqual([
      ['Waiting for the stream to be active…'],
      ['The stream is now active.']
    ]);
  });

  test('confirmStreamTags merges existing tags before tagging a stream', async () => {
    listTagsForStream.mockResolvedValueOnce({ Tags: [{ Key: 'foo', Value: 'bar' }] });
    await expect(
      confirmStreamTags({ ...commonParams, tags: { baz: 'qux' } })
    ).resolves.toBeUndefined();
    expect(addTagsToStream).toHaveBeenCalledWith({
      StreamName: 'foo',
      Tags: { baz: 'qux', foo: 'bar' }
    });
    expect(debug.mock.calls).toEqual([['The stream tags have been updated.']]);
  });

  test("confirmStreamTags won't try to update the stream tags if not needed", async () => {
    listTagsForStream.mockResolvedValueOnce({ Tags: [{ Key: 'foo', Value: 'bar' }] });
    await expect(
      confirmStreamTags({ ...commonParams, tags: { foo: 'bar' } })
    ).resolves.toBeUndefined();
    expect(addTagsToStream).not.toHaveBeenCalled();
    expect(debug.mock.calls).toEqual([['The stream is already tagged as required.']]);
  });

  test('ensureStreamEncription will encrypt a non-encrypted stream', async () => {
    describeStream.mockResolvedValueOnce({ StreamDescription: { EncryptionType: 'NONE' } });
    await expect(
      ensureStreamEncription({ ...commonParams, encryption: { keyId: 'bar', type: 'baz' } })
    ).resolves.toBeUndefined();
    expect(startStreamEncryption).toHaveBeenCalledWith({
      EncryptionType: 'baz',
      KeyId: 'bar',
      StreamName: 'foo'
    });
    expect(debug.mock.calls).toEqual([
      ['Trying to encrypt the stream…'],
      ['Waiting for the stream to update…'],
      ['The stream is now encrypted.']
    ]);
  });

  test("ensureStreamEncription won't try to encrypt an already encrypted stream", async () => {
    describeStream.mockResolvedValueOnce({ StreamDescription: { EncryptionType: 'bar' } });
    await expect(
      ensureStreamEncription({ ...commonParams, encryption: { keyId: 'baz', type: 'qux' } })
    ).resolves.toBeUndefined();
    expect(startStreamEncryption).not.toHaveBeenCalled();
    expect(debug.mock.calls).toEqual([['The stream is already encrypted.']]);
  });

  test("ensureStreamExists won't try to create a stream if it already exists", async () => {
    describeStream.mockResolvedValueOnce({
      StreamDescription: {
        StreamARN: 'bar',
        StreamCreationTimestamp: new Date('2019-01-01'),
        StreamStatus: 'ACTIVE'
      }
    });
    await expect(ensureStreamExists(commonParams)).resolves.toEqual({
      streamArn: 'bar',
      streamCreatedOn: '2019-01-01T00:00:00.000Z'
    });
    expect(debug.mock.calls).toEqual([
      ['Verifying the "foo" stream exists and it\'s active…'],
      ["The stream exists and it's active."]
    ]);
  });

  test("ensureStreamExists tries to create a stream if it doesn't exists", async () => {
    describeStream.mockRejectedValueOnce(
      Object.assign(new Error('bar'), { code: 'ResourceNotFoundException' })
    );
    waitFor.mockResolvedValueOnce({
      StreamDescription: { StreamARN: 'baz', StreamCreationTimestamp: new Date('2019-01-01') }
    });
    await expect(ensureStreamExists(commonParams)).resolves.toEqual({
      streamArn: 'baz',
      streamCreatedOn: '2019-01-01T00:00:00.000Z'
    });
    expect(createStream).toHaveBeenCalledWith({ ShardCount: 1, StreamName: 'foo' });
    expect(waitFor).toHaveBeenCalledWith('streamExists', { StreamName: 'foo' });
    expect(debug.mock.calls).toEqual([
      ['Verifying the "foo" stream exists and it\'s active…'],
      ['Trying to create the stream…'],
      ['Waiting for the new stream to be active…'],
      ['The new stream is now active.']
    ]);
  });

  test("ensureStreamExists won't create a stream if it doesn't exists when opted out", async () => {
    describeStream.mockRejectedValueOnce(
      Object.assign(new Error('bar'), { code: 'ResourceNotFoundException' })
    );
    waitFor.mockResolvedValueOnce({
      StreamDescription: { StreamARN: 'baz', StreamCreationTimestamp: new Date('2019-01-01') }
    });
    await expect(
      ensureStreamExists({ ...commonParams, createStreamIfNeeded: false })
    ).resolves.toEqual({ streamArn: null });
    expect(debug.mock.calls).toEqual([
      ['Verifying the "foo" stream exists and it\'s active…'],
      ["The stream exists and it's active."]
    ]);
  });
});
