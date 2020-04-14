'use strict';

const { promisify } = require('util');
const stream = require('./stream');

const wait = promisify(setTimeout);

jest.mock('util', () => {
  const promisifiedMock = jest.fn();
  return { promisify: () => promisifiedMock };
});

describe('lib/stream', () => {
  const {
    checkIfStreamExists,
    confirmStreamTags,
    ensureStreamEncription,
    ensureStreamExists,
    getEnhancedConsumers,
    getStreamShards,
    registerEnhancedConsumer
  } = stream;

  const debug = jest.fn();
  const errorMock = jest.fn();
  const logger = { debug, error: errorMock };

  const addTagsToStream = jest.fn();
  const createStream = jest.fn();
  const describeStreamSummary = jest.fn();
  const listShards = jest.fn();
  const listStreamConsumers = jest.fn();
  const listTagsForStream = jest.fn();
  const registerStreamConsumer = jest.fn();
  const startStreamEncryption = jest.fn();
  const waitFor = jest.fn();
  const client = {
    addTagsToStream,
    createStream,
    describeStreamSummary,
    listShards,
    listStreamConsumers,
    listTagsForStream,
    registerStreamConsumer,
    startStreamEncryption,
    waitFor
  };

  const commonParams = {
    client,
    consumerName: 'test-consumer',
    createStreamIfNeeded: true,
    logger,
    shardCount: 1,
    streamArn: 'arn:test-stream',
    streamName: 'test-stream'
  };

  afterEach(() => {
    addTagsToStream.mockClear();
    createStream.mockClear();
    debug.mockClear();
    describeStreamSummary.mockClear();
    errorMock.mockClear();
    listShards.mockClear();
    listStreamConsumers.mockClear();
    listTagsForStream.mockClear();
    registerStreamConsumer.mockClear();
    startStreamEncryption.mockClear();
    wait.mockClear();
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
    describeStreamSummary.mockResolvedValueOnce({
      StreamDescriptionSummary: {
        StreamARN: 'foo',
        StreamCreationTimestamp: new Date('2019-01-01'),
        StreamStatus: 'ACTIVE'
      }
    });
    await expect(checkIfStreamExists(commonParams)).resolves.toEqual({
      streamArn: 'foo',
      streamCreatedOn: '2019-01-01T00:00:00.000Z'
    });
    expect(describeStreamSummary).toHaveBeenCalledWith({ StreamName: 'test-stream' });
  });

  test("checkIfStreamExists returns a null ARN if the stream doesn't exist", async () => {
    const error = Object.assign(new Error('foo'), { code: 'ResourceNotFoundException' });
    describeStreamSummary.mockRejectedValueOnce(error);
    await expect(checkIfStreamExists(commonParams)).resolves.toEqual({ streamArn: null });
    expect(errorMock).not.toHaveBeenCalled();
  });

  test('checkIfStreamExists throws errors from the internal calls', async () => {
    const error = new Error('foo');
    describeStreamSummary.mockRejectedValueOnce(error);
    await expect(checkIfStreamExists(commonParams)).rejects.toBe(error);
    expect(errorMock).toHaveBeenCalledWith(error);
  });

  test('checkIfStreamExists waits until the stream is deleted before resolving', async () => {
    describeStreamSummary.mockResolvedValueOnce({
      StreamDescriptionSummary: {
        StreamARN: 'foo',
        StreamCreationTimestamp: new Date('2019-01-01'),
        StreamStatus: 'DELETING'
      }
    });
    await expect(checkIfStreamExists(commonParams)).resolves.toEqual({ streamArn: null });
    expect(waitFor).toHaveBeenCalledWith('streamNotExists', { StreamName: 'test-stream' });
    expect(debug.mock.calls).toEqual([
      ['Waiting for the stream to complete deletion…'],
      ['The stream is now gone.']
    ]);
  });

  test('checkIfStreamExists resolves until the stream finishes updating', async () => {
    describeStreamSummary.mockResolvedValueOnce({
      StreamDescriptionSummary: {
        StreamARN: 'foo',
        StreamCreationTimestamp: new Date('2019-01-01'),
        StreamStatus: 'UPDATING'
      }
    });
    await expect(checkIfStreamExists(commonParams)).resolves.toEqual({
      streamArn: 'foo',
      streamCreatedOn: '2019-01-01T00:00:00.000Z'
    });
    expect(waitFor).toHaveBeenCalledWith('streamExists', { StreamName: 'test-stream' });
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
      StreamName: 'test-stream',
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
    describeStreamSummary.mockResolvedValueOnce({
      StreamDescriptionSummary: { EncryptionType: 'NONE' }
    });
    await expect(
      ensureStreamEncription({ ...commonParams, encryption: { keyId: 'foo', type: 'bar' } })
    ).resolves.toBeUndefined();
    expect(startStreamEncryption).toHaveBeenCalledWith({
      EncryptionType: 'bar',
      KeyId: 'foo',
      StreamName: 'test-stream'
    });
    expect(debug.mock.calls).toEqual([
      ['Trying to encrypt the stream…'],
      ['Waiting for the stream to update…'],
      ['The stream is now encrypted.']
    ]);
  });

  test("ensureStreamEncription won't try to encrypt an already encrypted stream", async () => {
    describeStreamSummary.mockResolvedValueOnce({
      StreamDescriptionSummary: { EncryptionType: 'foo' }
    });
    await expect(
      ensureStreamEncription({ ...commonParams, encryption: { keyId: 'bar', type: 'baz' } })
    ).resolves.toBeUndefined();
    expect(startStreamEncryption).not.toHaveBeenCalled();
    expect(debug.mock.calls).toEqual([['The stream is already encrypted.']]);
  });

  test("ensureStreamExists won't try to create a stream if it already exists", async () => {
    describeStreamSummary.mockResolvedValueOnce({
      StreamDescriptionSummary: {
        StreamARN: 'foo',
        StreamCreationTimestamp: new Date('2019-01-01'),
        StreamStatus: 'ACTIVE'
      }
    });
    await expect(ensureStreamExists(commonParams)).resolves.toEqual({
      streamArn: 'foo',
      streamCreatedOn: '2019-01-01T00:00:00.000Z'
    });
    expect(debug.mock.calls).toEqual([
      ['Verifying the "test-stream" stream exists and it\'s active…'],
      ["The stream exists and it's active."]
    ]);
  });

  test("ensureStreamExists tries to create a stream if it doesn't exists", async () => {
    describeStreamSummary.mockRejectedValueOnce(
      Object.assign(new Error('foo'), { code: 'ResourceNotFoundException' })
    );
    waitFor.mockResolvedValueOnce({
      StreamDescription: { StreamARN: 'bar', StreamCreationTimestamp: new Date('2019-01-01') }
    });
    await expect(ensureStreamExists(commonParams)).resolves.toEqual({
      streamArn: 'bar',
      streamCreatedOn: '2019-01-01T00:00:00.000Z'
    });
    expect(createStream).toHaveBeenCalledWith({ ShardCount: 1, StreamName: 'test-stream' });
    expect(waitFor).toHaveBeenCalledWith('streamExists', { StreamName: 'test-stream' });
    expect(debug.mock.calls).toEqual([
      ['Verifying the "test-stream" stream exists and it\'s active…'],
      ['Trying to create the stream…'],
      ['Waiting for the new stream to be active…'],
      ['The new stream is now active.']
    ]);
  });

  test("ensureStreamExists won't create a stream if it doesn't exists when opted out", async () => {
    describeStreamSummary.mockRejectedValueOnce(
      Object.assign(new Error('foo'), { code: 'ResourceNotFoundException' })
    );
    waitFor.mockResolvedValueOnce({
      StreamDescriptionSummary: {
        StreamARN: 'bar',
        StreamCreationTimestamp: new Date('2019-01-01')
      }
    });
    await expect(
      ensureStreamExists({ ...commonParams, createStreamIfNeeded: false })
    ).resolves.toEqual({ streamArn: null });
    expect(debug.mock.calls).toEqual([
      ['Verifying the "test-stream" stream exists and it\'s active…'],
      ["The stream exists and it's active."]
    ]);
  });

  test('getEnhancedConsumers resolves with an object listing the enhanced consumers', async () => {
    listStreamConsumers.mockResolvedValueOnce({
      Consumers: [{ ConsumerARN: 'foo', ConsumerName: 'bar', ConsumerStatus: 'ACTIVE' }]
    });
    await expect(getEnhancedConsumers(commonParams)).resolves.toEqual({
      bar: { arn: 'foo', status: 'ACTIVE' }
    });
  });

  test('getEnhancedConsumers will wait until all the enhanced consumers are active', async () => {
    listStreamConsumers.mockResolvedValueOnce({
      Consumers: [{ ConsumerARN: 'foo', ConsumerName: 'bar', ConsumerStatus: 'UPDATING' }]
    });
    listStreamConsumers.mockResolvedValueOnce({
      Consumers: [{ ConsumerARN: 'foo', ConsumerName: 'bar', ConsumerStatus: 'ACTIVE' }]
    });
    await expect(getEnhancedConsumers(commonParams)).resolves.toEqual({
      bar: { arn: 'foo', status: 'ACTIVE' }
    });
    expect(wait).toHaveBeenCalledWith(3000);
  });

  test('getStreamShards resolves with an object listing the stream shards', async () => {
    listShards.mockResolvedValueOnce({
      Shards: [
        {
          SequenceNumberRange: { StartingSequenceNumber: 0 },
          ShardId: 'foo'
        },
        {
          ParentShardId: 'foo',
          SequenceNumberRange: { StartingSequenceNumber: 1 },
          ShardId: 'bar'
        },
        {
          ParentShardId: 'baz',
          SequenceNumberRange: { StartingSequenceNumber: 2 },
          ShardId: 'qux'
        }
      ]
    });
    await expect(getStreamShards(commonParams)).resolves.toEqual({
      bar: { parent: 'foo', startingSequenceNumber: 1 },
      foo: { parent: null, startingSequenceNumber: 0 },
      qux: { parent: null, startingSequenceNumber: 2 }
    });
    expect(debug.mock.calls).toEqual([['Retrieving shards for the "test-stream" stream…']]);
  });

  test('registerEnhancedConsumer should wait for the activation', async () => {
    registerStreamConsumer.mockResolvedValueOnce({ ConsumerStatus: 'CREATING' });
    listStreamConsumers.mockResolvedValueOnce({ Consumers: [] });
    listStreamConsumers.mockResolvedValueOnce({
      Consumers: [{ ConsumerName: 'test-consumer', ConsumerStatus: 'ACTIVE' }]
    });
    await expect(registerEnhancedConsumer(commonParams)).resolves.toBeUndefined();
    expect(debug.mock.calls).toEqual([
      ['Registering enhanced consumer "test-consumer"…'],
      ['Waiting for the new enhanced consumer "test-consumer" to be active…'],
      ['The enhanced consumer "test-consumer" is now active.']
    ]);
    expect(registerStreamConsumer).toHaveBeenCalledWith({
      ConsumerName: 'test-consumer',
      StreamARN: 'arn:test-stream'
    });
    expect(listStreamConsumers).toHaveBeenCalledTimes(2);
  });
});
