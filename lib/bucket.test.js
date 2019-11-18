'use strict';

const bucket = require('./bucket');

describe('lib/bucket', () => {
  const bucketName = 'test bucket name';
  const streamName = 'test-stream-name';
  const tags = [];

  const { confirmBucketLifecycleConfiguration, confirmBucketTags, ensureBucketExists } = bucket;

  const createBucket = jest.fn();
  const getBucketLifecycleConfiguration = jest.fn();
  const getBucketTagging = jest.fn();
  const getObject = jest.fn();
  const headBucket = jest.fn();
  const putBucketLifecycleConfiguration = jest.fn();
  const putBucketTagging = jest.fn();
  const putObject = jest.fn();
  const client = {
    createBucket,
    getBucketLifecycleConfiguration,
    getBucketTagging,
    getObject,
    headBucket,
    putBucketLifecycleConfiguration,
    putBucketTagging,
    putObject
  };

  const debug = jest.fn();
  const errorMock = jest.fn();
  const logger = { debug, error: errorMock };

  afterEach(() => {
    createBucket.mockClear();
    getBucketLifecycleConfiguration.mockClear();
    getBucketTagging.mockClear();
    getObject.mockClear();
    headBucket.mockClear();
    putBucketLifecycleConfiguration.mockClear();
    putBucketTagging.mockClear();
    putObject.mockClear();

    debug.mockClear();
    errorMock.mockClear();
  });

  test('the module exports the expected', () => {
    expect(bucket).toEqual({
      confirmBucketLifecycleConfiguration: expect.any(Function),
      confirmBucketTags: expect.any(Function),
      ensureBucketExists: expect.any(Function)
    });
  });

  describe('confirmBucketLifecycleConfiguration', () => {
    const expectedDefaultRule = {
      AbortIncompleteMultipartUpload: {
        DaysAfterInitiation: 1
      },
      Expiration: { Days: 1 },
      Filter: {
        Prefix: `${streamName}--`
      },
      ID: 'lifion-kinesis-ttl-rule',
      NoncurrentVersionExpiration: {
        NoncurrentDays: 1
      },
      Status: 'Enabled'
    };

    test('sets an expiration rule on buckets with no existing rules key', async () => {
      getBucketLifecycleConfiguration.mockResolvedValueOnce({});
      putBucketLifecycleConfiguration.mockResolvedValueOnce();

      await confirmBucketLifecycleConfiguration({ bucketName, client, logger, streamName });

      expect(getBucketLifecycleConfiguration).toHaveBeenCalledWith({ Bucket: bucketName });
      expect(putBucketLifecycleConfiguration).toHaveBeenCalledWith({
        Bucket: 'test bucket name',
        LifecycleConfiguration: {
          Rules: [expectedDefaultRule]
        }
      });
      expect(logger.debug).toHaveBeenCalledWith('The bucket rules have been updated.');
    });
    test('sets an expiration rule on buckets with no existing expiration rule', async () => {
      getBucketLifecycleConfiguration.mockResolvedValueOnce({ Rules: [] });
      putBucketLifecycleConfiguration.mockResolvedValueOnce();

      await confirmBucketLifecycleConfiguration({ bucketName, client, logger, streamName });

      expect(getBucketLifecycleConfiguration).toHaveBeenCalledWith({ Bucket: bucketName });
      expect(putBucketLifecycleConfiguration).toHaveBeenCalledWith({
        Bucket: 'test bucket name',
        LifecycleConfiguration: {
          Rules: [expectedDefaultRule]
        }
      });
      expect(logger.debug).toHaveBeenCalledWith('The bucket rules have been updated.');
    });

    test('merges existing rules with a created expiration rule', async () => {
      const existingRules = [{ Filter: {} }];
      getBucketLifecycleConfiguration.mockResolvedValueOnce({ Rules: existingRules });
      putBucketLifecycleConfiguration.mockResolvedValueOnce();

      await confirmBucketLifecycleConfiguration({ bucketName, client, logger, streamName });

      expect(getBucketLifecycleConfiguration).toHaveBeenCalledWith({ Bucket: bucketName });
      expect(putBucketLifecycleConfiguration).toHaveBeenCalledWith({
        Bucket: 'test bucket name',
        LifecycleConfiguration: {
          Rules: [...existingRules, expectedDefaultRule]
        }
      });
      expect(logger.debug).toHaveBeenCalledWith('The bucket rules have been updated.');
    });

    test('does nothing when an expiration rule already exists on the bucket', async () => {
      const existingRules = [expectedDefaultRule];
      getBucketLifecycleConfiguration.mockResolvedValueOnce({ Rules: existingRules });
      putBucketLifecycleConfiguration.mockResolvedValueOnce();

      await confirmBucketLifecycleConfiguration({ bucketName, client, logger, streamName });

      expect(getBucketLifecycleConfiguration).toHaveBeenCalledWith({ Bucket: bucketName });
      expect(logger.debug).toHaveBeenCalledWith(
        'The bucket rules are already defined as required.'
      );
    });
  });

  describe('confirmBucketTags', () => {
    test('does not push any tags to the bucket', async () => {
      getBucketTagging.mockResolvedValueOnce({ TagSet: [] });
      putBucketTagging.mockResolvedValueOnce();

      await confirmBucketTags({ bucketName, client, logger, tags });

      expect(getBucketTagging).toHaveBeenCalledWith({ Bucket: bucketName });
      expect(putBucketTagging).not.toHaveBeenCalled();
      expect(logger.debug).toHaveBeenCalledWith('The bucket is already tagged as required.');
    });

    test('pushes the provided tags to the bucket', async () => {
      getBucketTagging.mockResolvedValueOnce({ TagSet: [] });
      putBucketTagging.mockResolvedValueOnce();

      const Key = 'test key';
      const Value = 'test value';
      const testTags = { [Key]: Value };
      await confirmBucketTags({ bucketName, client, logger, tags: testTags });

      expect(getBucketTagging).toHaveBeenCalledWith({ Bucket: bucketName });
      expect(putBucketTagging).toHaveBeenCalledWith({
        Bucket: 'test bucket name',
        Tagging: { TagSet: [{ Key, Value }] }
      });
      expect(logger.debug).toHaveBeenCalledWith('The bucket tags have been updated.');
    });

    test('pushes the merged existing and provided tags to the bucket', async () => {
      const Key = 'test key';
      const Value = 'test value';
      getBucketTagging.mockResolvedValueOnce({ TagSet: [{ Key, Value }] });
      putBucketTagging.mockResolvedValueOnce();

      const newKey = 'new test key';
      const newValue = 'new test value';
      const testTags = { [newKey]: newValue };
      await confirmBucketTags({ bucketName, client, logger, tags: testTags });

      expect(getBucketTagging).toHaveBeenCalledWith({ Bucket: bucketName });
      expect(putBucketTagging).toHaveBeenCalledWith({
        Bucket: 'test bucket name',
        Tagging: {
          TagSet: [
            { Key, Value },
            { Key: newKey, Value: newValue }
          ]
        }
      });
      expect(logger.debug).toHaveBeenCalledWith('The bucket tags have been updated.');
    });
  });

  describe('ensureBucketExists', () => {
    test('does nothing when the bucket exists', async () => {
      headBucket.mockResolvedValueOnce();

      await ensureBucketExists({ bucketName, client, logger });

      expect(logger.debug).toHaveBeenCalledWith(
        `Verifying the "${bucketName}" bucket exists and accessible…`
      );
      expect(headBucket).toHaveBeenCalledWith({ Bucket: bucketName });
      expect(logger.debug).toHaveBeenCalledWith('Bucket exists and is accessible.');
    });

    test('creates a bucket when one does not exist', async () => {
      const checkBucketError = new Error('checkBucketError');
      const bucketData = { some: 'bucket data' };
      headBucket.mockRejectedValueOnce(checkBucketError);
      createBucket.mockResolvedValueOnce(bucketData);

      await ensureBucketExists({ bucketName, client, logger });

      expect(logger.debug).toHaveBeenCalledWith(
        `Verifying the "${bucketName}" bucket exists and accessible…`
      );
      expect(headBucket).toHaveBeenCalledWith({ Bucket: bucketName });
      expect(logger.debug).toHaveBeenCalledWith('Bucket is not accessible.');
      expect(logger.debug).toHaveBeenCalledWith('Trying to create the bucket…');
      expect(createBucket).toHaveBeenCalledWith({ Bucket: bucketName });
    });

    test('does nothing when the bucket does not exists but creating a bucket returns no data', async () => {
      const checkBucketError = new Error('checkBucketError');
      const bucketData = null;
      headBucket.mockRejectedValueOnce(checkBucketError);
      createBucket.mockResolvedValueOnce(bucketData);

      await ensureBucketExists({ bucketName, client, logger });

      expect(logger.debug).toHaveBeenCalledWith(
        `Verifying the "${bucketName}" bucket exists and accessible…`
      );
      expect(headBucket).toHaveBeenCalledWith({ Bucket: bucketName });
      expect(logger.debug).toHaveBeenCalledWith('Bucket is not accessible.');
      expect(logger.debug).toHaveBeenCalledWith('Trying to create the bucket…');
      expect(createBucket).toHaveBeenCalledWith({ Bucket: bucketName });
    });

    test('throws when creating a bucket rejects', async () => {
      const checkBucketError = new Error('checkBucketError');
      const createBucketError = new Error('createBuckettError');
      headBucket.mockRejectedValueOnce(checkBucketError);
      createBucket.mockRejectedValueOnce(createBucketError);

      const promise = ensureBucketExists({ bucketName, client, logger });

      await expect(promise).rejects.toThrow(createBucketError);

      expect(logger.debug).toHaveBeenCalledWith(
        `Verifying the "${bucketName}" bucket exists and accessible…`
      );
      expect(headBucket).toHaveBeenCalledWith({ Bucket: bucketName });
      expect(logger.debug).toHaveBeenCalledWith('Bucket is not accessible.');
      expect(logger.debug).toHaveBeenCalledWith('Trying to create the bucket…');
      expect(createBucket).toHaveBeenCalledWith({ Bucket: bucketName });
    });
  });
});
