import { vi } from 'vitest';

function createResponseMock(data = {}) {
  return vi.fn(() => Promise.resolve(data));
}

const credentials = vi.fn(() =>
  Promise.resolve({
    accessKeyId: 'resolved-access-key-id',
    secretAccessKey: 'resolved-secret-access-key'
  })
);

const createBucket = createResponseMock();
const getBucketLifecycleConfiguration = createResponseMock();
const getBucketTagging = createResponseMock();
const getObject = createResponseMock();
const headBucket = createResponseMock();
const putBucketLifecycleConfiguration = createResponseMock();
const putBucketTagging = createResponseMock();
const putObject = createResponseMock();

const S3 = vi.fn(function () {
  return {
    config: { credentials },
    createBucket,
    getBucketLifecycleConfiguration,
    getBucketTagging,
    getObject,
    headBucket,
    putBucketLifecycleConfiguration,
    putBucketTagging,
    putObject
  };
});

function mockClear() {
  credentials.mockClear();
  createBucket.mockClear();
  getBucketLifecycleConfiguration.mockClear();
  getBucketTagging.mockClear();
  getObject.mockClear();
  headBucket.mockClear();
  putBucketLifecycleConfiguration.mockClear();
  putBucketTagging.mockClear();
  putObject.mockClear();
  S3.mockClear();
}

export { S3, mockClear };
