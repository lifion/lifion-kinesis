/**
 * Module with a collection of constants used across the codebase.
 *
 * @module constants
 * @private
 */

'use strict';

/**
 * A list of error codes that should not be retried.
 *
 * @memberof module:constants
 */
const BAIL_RETRY_LIST = Object.freeze([
  'ConditionalCheckFailedException',
  'ConfigError',
  'ExpiredIteratorException',
  'MissingParameter',
  'MissingRequiredParameter',
  'MultipleValidationErrors',
  'NoSuchBucket',
  'NoSuchKey',
  'NoSuchLifecycleConfiguration',
  'NoSuchTagSet',
  'NotFound',
  'ResourceInUseException',
  'ResourceNotFoundException',
  'TagSet',
  'UnexpectedParameter',
  'UnknownOperationException',
  'ValidationException'
]);

/**
 * A list of error codes that should always be retried.
 *
 * @memberof module:constants
 */
const FORCED_RETRY_LIST = Object.freeze(['ENOTFOUND', 'ENETUNREACH']);

/**
 * A list of error codes that should be logged as debug rather than warn
 *
 * @memberof module:constants
 */
const SUPRESSED_ERROR_CODES = Object.freeze(['ProvisionedThroughputExceededException']);

module.exports = Object.freeze({
  BAIL_RETRY_LIST,
  FORCED_RETRY_LIST,
  SUPRESSED_ERROR_CODES
});
