'use strict';

const BAIL_RETRY_LIST = Object.freeze([
  'ConditionalCheckFailedException',
  'MissingParameter',
  'MissingRequiredParameter',
  'MultipleValidationErrors',
  'ResourceInUseException',
  'ResourceNotFoundException',
  'UnexpectedParameter',
  'UnknownOperationException',
  'ValidationException'
]);

const FORCED_RETRY_LIST = Object.freeze(['ENOTFOUND', 'ENETUNREACH']);

module.exports = Object.freeze({
  BAIL_RETRY_LIST,
  FORCED_RETRY_LIST
});
