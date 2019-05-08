'use strict';

const { CAPTURE_STACK_TRACE } = process.env;

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
  CAPTURE_STACK_TRACE: Boolean(CAPTURE_STACK_TRACE),
  FORCED_RETRY_LIST
});
