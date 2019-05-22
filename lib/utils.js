'use strict';

const isRetryAllowed = require('is-retry-allowed');

const { BAIL_RETRY_LIST, FORCED_RETRY_LIST } = require('./constants');

const { CAPTURE_STACK_TRACE } = process.env;

function shouldBailRetry(err) {
  const { code } = err;
  if (FORCED_RETRY_LIST.includes(code)) return false;
  return BAIL_RETRY_LIST.includes(code) || !isRetryAllowed(err);
}

function transformErrorStack(err, stackObj) {
  if (!stackObj) return err;
  const { code, message, requestId, statusCode } = err;
  const error = new Error(message);
  const newStack = stackObj.stack;
  const newLineIndex = newStack.indexOf('\n');
  const stack = `${code}: ${message}${newStack.substr(newLineIndex)}`;
  return Object.assign(
    error,
    { code, stack },
    requestId && { requestId },
    statusCode && { statusCode }
  );
}

function getStackObj(caller) {
  let stackObj;
  if (CAPTURE_STACK_TRACE) {
    stackObj = {};
    Error.captureStackTrace(stackObj, caller);
  }
  return stackObj;
}

module.exports = { getStackObj, shouldBailRetry, transformErrorStack };
