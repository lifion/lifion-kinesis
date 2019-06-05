/**
 * A module with a collection of common functions.
 *
 * @module utils
 * @private
 */

'use strict';

const isRetryAllowed = require('is-retry-allowed');

const { BAIL_RETRY_LIST, FORCED_RETRY_LIST } = require('./constants');

const { CAPTURE_STACK_TRACE } = process.env;

/**
 * Determines if the request or call that caused the given error should not be retried.
 *
 * @param {Error} err - The error thrown by a request or call on a retry attempt.
 * @returns {boolean} `true` if the request or call should not be retried, `false` otherwise.
 * @memberof module:utils
 */
function shouldBailRetry(err) {
  const { code } = err;
  if (FORCED_RETRY_LIST.includes(code)) return false;
  return BAIL_RETRY_LIST.includes(code) || !isRetryAllowed(err);
}

/**
 * Transforms the given error instance by replacing the stack with the one captured in `stackObj`.
 * If `stackObj` is undefined, the error isn't mutated and it's returned in its original form.
 *
 * @param {Error} err - The error instance to transform.
 * @param {Object} [stackObj] - An object that stores a captured error stack.
 * @returns {Error} An error with a mutated stack (if given one).
 * @memberof module:utils
 */
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

/**
 * Captures the current stack trace into an object if the `CAPTURE_STACK_TRACE` environment
 * variable is defined. The returned object is used in conjuntion with `transformErrorStack` to
 * modify the stack of asynchronous errors thrown by the AWS SDK.
 *
 * @param {Function} caller - The function or class whose constructor could throw an error.
 * @returns {Object} An object with a captured stack trace (if enabled).
 * @memberof module:utils
 */
function getStackObj(caller) {
  let stackObj;
  if (CAPTURE_STACK_TRACE) {
    stackObj = {};
    Error.captureStackTrace(stackObj, caller);
  }
  return stackObj;
}

module.exports = { getStackObj, shouldBailRetry, transformErrorStack };
