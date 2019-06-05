'use strict';

async function asyncRetry(func, opts) {
  let data;
  let error;
  let stopRetries;

  const bail = err => {
    error = err;
  };

  do {
    error = null;
    stopRetries = true;
    try {
      data = await func(bail);
    } catch (err) {
      opts.onRetry(err);
      error = err;
      stopRetries = false;
    }
  } while (!stopRetries);

  if (error) throw error;

  return data;
}

module.exports = asyncRetry;
