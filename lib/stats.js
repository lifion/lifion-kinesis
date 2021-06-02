/**
 * Module that manages statistics about the requests to AWS.
 *
 * @module stats
 * @private
 */

'use strict';

const Cache = require('lru-cache');

const { version } = require('../package.json');

const FIVE_MINS = 5 * 60 * 1000;

const moduleStats = {};
const streams = {};

/**
 * Returns an object where the stats should be stored. If a stream name is not provided,
 * the object that stores the module stats is returned instead.
 *
 * @param {string} [streamName] - The name of a Kinesis stream.
 * @param {string} [key] - A key for a store object.
 * @returns {Object} The store for the stats.
 * @private
 */
function getStore(streamName, key) {
  let store = moduleStats;
  if (streamName) {
    if (!streams[streamName]) streams[streamName] = {};
    store = streams[streamName];
  }
  if (key) {
    if (!store[key]) store[key] = {};
    return store[key];
  }
  return store;
}

/**
 * Stores an exception in the history of recent exceptions of the given store.
 *
 * @param {Object} params - The parameters.
 * @param {Error} params.exception - The exception to store.
 * @param {Object} params.store - The object where to store the exception.
 * @private
 */
function cacheException(params) {
  const { exception, store } = params;
  if (!store.recentExceptions) {
    store.recentExceptions = new Cache({ maxAge: FIVE_MINS });
  }
  store.recentExceptions.set(new Date(), exception);
}

/**
 * Returns an object with a list of exceptions and its count.
 *
 * @param {Object} cache - The cache instance where the exceptions are stored.
 * @returns {Object}
 * @private
 */
function formatExceptions(cache) {
  cache.prune();
  const count = cache.itemCount;
  const exceptions = [];
  cache.forEach((err, timestamp) => {
    const { code, message, requestId, statusCode } = err;
    exceptions.push({
      message,
      timestamp,
      ...(code && { code }),
      ...(requestId && { requestId }),
      ...(statusCode && { statusCode })
    });
  });
  return { count, exceptions };
}

/**
 * Retrieves the stats for the module, or for a given stream (if the stream name is provided).
 *
 * @param {string} [streamName] - The name of the stream.
 * @returns {Object}
 * @memberof module:stats
 */
function getStats(streamName) {
  const formatValues = (results, [key, value]) => {
    const { recentExceptions } = value;
    let formattedValue = value;
    if (key === 'recentExceptions') {
      formattedValue = formatExceptions(value);
    } else if (recentExceptions) {
      formattedValue = { ...value, recentExceptions: formatExceptions(recentExceptions) };
    }
    return Object.assign(results, { [key]: formattedValue });
  };

  const formattedStats = Object.entries(moduleStats).reduce(formatValues, { version });

  if (streamName) {
    const { recentExceptions, ...baseStats } = formattedStats;
    const streamData = getStore(streamName);
    baseStats.kinesis = Object.entries(streamData).reduce(formatValues, {});
    return baseStats;
  }

  const formattedStreams = Object.entries(streams).reduce((results, [key, value]) => {
    const finalValue = Object.entries(value).reduce(formatValues, {});
    return Object.assign(results, { [key]: finalValue });
  }, {});

  return Object.assign(formattedStats, { streams: formattedStreams });
}

/**
 * Reports an error into the stats.
 *
 * @param {string} source - Either `kinesis` or `dynamoDb`.
 * @param {Error} exception - The error to store.
 * @param {string} [streamName] - If source is `kinesis`, the name of the stream.
 * @memberof module:stats
 */
function reportError(source, exception, streamName) {
  if (!source) throw new TypeError('The "source" argument is required.');
  if (!exception) throw new TypeError('The "exception" argument is required.');
  if (streamName) cacheException({ exception, store: getStore(streamName) });
  cacheException({ exception, store: getStore(null, source) });
  cacheException({ exception, store: moduleStats });
}

/**
 * Reports the consumption of a Kinesis record by a consumer into the stats.
 *
 * @param {string} streamName - The name of the Kinesis stream from where the record originated.
 * @memberof module:stats
 */
function reportRecordConsumed(streamName) {
  if (!streamName) throw new TypeError('The "streamName" argument is required.');
  const now = new Date();
  getStore(streamName).lastRecordConsumed = now;
  getStore(null, 'kinesis').lastRecordConsumed = now;
}

/**
 * Reports the submission of a Kinesis record into the stats.
 *
 * @param {string} streamName - The name of the Kinesis stream where the record was sent.
 * @memberof module:stats
 */
function reportRecordSent(streamName) {
  if (!streamName) throw new TypeError('The "streamName" argument is required.');
  const now = new Date();
  getStore(streamName).lastRecordSent = now;
  getStore(null, 'kinesis').lastRecordSent = now;
}

/**
 * Reports a successful AWS request response into the stats.
 *
 * @param {string} source - Either `kinesis` or `dynamoDb`.
 * @param {string} [streamName] - The name of the Kinesis stream.
 * @memberof module:stats
 */
function reportResponse(source, streamName) {
  if (!source) throw new TypeError('The "source" argument is required.');
  const now = new Date();
  if (streamName) getStore(streamName).lastAwsResponse = now;
  getStore(null, source).lastAwsResponse = now;
  moduleStats.lastAwsResponse = now;
}

module.exports = {
  getStats,
  reportError,
  reportRecordConsumed,
  reportRecordSent,
  reportResponse
};
