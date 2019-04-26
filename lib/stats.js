'use strict';

const Cache = require('lru-cache');
const { version } = require('../package.json');

const FIVE_MINS = 5 * 60 * 1000;
const STATS_INTERVAL = 10 * 1000;

const moduleStats = {};
const streams = {};

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

function cacheException(params) {
  const { exception, store } = params;
  if (!store.recentExceptions) {
    store.recentExceptions = new Cache({ maxAge: FIVE_MINS });
  }
  store.recentExceptions.set(new Date(), exception);
}

function formatExceptions(cache) {
  cache.prune();
  const count = cache.itemCount;
  const exceptions = [];
  cache.forEach((err, timestamp) => {
    const { code, message, requestId, statusCode } = err;
    exceptions.push({ code, message, requestId, statusCode, timestamp });
  });
  return { count, exceptions };
}

function getStats(streamName) {
  const formatValues = (results, [key, value]) => {
    const { recentExceptions } = value;
    let formattedValue = value;
    if (key === 'recentExceptions') {
      formattedValue = formatExceptions(value);
    } else if (recentExceptions) {
      formattedValue = Object.assign({}, value, {
        recentExceptions: formatExceptions(recentExceptions)
      });
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

function reportException(exception, source, streamName) {
  if (streamName) {
    cacheException({ exception, store: getStore(streamName) });
  }
  cacheException({ exception, store: getStore(null, source) });
  cacheException({ exception, store: moduleStats });
}

function reportRecordConsumed(streamName) {
  const now = new Date();
  getStore(streamName).lastRecordConsumed = now;
  getStore(null, 'kinesis').lastRecordConsumed = now;
}

function reportRecordSent(streamName) {
  const now = new Date();
  getStore(streamName).lastRecordSent = now;
  getStore(null, 'kinesis').lastRecordSent = now;
}

function reportSuccess(source, streamName) {
  const now = new Date();
  if (streamName) getStore(streamName).lastAwsResponse = now;
  getStore(null, source).lastAwsResponse = now;
  moduleStats.lastAwsResponse = now;
}

function startStatsEmitter(instance, { statsInterval, streamName }) {
  setInterval(() => {
    instance.emit('stats', getStats(streamName));
  }, statsInterval || STATS_INTERVAL);
}

module.exports = {
  getStats,
  reportException,
  reportRecordConsumed,
  reportRecordSent,
  reportSuccess,
  startStatsEmitter
};
