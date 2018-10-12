'use strict';

const isJsonRegex = /^[{[].*[}\]]$/;

function isJson(input) {
  return isJsonRegex.test(input);
}

function noop() {}

function safeJsonParse(input) {
  try {
    return JSON.parse(input);
  } catch (err) {
    return {};
  }
}

function wait(ms) {
  return new Promise(resolve => {
    setTimeout(resolve, ms);
  });
}

module.exports = { isJson, noop, safeJsonParse, wait };
