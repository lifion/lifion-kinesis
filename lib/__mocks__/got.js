'use strict';

const { Transform } = require('stream');

let hooks;
let response;

const abort = jest.fn();

const stream = jest.fn(() => {
  response = new Transform({ objectMode: true });
  setImmediate(() => {
    const request = { abort };
    response.emit('request', request);
  });
  return response;
});

const extend = jest.fn((...args) => {
  [{ hooks }] = args;
  return { stream };
});

function getMocks() {
  return { abort, extend, response, stream };
}

function mockClear() {
  hooks = {};
  abort.mockClear();
  extend.mockClear();
  stream.mockClear();
}

function getHooks() {
  return hooks;
}

module.exports = { extend, getHooks, getMocks, mockClear };
