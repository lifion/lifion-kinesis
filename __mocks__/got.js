import { Transform } from 'node:stream';
import { vi } from 'vitest';

let hooks;
let response;

const abort = vi.fn();

const stream = vi.fn(() => {
  response = new Transform({ objectMode: true });
  setImmediate(() => {
    const request = { abort };
    response.emit('request', request);
  });
  return response;
});

const extend = vi.fn((...args) => {
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

export default { extend, getHooks, getMocks, mockClear };
