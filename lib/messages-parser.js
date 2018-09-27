'use strict';

const { Transform } = require('stream');
const { default: hex } = require('hex-debug');

const privateData = new WeakMap();

function internal(instance) {
  if (!privateData.has(instance)) privateData.set(instance, {});
  return privateData.get(instance);
}

class MessagesParser extends Transform {
  constructor(ctx) {
    super();
    Object.assign(internal(this), ctx);
  }

  _transform(chunk, encoding, callback) {
    const { logger } = internal(this);
    logger.debug(`Got ${chunk.length} bytes.`, `\n${hex(chunk)}`);
    callback();
  }
}

module.exports = MessagesParser;
