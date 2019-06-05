'use strict';

const { Transform } = require('stream');

function Parser() {
  return new Transform({
    objectMode: true,
    transform(chunk, encoding, callback) {
      this.push(chunk);
      callback();
    }
  });
}

module.exports = { Parser };
