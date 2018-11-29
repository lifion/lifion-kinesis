'use strict';

const { decompressAsync } = require('lzutf8');

module.exports = {
  'LZ-UTF8': {
    decompress: input =>
      new Promise((resolve, reject) => {
        decompressAsync(input, { inputEncoding: 'Base64', useWebWorker: false }, (output, err) => {
          if (!err) resolve(output);
          else reject(err);
        });
      })
  }
};
