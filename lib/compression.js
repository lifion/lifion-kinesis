'use strict';

const { decompressAsync } = require('lzutf8');

module.exports = {
  'LZ-UTF8': {
    decompress: (input, inputEncoding = 'Base64') =>
      new Promise((resolve, reject) => {
        const options = { inputEncoding, useWebWorker: false };
        decompressAsync(input, options, (output, err) => {
          if (!err) resolve(output);
          else reject(err);
        });
      })
  }
};
