'use strict';

const { compressAsync, decompressAsync } = require('lzutf8');

module.exports = {
  'LZ-UTF8': {
    compress: (input, outputEncoding) =>
      new Promise((resolve, reject) => {
        const options = { outputEncoding, useWebWorker: false };
        compressAsync(input, options, (output, err) => {
          if (!err) resolve(output);
          else reject(err);
        });
      }),
    decompress: (input, inputEncoding) =>
      new Promise((resolve, reject) => {
        const options = { inputEncoding, useWebWorker: false };
        decompressAsync(input, options, (output, err) => {
          if (!err) resolve(output);
          else reject(err);
        });
      })
  }
};
