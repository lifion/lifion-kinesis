'use strict';

module.exports.noop = () => {};

module.exports.wait = ms =>
  new Promise(resolve => {
    setTimeout(resolve, ms);
  });
