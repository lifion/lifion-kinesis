'use strict';

const isJsonRegex = /^[{[].*[}\]]$/;

module.exports.isJson = input => isJsonRegex.test(input);

module.exports.noop = () => {};

module.exports.wait = ms =>
  new Promise(resolve => {
    setTimeout(resolve, ms);
  });
