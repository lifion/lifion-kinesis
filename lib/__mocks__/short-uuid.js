'use strict';

let counter = 0;

module.exports = {
  generate: () => {
    const value = counter.toString().padStart(4, '0');
    counter += 1;
    return value;
  },
  resetMockCounter: () => {
    counter = 0;
  }
};
