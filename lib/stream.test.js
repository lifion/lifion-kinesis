'use strict';

const stream = require('./stream');

describe('lib/stream', () => {
  test('the module exports the expected', () => {
    expect(stream).toEqual({
      checkIfStreamExists: expect.any(Function),
      confirmStreamTags: expect.any(Function),
      ensureStreamEncription: expect.any(Function),
      ensureStreamExists: expect.any(Function),
      getEnhancedConsumers: expect.any(Function),
      getStreamShards: expect.any(Function),
      registerEnhancedConsumer: expect.any(Function)
    });
  });
});
