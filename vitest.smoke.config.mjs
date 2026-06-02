import { defineConfig } from 'vitest/config';

// Smoke suite: fast, no mocks, no LocalStack. Guards against regressions where
// simply requiring the client leaves a handle that keeps the Node event loop
// alive (see #474: lzutf8 < 0.6.3 opened a MessagePort at require time, so a
// bare `require('lifion-kinesis')` blocked the host process from exiting).
export default defineConfig({
  test: {
    coverage: { enabled: false },
    environment: 'node',
    include: ['test/smoke/**/*.test.mjs'],
    reporters: ['default']
  }
});
