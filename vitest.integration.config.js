import { defineConfig } from 'vitest/config';

// Integration suite: runs the real client against a local AWS stack (LocalStack).
// Kept separate from the jest unit suite — no coverage gate, serial, generous
// timeouts since it creates real streams/tables and waits on lease/poll cycles.
export default defineConfig({
  test: {
    coverage: { enabled: false },
    environment: 'node',
    fileParallelism: false,
    globalSetup: ['test/integration/setup/wait-for-localstack.js'],
    hookTimeout: 120_000,
    include: ['test/integration/**/*.test.js'],
    pool: 'forks',
    reporters: ['default'],
    testTimeout: 120_000
  }
});
