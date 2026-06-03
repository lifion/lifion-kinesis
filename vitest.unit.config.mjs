import { defineConfig } from 'vitest/config';

// Unit suite: the co-located lib/*.test.js specs, migrated off jest. Mirrors the
// old .jest.json — node environment, 100% coverage gate. Node-module mocks live
// in the repo-root __mocks__ dir and are applied globally via the setup file (jest
// auto-applied them; vitest needs an explicit vi.mock per module).
export default defineConfig({
  test: {
    coverage: {
      exclude: ['lib/**/*.test.js'],
      include: ['lib/**/*.js'],
      provider: 'v8',
      reporter: ['clover', 'lcov', 'text'],
      reportsDirectory: 'coverage',
      thresholds: { branches: 100, functions: 100, lines: 100, statements: 100 }
    },
    environment: 'node',
    include: ['lib/**/*.test.js'],
    setupFiles: ['test/unit-setup.mjs']
  }
});
