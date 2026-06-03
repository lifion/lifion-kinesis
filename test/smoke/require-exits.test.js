import { execFile } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';
import { describe, expect, it } from 'vitest';

const execFileAsync = promisify(execFile);

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../..');
const entry = path.join(repoRoot, 'lib', 'index.js');

// Requiring the client must not leave a handle that keeps the event loop alive,
// or a bare `require('lifion-kinesis')` blocks the host process from exiting
// (#474, caused by lzutf8 < 0.6.3 opening a MessagePort at require time). Run
// the require in a child process: if a ref'd handle lingers, the child never
// exits on its own and the timeout has to kill it, failing the test.
describe('requiring the client', () => {
  it('lets the process exit and opens no MessagePort', async () => {
    const script = `require(${JSON.stringify(
      entry
    )});console.log(JSON.stringify(process.getActiveResourcesInfo()));`;

    let stdout;
    try {
      ({ stdout } = await execFileAsync(process.execPath, ['-e', script], { timeout: 10_000 }));
    } catch (err) {
      if (err.killed) {
        throw new Error(
          'Requiring the client kept the event loop alive; the process had to be killed. ' +
            "A dependency likely opened a ref'd handle at require time (see #474)."
        );
      }
      throw err;
    }

    const resources = JSON.parse(stdout.trim());
    expect(resources).not.toContain('MessagePort');
    // The vitest timeout must exceed the child's execFile timeout so that, on a
    // regression, the child is killed first and the assertion above reports it
    // (rather than vitest timing out with a generic message).
  }, 20_000);
});
