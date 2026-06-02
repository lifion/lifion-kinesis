// Global setup for the integration suite: blocks until LocalStack reports the
// services we exercise are up. Set SKIP_LOCALSTACK_WAIT=true to bypass (useful
// for verifying the runner itself without a stack running).
const ENDPOINT = process.env.LOCALSTACK_ENDPOINT || 'http://localhost:4566';
const HEALTH_URL = `${ENDPOINT}/_localstack/health`;
const REQUIRED = ['kinesis', 'dynamodb'];
const READY_STATES = new Set(['available', 'running']);
const TIMEOUT_MS = 90_000;
const POLL_MS = 2_000;

export default async function setup() {
  if (process.env.SKIP_LOCALSTACK_WAIT === 'true') return;

  const deadline = Date.now() + TIMEOUT_MS;
  for (;;) {
    try {
      const res = await fetch(HEALTH_URL);
      if (res.ok) {
        const { services = {} } = await res.json();
        if (REQUIRED.every((name) => READY_STATES.has(services[name]))) return;
      }
    } catch {
      // LocalStack not accepting connections yet.
    }
    if (Date.now() > deadline) {
      throw new Error(
        `LocalStack was not ready at ${HEALTH_URL} within ${TIMEOUT_MS / 1000}s. ` +
          `Required services: ${REQUIRED.join(', ')}.`
      );
    }
    await new Promise((resolve) => setTimeout(resolve, POLL_MS));
  }
}
