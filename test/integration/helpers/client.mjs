import { randomUUID } from 'node:crypto';

import Kinesis from '../../../lib/index.js';

const ENDPOINT = process.env.LOCALSTACK_ENDPOINT || 'http://localhost:4566';
const REGION = process.env.AWS_REGION || 'us-east-1';
const CREDENTIALS = { accessKeyId: 'test', secretAccessKey: 'test' };

/**
 * A unique stream name per test so runs don't share Kinesis/DynamoDB state.
 */
export function uniqueStreamName(prefix = 'lifion-it') {
  return `${prefix}-${randomUUID()}`;
}

/**
 * Builds a Kinesis client pointed at the local stack, with fast lease cycles and
 * TRIM_HORIZON so a test reads every record regardless of consumer start timing.
 * Pass `streamName` (and e.g. `useEnhancedFanOut: true`) via overrides.
 */
export function createClient(overrides = {}) {
  const { dynamoDb: dynamoDbOverrides, ...rest } = overrides;
  return new Kinesis({
    ...CREDENTIALS,
    createStreamIfNeeded: true,
    dynamoDb: { ...CREDENTIALS, endpoint: ENDPOINT, region: REGION, ...dynamoDbOverrides },
    endpoint: ENDPOINT,
    initialPositionInStream: 'TRIM_HORIZON',
    leaseAcquisitionInterval: 2_000,
    leaseAcquisitionRecoveryInterval: 2_000,
    region: REGION,
    shardCount: 1,
    ...rest
  });
}

/**
 * Collects records emitted on the consumer stream until `isDone(records)` is
 * truthy, then resolves with all collected records. Rejects on stream error or
 * timeout. `isDone` receives the flat list of records seen so far, which lets
 * callers wait on distinct keys and stay robust to Kinesis at-least-once
 * delivery and batch boundaries.
 */
export function collectRecords(client, isDone, timeoutMs = 60_000) {
  return new Promise((resolve, reject) => {
    const records = [];

    const timer = setTimeout(() => {
      cleanup();
      reject(new Error(`Timed out after ${timeoutMs}ms; collected ${records.length} record(s)`));
    }, timeoutMs);

    function cleanup() {
      clearTimeout(timer);
      client.removeListener('data', onData);
      client.removeListener('error', onError);
    }

    function onData(message) {
      for (const record of message.records) records.push(record);
      if (isDone(records)) {
        cleanup();
        resolve(records);
      }
    }

    function onError(err) {
      cleanup();
      reject(err);
    }

    client.on('data', onData);
    client.on('error', onError);
  });
}

/**
 * Distinct count of record payload ids (records carry `data: { id }` in these
 * specs). Used as the completion signal for `collectRecords`.
 */
export function distinctIds(records) {
  return new Set(records.map((record) => record.data.id)).size;
}
