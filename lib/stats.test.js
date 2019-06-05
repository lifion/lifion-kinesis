/* eslint-disable global-require */

'use strict';

const { version } = require('../package.json');

jest.useFakeTimers();

describe('lib/stats', () => {
  let getStats;
  let reportError;
  let reportRecordConsumed;
  let reportRecordSent;
  let reportResponse;
  let stats;

  beforeEach(() => {
    stats = require('./stats');
    ({ getStats, reportError, reportRecordConsumed, reportRecordSent, reportResponse } = stats);
  });

  afterEach(() => {
    jest.resetModules();
  });

  test('the module exports the expected', () => {
    expect(stats).toEqual({
      getStats: expect.any(Function),
      reportError: expect.any(Function),
      reportRecordConsumed: expect.any(Function),
      reportRecordSent: expect.any(Function),
      reportResponse: expect.any(Function)
    });
  });

  test('getStats returns empty data when nothing has been reported', () => {
    expect(getStats()).toEqual({ streams: {}, version });
  });

  test('reportError throws if not called with a source', () => {
    expect(() => reportError()).toThrow('The "source" argument is required.');
  });

  test('reportError throws if not called with an exception', () => {
    expect(() => reportError('foo')).toThrow('The "exception" argument is required.');
  });

  test('reportError adds an error to the stats', () => {
    reportError('foo', new Error('bar'));
    reportError('foo', new Error('baz'));
    const exceptions = [
      { message: 'baz', timestamp: expect.any(Date) },
      { message: 'bar', timestamp: expect.any(Date) }
    ];
    const recentExceptions = { count: 2, exceptions };
    expect(getStats()).toEqual({
      foo: { recentExceptions },
      recentExceptions,
      streams: {},
      version
    });
  });

  test('reportError preserves error code, request ID, and status code', () => {
    const error = Object.assign(new Error('bar'), {
      code: 'baz',
      requestId: 'qux',
      statusCode: 'quux'
    });
    reportError('foo', error);
    const exceptions = [
      {
        code: 'baz',
        message: 'bar',
        requestId: 'qux',
        statusCode: 'quux',
        timestamp: expect.any(Date)
      }
    ];
    const recentExceptions = { count: 1, exceptions };
    expect(getStats()).toEqual({
      foo: { recentExceptions },
      recentExceptions,
      streams: {},
      version
    });
  });

  test('reportError adds stream errors to the stats', () => {
    reportError('foo', new Error('bar'), 'baz');
    const exceptions = [{ message: 'bar', timestamp: expect.any(Date) }];
    const recentExceptions = { count: 1, exceptions };
    expect(getStats('baz')).toEqual({
      foo: { recentExceptions },
      kinesis: { recentExceptions },
      version
    });
  });

  test('reportRecordConsumed throws if not called with a stream name', () => {
    expect(() => reportRecordConsumed()).toThrow('The "streamName" argument is required.');
  });

  test('reportRecordConsumed updates the last record consumed stats', () => {
    reportRecordConsumed('foo');
    const lastRecordConsumed = expect.any(Date);
    expect(getStats()).toEqual({
      kinesis: { lastRecordConsumed },
      streams: { foo: { lastRecordConsumed } },
      version
    });
  });

  test('reportRecordSent throws if not called with a stream name', () => {
    expect(() => reportRecordSent()).toThrow('The "streamName" argument is required.');
  });

  test('reportRecordSent updates the last record sent stats', () => {
    reportRecordSent('foo');
    const lastRecordSent = expect.any(Date);
    expect(getStats()).toEqual({
      kinesis: { lastRecordSent },
      streams: { foo: { lastRecordSent } },
      version
    });
  });

  test('reportResponse throws if not called with a source', () => {
    expect(() => reportResponse()).toThrow('The "source" argument is required.');
  });

  test('reportResponse updates the last AWS response stats', () => {
    reportResponse('foo');
    reportResponse('bar', 'baz');
    reportResponse('bar', 'qux');
    const lastAwsResponse = expect.any(Date);
    expect(getStats()).toEqual({
      bar: { lastAwsResponse },
      foo: { lastAwsResponse },
      lastAwsResponse,
      streams: { baz: { lastAwsResponse }, qux: { lastAwsResponse } },
      version
    });
  });
});
