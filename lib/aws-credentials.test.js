import { describe, expect, test } from 'vitest';

import { assertCredentialsSupported } from './aws-credentials';

describe('lib/aws-credentials', () => {
  test('throws when flat credentials are passed at the top level', () => {
    expect(() => assertCredentialsSupported({ accessKeyId: 'foo' })).toThrow(TypeError);
    expect(() => assertCredentialsSupported({ secretAccessKey: 'bar' })).toThrow(/not supported/);
    expect(() => assertCredentialsSupported({ sessionToken: 'baz' })).toThrow(/credentials/);
  });

  test('does not throw for a credentials object or provider', () => {
    expect(() => assertCredentialsSupported({ credentials: { accessKeyId: 'foo' } })).not.toThrow();
    expect(() => assertCredentialsSupported({ credentials: () => ({}) })).not.toThrow();
  });

  test('does not throw for non-credential options', () => {
    expect(() =>
      assertCredentialsSupported({ endpoint: 'http://localhost', region: 'us-east-1' })
    ).not.toThrow();
  });

  test('defaults the options to an empty object', () => {
    expect(() => assertCredentialsSupported()).not.toThrow();
  });
});
