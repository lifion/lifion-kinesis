import { describe, expect, test } from 'vitest';

import * as constants from './constants';

describe('lib/constants', () => {
  test('the module exports the expected', () => {
    expect({ ...constants }).toEqual({
      BAIL_RETRY_LIST: expect.any(Array),
      FORCED_RETRY_LIST: expect.any(Array)
    });
  });
});
