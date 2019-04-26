'use strict';

const compression = require('./compression');

describe('lib/compression', () => {
  test('the module exports the expected', () => {
    expect(compression).toEqual({
      'LZ-UTF8': {
        compress: expect.any(Function),
        decompress: expect.any(Function)
      }
    });
  });

  describe('LZ-UTF8 suite', () => {
    const { compress, decompress } = compression['LZ-UTF8'];

    test('compress works as expected', async () => {
      await expect(compress('foo', 'Base64')).resolves.toEqual('Zm9v');
    });

    test('compress throws exceptions', async () => {
      await expect(compress('foo', 'bar')).rejects.toThrow('invalid output encoding');
    });

    test('decompress works as expected', async () => {
      await expect(decompress('Zm9v', 'Base64')).resolves.toEqual('foo');
    });

    test('decompress throws exceptions', async () => {
      await expect(decompress('foo', 'bar')).rejects.toThrow('invalid input encoding');
    });
  });
});
