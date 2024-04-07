import { Effect } from 'effect';
import { cli } from './cli';

describe('cli', () => {
  it('should work', async () => {
    await expect(
      Effect.runPromise(
        cli([
          '', // node
          '', // javascript filename
          '-W',
          'topsecret',
          'query',
          '-c',
          'select current_timestamp; select 1',
        ]),
      ),
    ).resolves.not.toThrow();
  });
});
