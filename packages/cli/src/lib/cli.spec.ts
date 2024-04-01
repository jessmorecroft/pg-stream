import { cli } from './cli';

describe('cli', () => {
  it('should work', () => {
    expect(
      () => cli([
        '', // node
        '', // javascript filename
        'query',
        '-W',
        'topsecret',
        '-c',
        'select current_timestamp; select 1',
      ]),
    ).not.toThrow();
  });
});
