import { core } from './core';
import { describe, it, expect } from 'vitest';

describe('core', () => {
  it('should work', () => {
    expect(core()).toEqual('core');
  });
});
