import { Pool } from 'effect';
import { Options, make as makePgClient } from './pg-client';

type PoolOptions = Omit<Parameters<typeof Pool.makeWithTTL>[0], 'acquire'>;

export type PgPool = ReturnType<typeof make>;

export const make = (options: Options & PoolOptions) =>
  Pool.makeWithTTL({
    acquire: makePgClient(options),
    ...options,
  });
