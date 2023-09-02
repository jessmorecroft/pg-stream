import { Effect } from 'effect';

export interface PgClient {
  runSql(sql: string): Effect.Effect<never, never, string[]>;
}
