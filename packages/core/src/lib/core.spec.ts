import { Effect } from 'effect';
import { makePgPool } from './core';
import { describe, it, expect } from 'vitest';
import * as Schema from '@effect/schema/Schema';

describe('core', () => {
  it('should work', async () => {
    const program = Effect.gen(function* (_) {
      const pgPool = yield* _(
        makePgPool({
          host: 'db',
          port: 5432,
          database: 'postgres',
          username: 'postgres',
          password: 'topsecret',
          min: 1,
          max: 10,
          timeToLive: '1 minutes',
        })
      );

      const pg1 = yield* _(pgPool.get());
      const pg2 = yield* _(pgPool.get());
      const pg3 = yield* _(pgPool.get());

      yield* _(
        pg1.executeSql({
          sql: 'create table mytable ( id serial, hello varchar )',
          schema: Schema.never,
        })
      );

      yield* _(
        Effect.all([
          pg1.executeSql({
            sql: 'insert into mytable ( hello ) values ("cya")',
            schema: Schema.never,
          }),
          pg2.executeSql({
            sql: 'insert into mytable ( hello ) values ("goodbye")',
            schema: Schema.never,
          }),
          pg3.executeSql({
            sql: 'insert into mytable ( hello ) values ("adios")',
            schema: Schema.never,
          }),
        ])
      );

      const rows = yield* _(
        pg1.executeSql({
          sql: 'select * from mytable',
          schema: Schema.struct({
            id: Schema.number,
            hello: Schema.string,
          }),
        })
      );

      yield* _(
        pg1.executeSql({
          sql: 'drop table mytable',
          schema: Schema.never,
        })
      );

      return rows;
    });

    const rows = await Effect.runPromise(program.pipe(Effect.scoped));

    expect(rows).equals([1, 2, 3]);
  });
});
