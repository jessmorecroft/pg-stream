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
          useSSL: true,
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
        pg1.executeCommand({
          sql: 'create table if not exists mytable ( id serial, hello varchar )',
        })
      );

      yield* _(
        Effect.all(
          [
            pg1.executeCommand({
              sql: "insert into mytable ( hello ) values ('cya')",
            }),
            pg2.executeCommand({
              sql: "insert into mytable ( hello ) values ('goodbye')",
            }),
            pg3.executeCommand({
              sql: "insert into mytable ( hello ) values ('adios')",
            }),
          ],
          { concurrency: 'unbounded' }
        )
      );

      const rows = yield* _(
        pg1.executeQuery({
          sql: 'select * from mytable',
          schema: Schema.struct({
            id: Schema.number,
            hello: Schema.string,
          }),
        })
      );

      yield* _(
        pg1.executeCommand({
          sql: 'drop table mytable',
        })
      );

      return rows;
    });

    const rows = await Effect.runPromise(program.pipe(Effect.scoped));

    expect(rows).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          hello: 'cya',
        }),
        expect.objectContaining({
          hello: 'goodbye',
        }),
        expect.objectContaining({
          hello: 'adios',
        }),
      ])
    );
  });
});
