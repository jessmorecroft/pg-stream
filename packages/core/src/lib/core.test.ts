import { Chunk, Effect, Hub, Stream, identity } from 'effect';
import { makePgPool } from './core';
import { describe, it, expect } from 'vitest';
import * as Schema from '@effect/schema/Schema';
import { walLsnFromString } from './util/wal-lsn-from-string';
import { PgOutputDecoratedMessageTypes } from './pg-client/transform-log-data';

describe('core', () => {
  it('should stream replication', async () => {
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
          replication: true,
        })
      );

      const pg1 = yield* _(pgPool.get());
      const pg2 = yield* _(pgPool.get());

      yield* _(
        pg1.executeCommand({ sql: `DROP PUBLICATION IF EXISTS test_pub` })
      );

      yield* _(
        pg2.executeCommand({
          sql: 'create table if not exists test ( greeting varchar )',
        })
      );

      yield* _(
        pg1.executeCommand({
          sql: `CREATE PUBLICATION test_pub FOR ALL TABLES`,
        })
      );

      yield* _(
        pg1.executeQuery({
          sql: `CREATE_REPLICATION_SLOT test_slot TEMPORARY LOGICAL pgoutput NOEXPORT_SNAPSHOT`,
          schema: Schema.tuple(
            Schema.struct({
              consistent_point: walLsnFromString,
            })
          ),
        })
      );

      const hub = yield* _(Hub.unbounded<PgOutputDecoratedMessageTypes>());

      const fibre = Effect.runFork(
        Stream.fromHub(hub).pipe(
          Stream.filter(({ type }) =>
            ['Insert', 'Update', 'Delete'].includes(type)
          ),
          Stream.take(3),
          Stream.runCollect,
          Effect.map(Chunk.toReadonlyArray)
        )
      );

      const fibre2 = Effect.runFork(
        pg1.recvlogical({
          slotName: 'test_slot',
          publicationNames: ['test_pub'],
          key: (data) => ('name' in data ? data.name : ''),
          process: (data) => hub.publish(data),
        })
      );

      yield* _(
        pg2.executeCommand({ sql: "insert into test values ('hello')" })
      );
      yield* _(pg2.executeCommand({ sql: "insert into test values ('hi')" }));
      yield* _(
        pg2.executeCommand({ sql: "insert into test values ('howdy')" })
      );
      yield* _(
        pg2.executeCommand({
          sql: 'drop table test',
        })
      );

      return yield* _(
        Effect.zipLeft(
          fibre.await().pipe(Effect.flatMap(identity)),
          Effect.zip(
            pg1.end.pipe(Effect.flatMap(() => pgPool.invalidate(pg1))),
            fibre2.await()
          )
        )
      );
    });

    const results = await Effect.runPromise(program.pipe(Effect.scoped));

    expect(results).toEqual([
      {
        name: 'test',
        namespace: 'public',
        newRecord: {
          greeting: 'hello',
        },
        type: 'Insert',
      },
      {
        name: 'test',
        namespace: 'public',
        newRecord: {
          greeting: 'hi',
        },
        type: 'Insert',
      },
      {
        name: 'test',
        namespace: 'public',
        newRecord: {
          greeting: 'howdy',
        },
        type: 'Insert',
      },
    ]);
  });

  it('should run commands, queries', async () => {
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
          timeToLive: '2 minutes',
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
          schema: Schema.nonEmptyArray(
            Schema.struct({
              id: Schema.number,
              hello: Schema.string,
            })
          ),
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
