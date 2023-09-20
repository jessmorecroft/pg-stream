import { Chunk, Deferred, Effect, Hub, Queue, Stream, identity } from 'effect';
import { makePgPool } from './core';
import { describe, it, expect } from 'vitest';
import * as Schema from '@effect/schema/Schema';
import { walLsnFromString } from './util/wal-lsn-from-string';
import { PgOutputDecoratedMessageTypes } from './pg-client/transform-log-data';
import _ from 'lodash';

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
        pg1.command({
          sql: `create publication test_pub for all tables`,
        })
      );

      yield* _(
        pg1.query({
          sql: `CREATE_REPLICATION_SLOT test_slot TEMPORARY LOGICAL pgoutput NOEXPORT_SNAPSHOT`,
          schema: Schema.tuple(
            Schema.struct({
              consistent_point: walLsnFromString,
            })
          ),
        })
      );

      const hub = yield* _(Hub.unbounded<PgOutputDecoratedMessageTypes>());

      const listening = yield* _(Deferred.make<never, void>());

      const fibre = Effect.runFork(
        Effect.race(
          Stream.fromHub(hub, { scoped: true }).pipe(
            Effect.tap(() => Deferred.complete(listening, Effect.unit)),
            Effect.flatMap((_) =>
              _.pipe(
                Stream.filter(({ type }) => type === 'Insert'),
                Stream.take(3),
                Stream.runCollect,
                Effect.map(Chunk.toReadonlyArray)
              )
            ),
            Effect.scoped
          ),
          Deferred.await(listening).pipe(
            Effect.flatMap(() =>
              pg1.recvlogical({
                slotName: 'test_slot',
                publicationNames: ['test_pub'],
                process: (data) => hub.publish(data),
              })
            ),
            Effect.flatMap(() => Effect.never)
          )
        )
      );

      yield* _(
        pg2.command({
          sql: 'create table if not exists test ( greeting varchar )',
        })
      );
      yield* _(pg2.command({ sql: "insert into test values ('hello')" }));
      yield* _(pg2.command({ sql: "insert into test values ('hi')" }));
      yield* _(pg2.command({ sql: "insert into test values ('howdy')" }));
      yield* _(
        pg2.command({
          sql: 'drop table test',
        })
      );
      yield* _(pg2.command({ sql: `drop publication if exists test_pub` }));

      return yield* _(fibre.await().pipe(Effect.flatMap(identity)));
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

  it('should persist replication state between restarts', async () => {
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

      const pg = yield* _(pgPool.get());

      yield* _(
        pg.command({
          sql: `create publication test_pub2 for all tables`,
        })
      );

      yield* _(
        pg.query({
          sql: `CREATE_REPLICATION_SLOT test_slot2 LOGICAL pgoutput NOEXPORT_SNAPSHOT`,
          schema: Schema.tuple(
            Schema.struct({
              consistent_point: walLsnFromString,
            })
          ),
        })
      );

      const queue = yield* _(Queue.unbounded<PgOutputDecoratedMessageTypes>());

      const recvlogical = Effect.gen(function* (_) {
        const stop = yield* _(Deferred.make<never, void>());

        const fibre = Effect.runFork(
          pgPool.get().pipe(
            Effect.flatMap((streamer) =>
              Effect.race(
                streamer.recvlogical({
                  slotName: 'test_slot2',
                  publicationNames: ['test_pub2'],
                  process: (data) => queue.offer(data),
                }),
                Deferred.await(stop)
              ).pipe(Effect.tap(() => pgPool.invalidate(streamer)))
            ),
            Effect.scoped
          )
        );

        return Deferred.complete(stop, Effect.unit).pipe(
          Effect.tap(() => fibre.await())
        );
      });

      yield* _(
        pg.command({
          sql: 'create table if not exists test2 ( numbers integer )',
        })
      );

      yield* _(pg.command({ sql: 'insert into test2 values (1)' }));
      yield* _(pg.command({ sql: 'insert into test2 values (2)' }));
      yield* _(pg.command({ sql: 'insert into test2 values (3)' }));

      yield* _(Effect.flatMap(recvlogical, Effect.delay('1 seconds')));

      yield* _(pg.command({ sql: 'insert into test2 values (4)' }));
      yield* _(pg.command({ sql: 'insert into test2 values (5)' }));
      yield* _(pg.command({ sql: 'insert into test2 values (6)' }));

      yield* _(Effect.flatMap(recvlogical, Effect.delay('1 seconds')));

      yield* _(pg.command({ sql: 'insert into test2 values (7)' }));
      yield* _(pg.command({ sql: 'insert into test2 values (8)' }));
      yield* _(pg.command({ sql: 'insert into test2 values (9)' }));

      yield* _(Effect.flatMap(recvlogical, Effect.delay('1 seconds')));

      yield* _(
        pg.command({
          sql: 'drop table test2',
        })
      );
      yield* _(pg.command({ sql: `drop publication if exists test_pub2` }));

      yield* _(
        pg.command({
          sql: 'DROP_REPLICATION_SLOT test_slot2',
        })
      );

      return yield* _(queue.takeAll().pipe(Effect.map(Chunk.toReadonlyArray)));
    });

    const results = await Effect.runPromise(program.pipe(Effect.scoped));

    expect(results.filter(({ type }) => type === 'Insert')).toEqual(
      _.times(9, (num) => ({
        name: 'test2',
        namespace: 'public',
        newRecord: {
          numbers: num + 1,
        },
        type: 'Insert',
      }))
    );
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
        pg1.command({
          sql: 'create table if not exists mytable ( id serial, hello varchar )',
        })
      );

      yield* _(
        Effect.all(
          [
            pg1.command({
              sql: "insert into mytable ( hello ) values ('cya')",
            }),
            pg2.command({
              sql: "insert into mytable ( hello ) values ('goodbye')",
            }),
            pg3.command({
              sql: "insert into mytable ( hello ) values ('adios')",
            }),
          ],
          { concurrency: 'unbounded' }
        )
      );

      const rows = yield* _(
        pg1.query({
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
        pg1.command({
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
