import { Chunk, Deferred, Effect, Queue, Stream, identity } from 'effect';
import { makePgPool } from './core';
import { describe, it, expect } from 'vitest';
import * as Schema from '@effect/schema/Schema';
import { walLsnFromString } from './util/wal-lsn-from-string';
import {
  DecoratedBegin,
  DecoratedCommit,
  DecoratedInsert,
  PgOutputDecoratedMessageTypes,
} from './pg-client/transform-log-data';
import _ from 'lodash';

describe('core', () => {
  it('should run commands , queries', async () => {
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
        pg1.command(
          'CREATE TABLE IF NOT EXISTS test_query ( id SERIAL, hello VARCHAR )'
        )
      );

      yield* _(
        Effect.all(
          [
            pg1.command("INSERT INTO test_query ( hello ) VALUES ('cya')"),
            pg2.command("INSERT INTO test_query ( hello ) VALUES ('goodbye')"),
            pg3.command("INSERT INTO test_query ( hello ) VALUES ('adios')"),
          ],
          { concurrency: 'unbounded' }
        )
      );

      const rows = yield* _(
        pg1.query(
          'SELECT * FROM test_query',
          Schema.nonEmptyArray(
            Schema.struct({
              id: Schema.number,
              hello: Schema.string,
            })
          )
        )
      );

      yield* _(pg1.command('DROP TABLE test_query'));

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

      yield* _(pg1.command('CREATE PUBLICATION test_pub FOR ALL TABLES'));

      yield* _(
        pg1.query(
          'CREATE_REPLICATION_SLOT test_slot TEMPORARY LOGICAL pgoutput NOEXPORT_SNAPSHOT',
          Schema.tuple(
            Schema.struct({
              consistent_point: walLsnFromString,
            })
          )
        )
      );

      const queue = yield* _(Queue.unbounded<PgOutputDecoratedMessageTypes>());

      const fibre = Effect.runFork(
        Effect.race(
          Stream.fromQueue(queue).pipe(
            Stream.takeUntil(({ type }) => type === 'Delete'),
            Stream.runCollect,
            Effect.map(Chunk.toReadonlyArray)
          ),
          pg1
            .recvlogical({
              slotName: 'test_slot',
              publicationNames: ['test_pub'],
              processor: {
                filter: (
                  msg: PgOutputDecoratedMessageTypes
                ): msg is Exclude<
                  PgOutputDecoratedMessageTypes,
                  DecoratedBegin | DecoratedCommit
                > => msg.type !== 'Begin' && msg.type !== 'Commit',
                process: (data) => queue.offerAll(data),
              },
            })
            .pipe(Effect.flatMap(() => Effect.never))
        )
      );

      yield* _(
        pg2.command(`CREATE TABLE IF NOT EXISTS test_replication
                    (id INTEGER PRIMARY KEY,
                     word VARCHAR NOT NULL,
                     flag BOOLEAN,
                     matrix INT[][],
                     blob JSON)`)
      );
      yield* _(
        pg2.command('ALTER TABLE test_replication REPLICA IDENTITY FULL')
      );
      yield* _(
        pg2.command(`INSERT INTO test_replication VALUES
                 (1, 'hello', null, null, '{"meaning":[42]}'),
                 (2, 'gday', true, array[array[1,2,3], array[4,5,6]], null)`)
      );
      yield* _(
        pg2.command("UPDATE test_replication SET word = 'hiya' WHERE id = 1")
      );
      yield* _(pg2.command('DELETE FROM test_replication WHERE id = 1'));
      yield* _(pg2.command('DROP TABLE test_replication'));
      yield* _(pg2.command('DROP PUBLICATION IF EXISTS test_pub'));

      return yield* _(fibre.await().pipe(Effect.flatMap(identity)));
    });

    const results = await Effect.runPromise(program.pipe(Effect.scoped));

    expect(results).toEqual([
      expect.objectContaining({
        type: 'Relation',
        name: 'test_replication',
        namespace: 'public',
        columns: [
          expect.objectContaining({ name: 'id', dataTypeName: 'int4' }),
          expect.objectContaining({
            name: 'word',
            dataTypeName: 'varchar',
          }),
          expect.objectContaining({
            name: 'flag',
            dataTypeName: 'bool',
          }),
          expect.objectContaining({
            name: 'matrix',
            dataTypeName: 'int4[]',
          }),
          expect.objectContaining({
            name: 'blob',
            dataTypeName: 'json',
          }),
        ],
      }),
      expect.objectContaining({
        type: 'Insert',
        namespace: 'public',
        name: 'test_replication',
        newRecord: {
          id: 1,
          word: 'hello',
          flag: null,
          matrix: null,
          blob: {
            meaning: [42],
          },
        },
      }),
      expect.objectContaining({
        type: 'Insert',
        namespace: 'public',
        name: 'test_replication',
        newRecord: {
          id: 2,
          word: 'gday',
          flag: true,
          matrix: [
            [1, 2, 3],
            [4, 5, 6],
          ],
          blob: null,
        },
      }),
      expect.objectContaining({
        type: 'Update',
        namespace: 'public',
        name: 'test_replication',
        newRecord: {
          id: 1,
          word: 'hiya',
          flag: null,
          matrix: null,
          blob: { meaning: [42] },
        },
        oldRecord: {
          id: 1,
          word: 'hello',
          flag: null,
          matrix: null,
          blob: { meaning: [42] },
        },
      }),
      expect.objectContaining({
        namespace: 'public',
        name: 'test_replication',
        type: 'Delete',
        oldRecord: {
          id: 1,
          word: 'hiya',
          flag: null,
          matrix: null,
          blob: { meaning: [42] },
        },
      }),
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

      yield* _(pg.command('CREATE PUBLICATION test_pub2 FOR ALL TABLES'));

      yield* _(
        pg.query(
          'CREATE_REPLICATION_SLOT test_slot2 LOGICAL pgoutput NOEXPORT_SNAPSHOT',
          Schema.tuple(
            Schema.struct({
              consistent_point: walLsnFromString,
            })
          )
        )
      );

      const queue = yield* _(Queue.unbounded<DecoratedInsert>());

      const recvlogical = Effect.gen(function* (_) {
        const stop = yield* _(Deferred.make<never, void>());

        const fibre = Effect.runFork(
          pgPool.get().pipe(
            Effect.flatMap((streamer) =>
              Effect.race(
                streamer.recvlogical({
                  slotName: 'test_slot2',
                  publicationNames: ['test_pub2'],
                  processor: {
                    filter: (msg): msg is DecoratedInsert =>
                      msg.type === 'Insert',
                    process: (data) => queue.offerAll(data),
                  },
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
        pg.command(
          'CREATE TABLE IF NOT EXISTS test_recovery ( numbers INTEGER )'
        )
      );

      yield* _(pg.command('INSERT INTO test_recovery VALUES (1)'));
      yield* _(pg.command('INSERT INTO test_recovery VALUES (2)'));
      yield* _(pg.command('INSERT INTO test_recovery VALUES (3)'));

      yield* _(Effect.flatMap(recvlogical, Effect.delay('1 seconds')));

      yield* _(pg.command('INSERT INTO test_recovery VALUES (4)'));
      yield* _(pg.command('INSERT INTO test_recovery VALUES (5)'));
      yield* _(pg.command('INSERT INTO test_recovery VALUES (6)'));

      yield* _(Effect.flatMap(recvlogical, Effect.delay('1 seconds')));

      yield* _(pg.command('INSERT INTO test_recovery VALUES (7)'));
      yield* _(pg.command('INSERT INTO test_recovery VALUES (8)'));
      yield* _(pg.command('INSERT INTO test_recovery VALUES (9)'));

      yield* _(Effect.flatMap(recvlogical, Effect.delay('1 seconds')));

      yield* _(pg.command('DROP TABLE test_recovery'));
      yield* _(pg.command('DROP PUBLICATION IF EXISTS test_pub2'));
      yield* _(pg.command('DROP_REPLICATION_SLOT test_slot2'));

      return yield* _(queue.takeAll().pipe(Effect.map(Chunk.toReadonlyArray)));
    });

    const results = await Effect.runPromise(program.pipe(Effect.scoped));

    expect(results.filter(({ type }) => type === 'Insert')).toEqual(
      _.times(9, (num) =>
        expect.objectContaining({
          name: 'test_recovery',
          namespace: 'public',
          newRecord: {
            numbers: num + 1,
          },
          type: 'Insert',
        })
      )
    );
  });

  it.each<number>([1500])('should stream changes quickly', async (rowCount) => {
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

      yield* _(pg1.command('CREATE PUBLICATION test_pub3 FOR ALL TABLES'));

      yield* _(
        pg1.query(
          'CREATE_REPLICATION_SLOT test_slot3 TEMPORARY LOGICAL pgoutput NOEXPORT_SNAPSHOT',
          Schema.tuple(
            Schema.struct({
              consistent_point: walLsnFromString,
            })
          )
        )
      );

      const queue = yield* _(Queue.bounded<DecoratedInsert>(10));

      const fibre = Effect.runFork(
        Effect.raceFirst(
          Stream.fromQueue(queue).pipe(
            Stream.scanEffect(
              { test_perf1: 0, test_perf2: 0 } as Record<string, number>,
              (prev, msg) => {
                const last = prev[msg.name];
                if (
                  last !== undefined &&
                  msg.newRecord['id'] === last + 1 &&
                  msg.newRecord['word'] === 'word'
                ) {
                  return Effect.succeed({ ...prev, [msg.name]: last + 1 });
                }
                return Effect.fail(new Error('unexpected ' + msg));
              }
            ),
            Stream.takeUntil(
              (counts) =>
                counts['test_perf1'] === rowCount &&
                counts['test_perf2'] === rowCount
            ),
            Stream.runLast
          ),
          pg1
            .recvlogical({
              slotName: 'test_slot3',
              publicationNames: ['test_pub3'],
              processor: {
                filter: (msg): msg is DecoratedInsert => msg.type === 'Insert',
                process: (data) => queue.offerAll(data),
                key: () => '',
              },
            })
            .pipe(Effect.flatMap(() => Effect.never))
        )
      );

      yield* _(
        pg2.command(
          'CREATE TABLE test_perf1 (id INT PRIMARY KEY, word VARCHAR)'
        )
      );
      yield* _(
        pg2.command(
          'CREATE TABLE test_perf2 (id INT PRIMARY KEY, word VARCHAR)'
        )
      );
      yield* _(
        pg2.command(
          `INSERT INTO test_perf1 SELECT g.*, 'word' FROM generate_series(1, ${rowCount}, 1) AS g(series)`
        )
      );
      yield* _(
        pg2.command(
          `INSERT INTO test_perf2 SELECT g.*, 'word' FROM generate_series(1, ${rowCount}, 1) AS g(series)`
        )
      );

      yield* _(pg2.command('DROP TABLE test_perf1'));
      yield* _(pg2.command('DROP TABLE test_perf2'));
      yield* _(pg2.command('DROP PUBLICATION IF EXISTS test_pub3'));

      return yield* _(fibre.await().pipe(Effect.flatMap(identity)));
    });

    await Effect.runPromise(program.pipe(Effect.scoped));
  });
});
