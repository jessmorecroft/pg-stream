import {
  DecoratedDelete,
  DecoratedInsert,
  DecoratedUpdate,
  PgOutputDecoratedMessageTypes,
  makePgPool,
} from '@jmorecroft67/pg-stream-core';
import { Chunk, Console, Deferred, Effect, Exit, Queue, Stream } from 'effect';

const program = Effect.gen(function* (_) {
  const pgPool = yield* _(
    makePgPool({
      host: 'localhost',
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

  const pg1 = yield* _(pgPool.get);
  const pg2 = yield* _(pgPool.get);

  // Create a publication and a temporary slot for test purposes. In a
  // production scenario, assuming you wanted to ensure you don't miss
  // events, you would use a permanent slot and would probably do this
  // one-time setup independent of your streaming code.

  yield* _(pg1.query('CREATE PUBLICATION example_publication FOR ALL TABLES'));

  yield* _(
    pg1.queryRaw(
      'CREATE_REPLICATION_SLOT example_slot TEMPORARY LOGICAL pgoutput NOEXPORT_SNAPSHOT'
    )
  );

  type InsertOrUpdateOrDelete =
    | DecoratedInsert
    | DecoratedUpdate
    | DecoratedDelete;

  // Bounded queue, which means pushing to this queue will be blocked
  // by a slow consumer, which in turn means that our consumption of
  // logs will also be blocked. This is what we want - a slow consumer
  // should slow our consumption of logs so that the rate we receive
  // is no more than the rate we're able to consume.

  const queue = yield* _(Queue.bounded<[string, InsertOrUpdateOrDelete]>(16));

  const signal = yield* _(Deferred.make<void>());

  const changes = yield* _(
    Effect.zipRight(
      pg1.recvlogical({
        slotName: 'example_slot',
        publicationNames: ['example_publication'],
        processor: {
          filter: (
            msg: PgOutputDecoratedMessageTypes
          ): msg is InsertOrUpdateOrDelete =>
            msg.type === 'Insert' ||
            msg.type === 'Update' ||
            msg.type === 'Delete',
          key: 'table',
          process: (key, data) =>
            queue.offerAll(Chunk.map(data, (_) => [key, _])),
        },
        signal,
      }),
      pg2
        .query(
          `
  CREATE TABLE example
   (id SERIAL PRIMARY KEY, message VARCHAR NOT NULL);
  INSERT INTO example VALUES (1, 'hello'), (2, 'world');
  ALTER TABLE example REPLICA IDENTITY FULL;
  UPDATE example SET message = 'goodbye'
    WHERE id = 1;
  DELETE FROM example
    WHERE id = 2;
  DROP TABLE example;`
        )
        .pipe(
          Effect.flatMap(() =>
            Stream.fromQueue(queue).pipe(
              Stream.map(([key, data]) => ({ ...data, key })),
              Stream.takeUntil((msg) => msg.type === 'Delete'),
              Stream.runCollect
            )
          ),
          // All done - tell recvlogical to unsubscribe.
          Effect.tap(() => Deferred.done(signal, Exit.succeed(undefined)))
        ),
      {
        concurrent: true,
      }
    )
  );

  yield* _(
    Console.table(Chunk.toReadonlyArray(changes), [
      'type',
      'key',
      'oldRecord',
      'newRecord',
    ])
  );

  // Cleanup our test publication.
  yield* _(pg1.query('DROP PUBLICATION example_publication'));
});

Effect.runPromise(
  program.pipe(Effect.scoped, Effect.catchAllDefect(Effect.logFatal))
);
