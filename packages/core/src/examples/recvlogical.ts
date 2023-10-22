import { Schema } from '@effect/schema';
import {
  DecoratedInsert,
  PgOutputDecoratedMessageTypes,
  makePgPool,
} from '../index';
import { Deferred, Effect, Exit, Queue, Stream } from 'effect';
import { inspect } from 'util';

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

  const pg1 = yield* _(pgPool.get());
  const pg2 = yield* _(pgPool.get());

  yield* _(pg1.query('CREATE PUBLICATION recvlogical_example FOR ALL TABLES'));

  yield* _(
    pg1.query(
      'CREATE_REPLICATION_SLOT temp_slot TEMPORARY LOGICAL pgoutput NOEXPORT_SNAPSHOT',
      Schema.any
    )
  );

  const queue = yield* _(Queue.unbounded<DecoratedInsert>());

  const signal = yield* _(Deferred.make<never, void>());

  const messages = yield* _(
    Effect.zipRight(
      pg1.recvlogical({
        slotName: 'temp_slot',
        publicationNames: ['recvlogical_example'],
        processor: {
          filter: (
            msg: PgOutputDecoratedMessageTypes
          ): msg is DecoratedInsert => msg.type === 'Insert',
          process: (_, data) => queue.offerAll(data),
        },
        signal,
      }),
      pg2
        .query(
          `
  CREATE TABLE IF NOT EXISTS example
   (id INTEGER PRIMARY KEY, message VARCHAR NOT NULL);
  INSERT INTO example VALUES (1, 'hello'), (2, 'world');
  DROP TABLE example;`
        )
        .pipe(
          Effect.flatMap(() =>
            Stream.fromQueue(queue).pipe(
              Stream.flatMap(
                Schema.parse(
                  Schema.struct({
                    newRecord: Schema.struct({
                      id: Schema.number,
                      message: Schema.string,
                    }),
                  })
                )
              ),
              Stream.takeUntil(
                ({ newRecord: { message } }) => message === 'world'
              ),
              Stream.runCollect
            )
          ),
          Effect.tap(() => Deferred.done(signal, Exit.succeed(undefined)))
        ),
      {
        concurrent: true,
      }
    )
  );

  yield* _(
    Effect.forEach(messages, (msg, index) =>
      Effect.log(`message ${index}: ${inspect(msg.newRecord)}`)
    )
  );

  yield* _(pg1.query('DROP PUBLICATION recvlogical_example'));
});

Effect.runPromise(
  program.pipe(Effect.scoped, Effect.catchAllDefect(Effect.logFatal))
);
