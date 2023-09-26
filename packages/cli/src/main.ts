import {
  makePgPool,
  DecoratedInsert,
  DecoratedDelete,
  DecoratedUpdate,
} from '@jmorecroft67/pg-stream-core';
import { Chunk, Effect, Queue } from 'effect';
import * as Schema from '@effect/schema/Schema';

const program = Effect.gen(function* (_) {
  const pgPool = yield* _(
    makePgPool({
      username: 'postgres',
      password: 'topsecret',
      host: 'localhost',
      database: 'postgres',
      replication: true,
      port: 5432,
      useSSL: true,
      min: 1,
      max: 5,
      timeToLive: '5 minutes',
    })
  );

  const pg1 = yield* _(pgPool.get());
  const pg2 = yield* _(pgPool.get());

  const queue = yield* _(
    Queue.unbounded<DecoratedInsert | DecoratedDelete | DecoratedUpdate>()
  );

  yield* _(pg2.command('create publication test_pub for all tables'));
  yield* _(
    pg2.query(
      'CREATE_REPLICATION_SLOT test_slot TEMPORARY LOGICAL pgoutput NOEXPORT_SNAPSHOT',
      Schema.any
    )
  );

  const fibre = Effect.runFork(
    Effect.timeout(
      pg2.recvlogical({
        slotName: 'test_slot',
        publicationNames: ['test_pub'],
        processor: {
          filter: (
            msg
          ): msg is DecoratedInsert | DecoratedDelete | DecoratedUpdate =>
            msg.type === 'Insert' ||
            msg.type === 'Delete' ||
            msg.type === 'Update',
          process: (data) => queue.offerAll(data),
        },
      }),
      '2 seconds'
    )
  );

  yield* _(
    pg1.command('create table test (id integer primary key, greeting varchar)')
  );
  yield* _(pg1.command('alter table test replica identity full'));
  yield* _(pg1.command(`insert into test values (1, 'hello'), (2, 'gday')`));
  yield* _(pg1.command(`update test set greeting = 'hi' where id = 1`));
  yield* _(pg1.command('delete from test where id = 1'));
  yield* _(pg1.command('drop table test'));
  yield* _(pg1.command('drop publication if exists test_pub'));

  yield* _(fibre.await());

  const chunk = yield* _(queue.takeAll());

  return Chunk.toReadonlyArray(chunk).map((log) =>
    log.type === 'Insert'
      ? { inserted: log.newRecord }
      : log.type === 'Update'
      ? { deleted: log.oldRecord, inserted: log.newRecord }
      : { deleted: log.oldRecord }
  );
});

Effect.runPromise(
  program.pipe(Effect.scoped, Effect.catchAllDefect(Effect.logFatal))
).then((streamed) => {
  console.log('returned:', JSON.stringify(streamed, null, 2));
});
