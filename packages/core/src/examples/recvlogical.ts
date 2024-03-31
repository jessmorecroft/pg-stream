import {
  DecoratedDelete,
  DecoratedInsert,
  DecoratedUpdate,
  PgOutputDecoratedMessageTypes,
  makePgPool,
} from "../index";
import { Chunk, Console, Deferred, Effect, Exit, Queue, Stream } from "effect";

const program = Effect.gen(function* (_) {
  const pgPool = yield* _(
    makePgPool({
      host: "localhost",
      port: 5432,
      useSSL: true,
      database: "postgres",
      username: "postgres",
      password: "topsecret",
      min: 1,
      max: 10,
      timeToLive: "1 minutes",
      replication: true,
    }),
  );

  const pg1 = yield* _(pgPool.get);
  const pg2 = yield* _(pgPool.get);

  //yield* _(pg1.query('CREATE PUBLICATION example_publication FOR ALL TABLES'));

  yield* _(
    pg1.queryRaw(
      "CREATE_REPLICATION_SLOT example_slot TEMPORARY LOGICAL pgoutput NOEXPORT_SNAPSHOT",
    ),
  );

  type InsertOrUpdateOrDelete =
    | DecoratedInsert
    | DecoratedUpdate
    | DecoratedDelete;

  const queue = yield* _(Queue.unbounded<[string, InsertOrUpdateOrDelete]>());

  const signal = yield* _(Deferred.make<void>());

  const changes = yield* _(
    Effect.zipRight(
      pg1.recvlogical({
        slotName: "example_slot",
        publicationNames: ["example_publication"],
        processor: {
          filter: (
            msg: PgOutputDecoratedMessageTypes,
          ): msg is InsertOrUpdateOrDelete =>
            msg.type === "Insert" ||
            msg.type === "Update" ||
            msg.type === "Delete",
          key: "table",
          process: (key, data) =>
            queue.offerAll(Chunk.map(data, (_) => [key, _])),
        },
        signal,
      }),
      pg2
        .query(
          `
  CREATE TABLE example
   (id INTEGER PRIMARY KEY, message VARCHAR NOT NULL);
  INSERT INTO example VALUES (1, 'hello'), (2, 'world');
  UPDATE example SET message = 'goodbye'
    WHERE id = 1;
  DELETE FROM example
    WHERE id = 2;
  DROP TABLE example;`,
        )
        .pipe(
          Effect.flatMap(() =>
            Stream.fromQueue(queue).pipe(
              Stream.map(([key, data]) => ({ ...data, key })),
              Stream.takeUntil((msg) => msg.type === "Delete"),
              Stream.runCollect,
            ),
          ),
          Effect.tap(() => Deferred.done(signal, Exit.succeed(undefined))),
        ),
      {
        concurrent: true,
      },
    ),
  );

  const changesArray = Chunk.toReadonlyArray(changes);

  yield* _(
    Console.table(
      changesArray,
      Object.keys(changesArray.find(({ type }) => type === "Update") ?? []),
    ),
  );

  yield* _(pg1.query("DROP PUBLICATION example_publication"));
});

Effect.runPromise(
  program.pipe(Effect.scoped, Effect.catchAllDefect(Effect.logFatal)),
);
