# pg-stream-core

A limited-functionality PostgreSQL client library for simple queries and most significantly, reliable logical replication streaming using the pgoutput protocol.

This lib is built on top of the awesome [Effect TS](https://www.effect.website) libraries. It talks directly to Postgres via the wire protocol. (ie. it does not utilise any existing pg libraries)

## structure

The pg-stream lib exposes a `PgClient` constructor and a `PgClientPool` constructor. Both of these are scoped to ensure any connections are closed when you are done.

The `PgClient` object provides methods for querying and pulling a logical replication feed:

### Querying

The querying methods send one or more SQL statements to the Postgres server for execution, and return any parsed results. If you use one of the non-raw variants you also must specify schemas for validation.

```ts
// Execute SQL, return any results as parsed result sets according to the
// supplied parser options.
<O extends MakeValueTypeParserOptions>
queryRaw(
  sql: string,
  parserOptions?: O
): Effect.Effect<
  Record<string, ValueType<O>>[][], PgClientError>

// Execute SQL, return any results as a list of parsed result sets that
// are each then validated/parsed by the supplied schema. Note each schema
// validates a result set, so allows you to potentially constrain the number
// of records for example.
<S extends Schema.Schema<any, any, any>>
queryMany(
  sqlOrOptions:
    | string
    | { sql: string; parserOptions: MakeValueTypeParserOptions },
  schema: S
): Effect.Effect<
  readonly Schema.Schema.Type<S>[],
  PgClientError,
  Schema.Schema.Context<S>>

// Execute SQL, return any results as a tuple of parsed result sets that are
// validated/parsed by the supplied schemas. If no schema, no result sets are
// expected and nothing is returned, if one schema a single validated result
// set is returned, and if multiple schemas a tuple of result sets are
// returned, each validated by the corresponding schema in order of receipt.
// Note each schema validates a result set, so allows you to potentially
// constrain the number of records for example.
<S extends Schema.Schema<any, any, any>[]>
query(
  sqlOrOptions:
    | string
    | { sql: string; parserOptions: MakeValueTypeParserOptions },
  ...schemas: [...S]
): Effect.Effect<
  NoneOneOrMany<SchemaTypes<S>>,
  PgClientError,
  Schema.Schema.Context<S[number]>>

// Execute SQL, return results as a stream of parsed results where each element
// is a row/ index tuple, such that multiple result sets will be returned in
// sequence with the index resetting to zero when we reach the beginning of the
// next result set.
<O extends MakeValueTypeParserOptions>
queryStreamRaw(
  sql: string,
  parserOptions?: O
): Stream.Stream<[Record<string, ValueType<O>>, number], PgClientError>

// Execute SQL, return results as a stream of parsed and validated results where
// each element is a row/ index tuple, such that multiple result sets will be
// returned in sequence with the index resetting to zero when we reach the
// beginning of the next result set. The records in each result set will be
// validated by the corresponding schema in order of receipt. Note each schema
// must define a result set record, which is unlike query or queryMany, which
// expect each schema to define a result set.
<S extends Schema.Schema<any, any, any>[]>
queryStream(
  sqlOrOptions:
    | string
    | { sql: string; parserOptions?: MakeValueTypeParserOptions },
  ...schemas: [...S]
): Stream.Stream<
  readonly [SchemaTypesUnion<S>, number],
  PgClientError,
  Schema.Schema.Context<S[number]>>

```

- `sql`: A string containing one or more SQL statements.
- `parserOptions`: Parser options that determine how each column's values are parsed from the string values that are returned by the server. This happens before any input schemas are applied. If you do not specify parser options the default behaviour is to parse all supported types.
- `sqlOrOptions`: SQL or an object containing SQL and parser options.
- `schemas`: Zero, one or more schemas that are used to validate any results. Each schema must define the either type of a result set (`query`, `queryMany`) or the type of a result set record (`queryStream`).

#### No result

Here's how to query when you don't expect a result.

```typescript
yield * _(pg.query('CREATE TABLE my_table (id SERIAL, message VARCHAR)'));
```

#### Single result set

If you expect a result set, you must specify a schema.

```ts
const results =
  yield *
  _(
    pg.query(
      `SELECT * FROM my_table`,
      Schema.tuple(
        Schema.struct({
          id: Schema.number,
          message: Schema.string,
        }),
      ),
    ),
  );
// const results: readonly [{
//    readonly id: number;
//    readonly message: string;
// }]
```

#### Multiple result sets

You can easily handle multiple result sets too, which is handy because your SQL will be executed as an implicit transaction. This allows you to safely do things that involve multiple operations that must either all succeed or all fail, and in the former case you can easily obtain all the results produced.

```typescript
const payment = 100;

const [a, b] =
  yield *
  _(
    pg.query(
      `
-- Check if the source account has enough balance
DO
$$
DECLARE
    source_balance NUMERIC;
BEGIN
    SELECT amount INTO source_balance FROM balance WHERE account_id = 'A' FOR UPDATE;
    IF source_balance < ${payment} THEN
        RAISE EXCEPTION 'Insufficient funds in source account';
    END IF;
END
$$;

-- If the source account has enough balance, proceed with the transfer
UPDATE balance SET amount = amount - ${payment} WHERE account_id = 'A' RETURNING *;
UPDATE balance SET amount = amount + ${payment} WHERE account_id = 'B' RETURNING *;
`,
      Schema.tuple(
        Schema.struct({
          account_id: Schema.literal('A'),
          amount: DecimalFromSelf,
        }),
      ),
      Schema.tuple(
        Schema.struct({
          account_id: Schema.literal('B'),
          amount: DecimalFromSelf,
        }),
      ),
    ),
  );
// const a: readonly [{
//    readonly account_id: "A";
//    readonly amount: Decimal;
// }]
// const b: readonly [{
//    readonly account_id: "B";
//    readonly amount: Decimal;
// }]
```

#### Multiple result sets again

Alternatively you might just prefer to define a single schema that applies for all result sets, of which there may be any number (including zero). `queryMany` may be used for this.

```typescript
const ids =
  yield *
  _(
    pg.queryMany(
      `
SELECT * FROM my_table;
SELECT * FROM my_other_table;`,
      Schema.nonEmptyArray(
        Schema.struct({
          id: Schema.number,
        }),
      ),
    ),
  );
// const ids: readonly (readonly [{
//     readonly id: number;
// }, ...{
//     readonly id: number;
// }[]])[]
```

#### Streaming a single result set

If you have a query that returns a lot of results, you may wish to use a streaming query. This will read the results as fast as you can consume them, applying backpressure at the socket level so that your client does not receive more data than it can handle.

```typescript
const dataStream = pg.queryStreamRaw(`select * from bigdata;`, NONE_ENABLED_PARSER_OPTIONS);
// const dataStream: Stream.Stream<never, PgClientError, [Record<string, ValueType<{}>>, number]>
```

#### Multiple result sets in a stream, with validation

Both `queryStreamRaw` and `queryStream` will handle multiple result sets, though `queryStream` let's you also specify schemas to validate (and transform, and type) your results.

```typescript
const petStream = pg.queryStream(
  `SELECT * FROM dog;
SELECT * FROM cat;
SELECT * FROM hamster;
`,
  Schema.struct({
    kind: Schema.literal('dog'),
    bark: Schema.literal('woof', 'arf', 'bowowow'),
  }),
  Schema.struct({
    kind: Schema.literal('cat'),
    lives: Schema.int()(Schema.number),
  }),
  Schema.struct({
    kind: Schema.literal('hamster'),
    wheel_rpm: Schema.int()(Schema.number),
  }),
);
// const petStream: Stream.Stream<never, PgClientError, readonly [{
//     readonly bark: "woof" | "arf" | "bowowow";
//     readonly kind: "dog";
// } | {
//     readonly lives: number;
//     readonly kind: "cat";
// } | {
//     readonly kind: "hamster";
//     readonly wheel_rpm: number;
// }, number]>
```

### Logical replication

The logical replication capability of this library, provided by the single `recvlogical` method, is its primary feature. It aims to provide a notification mechanism that might form the basis of a robust Change-Data-Capture (CDC) solution.

```typescript
// You must implement this to process transaction logs.
export interface XLogProcessor<E, T extends PgOutputDecoratedMessageTypes> {
  // Filter the messages you are interested in.
  filter(msg: PgOutputDecoratedMessageTypes): msg is T;
  // Key generator may be one of:
  // - "serial" - everything in serial (default)
  // - "table" - partition key per table
  // - Custom partition key - some record property for example, like a customer
  //                          identifier
  key?: ((msg: T) => string) | 'serial' | 'table';
  // End of chunk allows a client to specify how events should be chunked/
  // grouped by specifying the last message in every chunk. For example, to
  // chunk in transaction batches this could return true when the message
  // is a commit message. The resultant chunk items are then grouped by key and
  // each group is sent to the process method.
  endOfChunk?: (msg: T) => boolean;
  // Process a chunk of messages grouped by key and optionally chunked according
  // to the endOfChunk method.
  process(key: string, chunk: Chunk.Chunk<T>): Effect.Effect<void, E>;
}

// The `recvlogical` method starts a logical replication stream from the server
// and feeds transaction log updates to a user supplied processor. The processor
// may optionally specify concurrency constraints, filtering and an "end of
// chunk" marker. The replication stream will only supply the processor as fast
// as it is able to process data, or more specifically, back pressure is applied
// to the server at the socket level. The transaction log checkpoint is updated
// at the Postgres server when the processor has processed all logs up to and
// including that point.
<E, T extends PgOutputDecoratedMessageTypes>
recvlogical({
  slotName,
  publicationNames,
  processor,
  signal,
}: {
  slotName: string;
  publicationNames: string[];
  processor: XLogProcessor<E, T>;
  signal?: Deferred.Deferred<void>;
}): Effect.Effect<void, PgClientError | XLogProcessorError<E>>
```

- `options`:
  - `slotName`: The name of the replication slot.
  - `publicationNames`: The names of the publications being subscribed to.
  - `processor`: A processor for the handling of streamed records, along with an optional partition key strategy (to constrain concurrency), optional record filtering and an optional "end of chunk" predicate. If no partition key strategy
    is defined, we default to processing updates serially. If no "end of chunk"
    predicate is defined then chunking behaviour is undefined.
  - `signal`: A optional signal for stopping the replication stream, which then allows continued use of the connection for further queries or streaming.

#### Example

```typescript
import {
  DecoratedDelete,
  DecoratedInsert,
  DecoratedUpdate,
  PgOutputDecoratedMessageTypes,
  makePgPool,
} from "@jmorecroft67/pg-stream-core";
import { Chunk, Console, Deferred, Effect, Exit, Queue, Stream } from "effect";

const program = Effect.gen(function* (_) {
  const pgPool = yield* _(
    makePgPool({
      host: "localhost",
      port: 5432,
      useSSL: false,
      database: "postgres",
      username: "postgres",
      password: "topsecret",
      min: 1,
      max: 10,
      timeToLive: "1 minutes",
      replication: true,
    })
  );

  const pg1 = yield* _(pgPool.get);
  const pg2 = yield* _(pgPool.get);

  // Create a publication and a temporary slot for test purposes. In a
  // production scenario, assuming you wanted to ensure you don't miss
  // events, you would use a permanent slot and would probably do this
  // one-time setup independent of your streaming code.

  // note unlike slots we're not able to create a temporary publication
  // in postgres, so we explicitly drop the publication after we're done,
  // at end of this scope.
  yield* _(
    Effect.acquireRelease(
      pg1.queryRaw(`CREATE PUBLICATION example_publication FOR ALL TABLES`),
      () =>
        pg1.queryRaw(`DROP PUBLICATION example_publication`).pipe(Effect.ignore)
    )
  );

  yield* _(
    pg1.queryRaw(
      "CREATE_REPLICATION_SLOT example_slot TEMPORARY LOGICAL pgoutput NOEXPORT_SNAPSHOT"
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
        slotName: "example_slot",
        publicationNames: ["example_publication"],
        processor: {
          filter: (
            msg: PgOutputDecoratedMessageTypes
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
              Stream.takeUntil((msg) => msg.type === "Delete"),
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
      "type",
      "key",
      "oldRecord",
      "newRecord",
    ])
  );
});

Effect.runPromise(
  program.pipe(Effect.scoped, Effect.catchAllDefect(Effect.logFatal))
);
```

The output of this running against a test postgres docker container with self-signed certficate is:

```
timestamp=2024-04-07T04:04:55.787Z level=INFO fiber=#0 message="CREATE PUBLICATION"
timestamp=2024-04-07T04:04:55.800Z level=INFO fiber=#0 message=CREATE_REPLICATION_SLOT
timestamp=2024-04-07T04:04:55.820Z level=INFO fiber=#67 message="CREATE TABLE"
timestamp=2024-04-07T04:04:55.820Z level=INFO fiber=#67 message="INSERT 0 2"
timestamp=2024-04-07T04:04:55.820Z level=INFO fiber=#67 message="ALTER TABLE"
timestamp=2024-04-07T04:04:55.820Z level=INFO fiber=#67 message="UPDATE 1"
timestamp=2024-04-07T04:04:55.820Z level=INFO fiber=#67 message="DELETE 1"
timestamp=2024-04-07T04:04:55.821Z level=INFO fiber=#67 message="DROP TABLE"
timestamp=2024-04-07T04:04:55.842Z level=INFO fiber=#66 message="COPY 0"
timestamp=2024-04-07T04:04:55.842Z level=INFO fiber=#66 message=START_REPLICATION
┌─────────┬──────────┬──────────────────┬─────────────────────────────┬───────────────────────────────┐
│ (index) │ type     │ key              │ oldRecord                   │ newRecord                     │
├─────────┼──────────┼──────────────────┼─────────────────────────────┼───────────────────────────────┤
│ 0       │ 'Insert' │ 'public.example' │                             │ { id: 1, message: 'hello' }   │
│ 1       │ 'Insert' │ 'public.example' │                             │ { id: 2, message: 'world' }   │
│ 2       │ 'Update' │ 'public.example' │ { id: 1, message: 'hello' } │ { id: 1, message: 'goodbye' } │
│ 3       │ 'Delete' │ 'public.example' │ { id: 2, message: 'world' } │                               │
└─────────┴──────────┴──────────────────┴─────────────────────────────┴───────────────────────────────┘
timestamp=2024-04-07T04:04:55.852Z level=INFO fiber=#0 message="DROP PUBLICATION"
```

### earlier versions

This version is a rewrite of the existing 1.X lib, which did not use Effect TS but relied more on fp-ts and more direct use of NodeJS streams. This new version is simpler and more flexible, and should integrate easily with Effect TS based applications.

### alternatives

This lib is very much a work in progress, has NOT been widely tested in the field and any use in a production environment should be carefully considered! This lib was written from a desire to have something simple, flexible and easily deployable to reliably push xlogs from Postgres, with backpressure support. It was also a great excuse to re-invent a few wheels and learn more about FP!

Thankfully there are a number of great, mature alternatives that may do what you're after.

- [postgres](https://www.npmjs.com/package/postgres) - the relatively new kid on the block. This is a fully featured JavaScript library that supports logical replication via its [realtime subscribe](https://www.npmjs.com/package/postgres#realtime-subscribe) feature. This feature provides a simple hook to receive insert, update and delete events, though events are delivered at the rate they are produced with an underlying NodeJS stream in (flowing mode)[https://nodejs.org/api/stream.html#two-reading-modes] (ie. no backpressure) and there is currently no support for persistent (ie. non-temporary) slots.
- [pg](https://www.npmjs.com/package/pg) - the default choice of client for connecting to Postgres from JavaScript. For logical replication scenarios there is the [pg-copy-streams](https://www.npmjs.com/package/pg-copy-streams) lib built on top of this, which I initially investigated using before naively deciding to do it all myself!
- [psql](https://www.postgresql.org/docs/current/app-psql.html) - the standard Postgres interactive client. Logical replication using the SQL interface is demonstrated [here](https://www.postgresql.org/docs/current/logicaldecoding-example.html).
- [pg_recvlogical](https://www.postgresql.org/docs/current/app-pgrecvlogical.html) - a utility for logical replication streaming shipped with Postgres, and inspiration for the `recvlogical` function name! An example of its use with the wal2json plugin is [here](https://access.crunchydata.com/documentation/wal2json/2.0/). (Note to keep things simple the pg-stream-core lib only uses the built-in pgoutput plugin)
- [Debezium](https://debezium.io/) - the batteries-included solution for change data capture (CDC) from Postgres, or a bunch of other databases. This one is probably the one to choose if you're trying to implement CDC and require a battle-tested solution.
