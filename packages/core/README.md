## pg-stream-core

A limited-functionality PostgreSQL client library for simple queries and most significantly, reliable logical replication streaming using the pgoutput protocol.

This lib is built on top of the awesome [Effect TS](https://www.effect.website) libraries. It talks directly to Postgres via the wire protocol. (ie. it does not utilise any existing pg libraries)

### structure

The pg-stream lib exposes a `PgClient` constructor and a `PgClientPool` constructor. Both of these are scoped to ensure any connections are closed when you are done.

The `PgClient` object provides the following methods:

#### query 

The `query` method sends one or more SQL statements to the Postgres server for execution, and returns zero, one or many sets of results that have been validated by the supplied schemas. 

```typescript
  <S extends [...Schema.Schema<any, any>[]]>
  query(
    sqlOrOptions:
      | string
      | { sql: string; parserOptions: MakeValueTypeParserOptions },
    ...schemas: S
  ): Effect.Effect<
    never,
    | WritableError
    | ReadableError
    | ParseMessageError
    | NoMoreMessagesError
    | PgServerError
    | PgParseError
    | ParseMessageGroupError,
    NoneOneOrMany<SchemaTypes<S>>
  >
```
- `sqlOrOptions`: A string containing one or more SQL statements or an object containing SQL and parser options. The parser options determine how each row is parsed before its entire result set is validated by a supplied schema. If you do not specify parser options the default behavior is to parse all supported types.
- `schemas`: Zero, one or more schemas that are used to validate any results. Each schema must define the type of a result set.

##### Example

```typescript
import { makePgClient } from '@jmorecroft67/pg-stream-core';
import { Effect } from 'effect';
import * as Schema from '@effect/schema/Schema';

const program = Effect.gen(function* (_) {
  const pg = yield* _(
    makePgClient({
      username: 'postgres',
      password: 'topsecret',
      database: 'postgres',
      host: 'localhost',
      port: 5432,
      useSSL: true,
    })
  );

  const test1Schema = Schema.nonEmptyArray(
    Schema.struct({
      id: Schema.number,
      greeting: Schema.string,
    })
  );
  const test2Schema = Schema.nonEmptyArray(
    Schema.struct({
      id: Schema.number,
      greeting: Schema.string,
      last_updated: Schema.DateFromSelf,
    })
  );

  const [test1, test2] = yield* _(
    pg.query(
      `create table test1 (id integer primary key, greeting varchar);
       insert into test1 values (1, 'hello'), (2, 'gday');
       select * from test1;
       drop table test1;
       create table test2 (id integer primary key, greeting varchar, last_updated timestamp);
       insert into test2 values (1, 'hello', now()), (2, 'gday', now());
       delete from test2 where id = 1;
       select * from test2;
       drop table test2;`,
      test1Schema,
      test2Schema
    )
  );

  return { test1, test2 };
});

Effect.runPromise(
  program.pipe(Effect.scoped, Effect.catchAllDefect(Effect.logFatal))
).then((results) => {
  console.log('returned:', results);
});
```

The output of this running against a test postgres docker container with self-signed cert is:

```
$ NODE_TLS_REJECT_UNAUTHORIZED=0 node dist/test.js 
(node:92744) Warning: Setting the NODE_TLS_REJECT_UNAUTHORIZED environment variable to '0' makes TLS connections and HTTPS requests insecure by disabling certificate verification.
(Use `node --trace-warnings ...` to show where the warning was created)
timestamp=2023-09-27T01:15:51.514Z level=INFO fiber=#0 message="CREATE TABLE"
timestamp=2023-09-27T01:15:51.516Z level=INFO fiber=#0 message="INSERT 0 2"
timestamp=2023-09-27T01:15:51.516Z level=INFO fiber=#0 message="SELECT 2"
timestamp=2023-09-27T01:15:51.516Z level=INFO fiber=#0 message="DROP TABLE"
timestamp=2023-09-27T01:15:51.516Z level=INFO fiber=#0 message="CREATE TABLE"
timestamp=2023-09-27T01:15:51.516Z level=INFO fiber=#0 message="INSERT 0 2"
timestamp=2023-09-27T01:15:51.516Z level=INFO fiber=#0 message="DELETE 1"
timestamp=2023-09-27T01:15:51.517Z level=INFO fiber=#0 message="SELECT 1"
timestamp=2023-09-27T01:15:51.517Z level=INFO fiber=#0 message="DROP TABLE"
returned: {
  test1: [ { id: 1, greeting: 'hello' }, { id: 2, greeting: 'gday' } ],
  test2: [
    { id: 2, greeting: 'gday', last_updated: 2023-09-26T15:15:51.474Z }
  ]
}
```

#### queryStream 

The `queryStream` method is like query, but returns a stream of a union of the schema types. 

```typescript
  <S extends [...Schema.Schema<any, any>[]]>
  queryStream(
    sqlOrOptions:
      | string
      | { sql: string; parserOptions: MakeValueTypeParserOptions },
    ...schemas: S
  ): Stream.Stream<
    never,
    | WritableError
    | ReadableError
    | ParseMessageError
    | NoMoreMessagesError
    | PgServerError
    | PgParseError
    | ParseMessageGroupError,
    SchemaTypesUnion<S>
  >
```
- `sqlOrOptions`: A string containing one or more SQL statements or an object containing SQL and parser options. The parser options determine how each row is parsed before being validated by a supplied schema. If you do not specify parser options the default behavior is to parse all supported types.
- `schemas`: Zero, one or more schemas that are used to validate any results. Each schema must define the type of a record in a result set.

##### Example

```typescript
import { makePgClient } from '@jmorecroft67/pg-stream-core';
import { Effect, Stream } from 'effect';
import * as Schema from '@effect/schema/Schema';

const program = Effect.gen(function* (_) {
  const pg = yield* _(
    makePgClient({
      username: 'postgres',
      password: 'topsecret',
      database: 'postgres',
      host: 'localhost',
      port: 5432,
      useSSL: true,
    })
  );

  const dogSchema = Schema.struct({
    id: Schema.number,
    name: Schema.string,
    age: Schema.number,
    bark: Schema.literal('woof', 'bowow', 'ruff'),
    last_updated: Schema.DateFromSelf,
  });

  const catSchema = Schema.struct({
    id: Schema.number,
    name: Schema.string,
    age: Schema.number,
    lives: Schema.between(1, 9)(Schema.number),
    last_updated: Schema.DateFromSelf,
  });

  const stream = pg.queryStream(
    `
    create type bark_type AS ENUM ('woof', 'bowow', 'ruff');
    create table dog (id serial, name text, age int, bark bark_type, last_updated timestamp);
    insert into dog values (1, 'bingo', 2, 'woof', now()), (2, 'spot', 7, 'ruff', now());
    select * from dog;
    drop table dog;
    drop type bark_type;
    create table cat (id serial, name text, age int, lives int, last_updated timestamp);
    insert into cat values (1, 'garfield', 2, 9, now()), (2, 'simba', 7, 2, now());
    select * from cat;
    drop table cat;`,
    Schema.attachPropertySignature('kind', 'dog')(dogSchema),
    Schema.attachPropertySignature('kind', 'cat')(catSchema)
  );

  return yield* _(
    stream.pipe(
      Stream.tap((animal) => {
        if (animal.kind === 'cat') {
          return Effect.log(`${animal.name} has ${animal.lives} lives!`);
        }
        return Effect.log(
          `${animal.name}'s bark sounds like "${animal.bark}"!`
        );
      }),
      Stream.runCollect
    )
  );
});

Effect.runPromise(
  program.pipe(Effect.scoped, Effect.catchAllDefect(Effect.logFatal))
).then((results) => {
  console.log('returned:', results);
});
```

The output of this running against a test postgres docker container with self-signed cert is:

```
$ node dist/packages/cli/main.js 
(node:20781) Warning: Setting the NODE_TLS_REJECT_UNAUTHORIZED environment variable to '0' makes TLS connections and HTTPS requests insecure by disabling certificate verification.
(Use `node --trace-warnings ...` to show where the warning was created)
timestamp=2023-10-09T13:12:48.353Z level=INFO fiber=#34 message="CREATE TYPE"
timestamp=2023-10-09T13:12:48.356Z level=INFO fiber=#34 message="CREATE TABLE"
timestamp=2023-10-09T13:12:48.356Z level=INFO fiber=#34 message="INSERT 0 2"
timestamp=2023-10-09T13:12:48.359Z level=INFO fiber=#34 message="bingo's bark sounds like \"woof\"!"
timestamp=2023-10-09T13:12:48.360Z level=INFO fiber=#34 message="spot's bark sounds like \"ruff\"!"
timestamp=2023-10-09T13:12:48.360Z level=INFO fiber=#34 message="SELECT 2"
timestamp=2023-10-09T13:12:48.360Z level=INFO fiber=#34 message="DROP TABLE"
timestamp=2023-10-09T13:12:48.361Z level=INFO fiber=#34 message="DROP TYPE"
timestamp=2023-10-09T13:12:48.361Z level=INFO fiber=#34 message="CREATE TABLE"
timestamp=2023-10-09T13:12:48.362Z level=INFO fiber=#34 message="INSERT 0 2"
timestamp=2023-10-09T13:12:48.363Z level=INFO fiber=#34 message="garfield has 9 lives!"
timestamp=2023-10-09T13:12:48.364Z level=INFO fiber=#34 message="simba has 2 lives!"
timestamp=2023-10-09T13:12:48.364Z level=INFO fiber=#34 message="SELECT 2"
timestamp=2023-10-09T13:12:48.365Z level=INFO fiber=#34 message="DROP TABLE"
returned: {
  _id: 'Chunk',
  values: [
    {
      kind: 'dog',
      id: 1,
      name: 'bingo',
      age: 2,
      bark: 'woof',
      last_updated: 2023-10-09T03:12:48.318Z
    },
    {
      kind: 'dog',
      id: 2,
      name: 'spot',
      age: 7,
      bark: 'ruff',
      last_updated: 2023-10-09T03:12:48.318Z
    },
    {
      kind: 'cat',
      id: 1,
      name: 'garfield',
      age: 2,
      lives: 9,
      last_updated: 2023-10-09T03:12:48.318Z
    },
    {
      kind: 'cat',
      id: 2,
      name: 'simba',
      age: 7,
      lives: 2,
      last_updated: 2023-10-09T03:12:48.318Z
    }
  ]
}
```

#### recvlogical 

The `recvlogical` method starts a logical replication stream from the server, which is fed to a supplied processor.
```typescript
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
    signal?: Deferred.Deferred<never, void>;
  }): Effect.Effect<
    never,
    | WritableError
    | ReadableError
    | ParseMessageError
    | NoMoreMessagesError
    | UnexpectedMessageError
    | PgServerError
    | PgParseError
    | ParseMessageGroupError
    | TableInfoNotFoundError
    | NoTransactionContextError
    | E,
    void
  > 

```
- `options`: 
  - `slotName`: The name of the replication slot.
  - `publicationNames`: The names of the publications being subscribed to.
  - `processor`: A processor for the handling of streamed records, along with optional partition key generation (to constrain concurrency) and optional record filtering. If no partition key generation is defined, we default to generating a unique key per database table.
  - `signal`: A optional signal for gracefully stopping the replication stream, which then allows continued use of the connection for further queries or streaming.

#### Example

```typescript
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

  yield* _(pg2.query('create publication test_pub for all tables'));
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
    pg1.query(`create table test (id integer primary key, greeting varchar);
               alter table test replica identity full;
               insert into test values (1, 'hello'), (2, 'gday');
               update test set greeting = 'hi' where id = 1;
               delete from test where id = 1;
               drop table test;
               drop publication if exists test_pub;`)
  );

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
```

The output of this running against a test postgres docker container with self-signed cert is:

```
$ NODE_TLS_REJECT_UNAUTHORIZED=0 node dist/test2.js 
(node:91318) Warning: Setting the NODE_TLS_REJECT_UNAUTHORIZED environment variable to '0' makes TLS connections and HTTPS requests insecure by disabling certificate verification.
(Use `node --trace-warnings ...` to show where the warning was created)
timestamp=2023-09-27T00:37:39.065Z level=INFO fiber=#0 message="CREATE PUBLICATION"
timestamp=2023-09-27T00:37:39.088Z level=INFO fiber=#0 message=CREATE_REPLICATION_SLOT
timestamp=2023-09-27T00:37:39.106Z level=INFO fiber=#86 message="SELECT 1"
timestamp=2023-09-27T00:37:39.126Z level=INFO fiber=#0 message="CREATE TABLE"
timestamp=2023-09-27T00:37:39.126Z level=INFO fiber=#0 message="ALTER TABLE"
timestamp=2023-09-27T00:37:39.126Z level=INFO fiber=#0 message="INSERT 0 2"
timestamp=2023-09-27T00:37:39.126Z level=INFO fiber=#0 message="UPDATE 1"
timestamp=2023-09-27T00:37:39.126Z level=INFO fiber=#0 message="DELETE 1"
timestamp=2023-09-27T00:37:39.126Z level=INFO fiber=#0 message="DROP TABLE"
timestamp=2023-09-27T00:37:39.126Z level=INFO fiber=#0 message="DROP PUBLICATION"
returned: [
  {
    "inserted": {
      "id": 1,
      "greeting": "hello"
    }
  },
  {
    "inserted": {
      "id": 2,
      "greeting": "gday"
    }
  },
  {
    "deleted": {
      "id": 1,
      "greeting": "hello"
    },
    "inserted": {
      "id": 1,
      "greeting": "hi"
    }
  },
  {
    "deleted": {
      "id": 1,
      "greeting": "hi"
    }
  }
]
```

### earlier versions

This version is a rewrite of the existing 1.X lib, which did not use Effect TS but relied more on fp-ts and more direct use of NodeJS streams. This new version is simpler and more flexible, and should integrate easily with Effect TS based applications.

### alternatives

This lib is very much a work in progress, has NOT been widely tested in the field and any use in a production environment should be carefully considered! This lib was written from a desire to have something simple, flexible and easily deployable to reliably push xlogs from Postgres, with backpressure support. It was also a great excuse to re-invent a few wheels and learn more about FP!

Thankfully there are a number of great, mature alternatives that may do what you're after.

- [pg](https://www.npmjs.com/package/pg) - the default choice of client for connecting to Postgres from JavaScript. For logical replication scenarios there is the [pg-copy-streams](https://www.npmjs.com/package/pg-copy-streams) lib built on top of this, which I initially investigated using before naively deciding to do it all myself!
- [psql](https://www.postgresql.org/docs/current/app-psql.html) - the standard Postgres interactive client. Logical replication using the SQL interface is demonstrated [here](https://www.postgresql.org/docs/current/logicaldecoding-example.html).
- [pg_recvlogical](https://www.postgresql.org/docs/current/app-pgrecvlogical.html) - a utility for logical replication streaming shipped with Postgres, and inspiration for the `recvlogical` function name! An example of its use with the wal2json plugin is [here](https://access.crunchydata.com/documentation/wal2json/2.0/). (Note to keep things simple the pg-stream-core lib only uses the built-in pgoutput plugin)
- [Debezium](https://debezium.io/) - the batteries-included solution for change data capture (CDC) from Postgres, or a bunch of other databases. This one is probably the one to choose if you're trying to implement CDC and require a battle-tested solution.
