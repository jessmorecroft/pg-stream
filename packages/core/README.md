## pg-stream-core

A limited-functionality PostgreSQL client library for simple queries and most significantly, reliable logical replication streaming using the pgoutput protocol.

This lib is built on top of the awesome [Effect TS](https://www.effect.website) libraries. It talks directly to Postgres via the wire protocol. (ie. it does not utilise any existing pg libraries)

### structure

The pg-stream lib exposes a PgClient constructor and a PgClientPool constructor. Both of these are scoped to ensure any connections are closed when you are done.

The PgClient object provides three methods:

- `command` - executes an SQL statement on the server where results are not expected
- `query` - executes an SQL statement on the server where results are expected
- `recvlogical` - starts a logical replication stream from the server, which is fed to a supplied processor.

### `command` and `query`

```typescript
import { makePgClient } from '@jmorecroft67/pg-stream-core';
import { Effect } from 'effect';
import * as Schema from '@effect/schema/Schema';

const program = Effect.gen(function* (_) {
  const pg = yield* _(
    makePgClient({
      username: 'postgres',
      password: 'password',
      database: 'postgres',
      host: 'localhost',
      port: 5432,
      useSSL: true,
    })
  );

  yield* _(pg.command('create table test (id integer primary key, greeting varchar)'));
  yield* _(pg.command('alter table test replica identity full'));
  yield* _(pg.command(`insert into test values (1, 'hello'), (2, 'gday')`));
  const rows = yield* _(
    pg.query(
      'select * from test',
      Schema.nonEmptyArray(
        Schema.struct({
          id: Schema.number,
          greeting: Schema.string,
        })
      )
    )
  );
  yield* _(pg.command('delete from test where id = 1'));
  yield* _(pg.command('drop table test'));

  return rows;
});

Effect.runPromise(program.pipe(Effect.scoped, Effect.catchAllDefect(Effect.logFatal))).then((rows) => {
  console.log('returned:', rows);
});
```

The output of this running against a test postgres docker container with self-signed cert is:

```
$ NODE_TLS_REJECT_UNAUTHORIZED=0 node dist/test.js
(node:84102) Warning: Setting the NODE_TLS_REJECT_UNAUTHORIZED environment variable to '0' makes TLS connections and HTTPS requests insecure by disabling certificate verification.
(Use `node --trace-warnings ...` to show where the warning was created)
timestamp=2023-09-26T09:20:37.536Z level=INFO fiber=#0 message="CREATE TABLE"
timestamp=2023-09-26T09:20:37.541Z level=INFO fiber=#0 message="ALTER TABLE"
timestamp=2023-09-26T09:20:37.545Z level=INFO fiber=#0 message="INSERT 0 2"
timestamp=2023-09-26T09:20:37.549Z level=INFO fiber=#0 message="SELECT 2"
timestamp=2023-09-26T09:20:37.553Z level=INFO fiber=#0 message="DELETE 1"
timestamp=2023-09-26T09:20:37.558Z level=INFO fiber=#0 message="DROP TABLE"
returned: [ { id: 1, greeting: 'hello' }, { id: 2, greeting: 'gday' } ]
```

### recvlogical

```typescript
import { makePgPool, DecoratedInsert, DecoratedDelete, DecoratedUpdate } from '@jmorecroft67/pg-stream-core';
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

  const queue = yield* _(Queue.unbounded<DecoratedInsert | DecoratedDelete | DecoratedUpdate>());

  yield* _(pg2.command('create publication test_pub for all tables'));
  yield* _(pg2.query('CREATE_REPLICATION_SLOT test_slot TEMPORARY LOGICAL pgoutput NOEXPORT_SNAPSHOT', Schema.any));

  const fibre = Effect.runFork(
    Effect.timeout(
      pg2.recvlogical({
        slotName: 'test_slot',
        publicationNames: ['test_pub'],
        processor: {
          filter: (msg): msg is DecoratedInsert | DecoratedDelete | DecoratedUpdate => msg.type === 'Insert' || msg.type === 'Delete' || msg.type === 'Update',
          process: (data) => queue.offerAll(data),
        },
      }),
      '2 seconds'
    )
  );

  yield* _(pg1.command('create table test (id integer primary key, greeting varchar)'));
  yield* _(pg1.command('alter table test replica identity full'));
  yield* _(pg1.command(`insert into test values (1, 'hello'), (2, 'gday')`));
  yield* _(pg1.command(`update test set greeting = 'hi' where id = 1`));
  yield* _(pg1.command('delete from test where id = 1'));
  yield* _(pg1.command('drop table test'));
  yield* _(pg1.command('drop publication if exists test_pub'));

  yield* _(fibre.await());

  const chunk = yield* _(queue.takeAll());

  return Chunk.toReadonlyArray(chunk).map((log) => (log.type === 'Insert' ? { inserted: log.newRecord } : log.type === 'Update' ? { deleted: log.oldRecord, inserted: log.newRecord } : { deleted: log.oldRecord }));
});

Effect.runPromise(program.pipe(Effect.scoped, Effect.catchAllDefect(Effect.logFatal))).then((streamed) => {
  console.log('returned:', JSON.stringify(streamed, null, 2));
});
```

The output of this running against a test postgres docker container with self-signed cert is:

```
$ NODE_TLS_REJECT_UNAUTHORIZED=0 node dist/test2.js
(node:85202) Warning: Setting the NODE_TLS_REJECT_UNAUTHORIZED environment variable to '0' makes TLS connections and HTTPS requests insecure by disabling certificate verification.
(Use `node --trace-warnings ...` to show where the warning was created)
timestamp=2023-09-26T10:06:01.508Z level=INFO fiber=#0 message="CREATE PUBLICATION"
timestamp=2023-09-26T10:06:01.522Z level=INFO fiber=#0 message=CREATE_REPLICATION_SLOT
timestamp=2023-09-26T10:06:01.533Z level=INFO fiber=#86 message="SELECT 1"
timestamp=2023-09-26T10:06:01.536Z level=INFO fiber=#0 message="CREATE TABLE"
timestamp=2023-09-26T10:06:01.553Z level=INFO fiber=#0 message="ALTER TABLE"
timestamp=2023-09-26T10:06:01.557Z level=INFO fiber=#0 message="INSERT 0 2"
timestamp=2023-09-26T10:06:01.561Z level=INFO fiber=#0 message="UPDATE 1"
timestamp=2023-09-26T10:06:01.583Z level=INFO fiber=#0 message="DELETE 1"
timestamp=2023-09-26T10:06:01.596Z level=INFO fiber=#0 message="DROP TABLE"
timestamp=2023-09-26T10:06:01.602Z level=INFO fiber=#0 message="DROP PUBLICATION"
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
  i
