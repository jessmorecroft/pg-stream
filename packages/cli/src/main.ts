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
