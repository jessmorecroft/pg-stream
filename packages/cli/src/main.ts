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
      Stream.tap(([animal]) => {
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
