import { Schema } from '@effect/schema';
import { ALL_ENABLED_PARSER_OPTIONS, makePgClient } from '../index';
import { Effect } from 'effect';
import { inspect } from 'util';

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

  // Let's query something simple, no parsing.
  const dogs = yield* _(
    pg.queryRaw(
      `
    CREATE TEMPORARY TABLE dog (
      id SERIAL,
      name TEXT,
      bark TEXT,
      created TIMESTAMP DEFAULT now()
    );
    INSERT INTO dog (name, bark) VALUES ('ralph', 'woof'), ('spot', 'arf');
    SELECT * FROM dog;`,
      {} // no parsing
    )
  );

  const logResults = <A>(results: readonly (readonly A[])[]) =>
    Effect.forEach(results, (rows, index) =>
      Effect.log(`result set ${index}: ${inspect(rows)}`)
    );

  yield* _(logResults(dogs));

  // Again, but let's parse everything this time. Note our dates lose their microsecond precision.
  const dogSchema = Schema.struct({
    id: Schema.number,
    name: Schema.string,
    bark: Schema.literal('woof', 'arf'),
    created: Schema.DateFromSelf,
  });

  const dogs2 = yield* _(
    pg.query('SELECT * FROM dog', Schema.array(dogSchema))
  );

  yield* _(logResults([dogs2]));

  // And again, but let's not parse dates.
  const dogSchema2 = Schema.struct({
    id: Schema.number,
    name: Schema.string,
    bark: Schema.literal('woof', 'arf'),
    created: Schema.string,
  });

  const dogs3 = yield* _(
    pg.query(
      {
        sql: 'SELECT * FROM dog',
        parserOptions: {
          ...ALL_ENABLED_PARSER_OPTIONS,
          parseDates: false,
        },
      },
      Schema.array(dogSchema2)
    )
  );

  yield* _(logResults([dogs3]));

  // Now let's try a couple of queries at the same time.
  const catSchema = Schema.struct({
    id: Schema.number,
    name: Schema.string,
    lives: Schema.int()(Schema.number),
    created: Schema.DateFromSelf,
  });

  const dogWithKindSchema = Schema.attachPropertySignature(
    'kind',
    'dog'
  )(dogSchema);
  const catWithKindSchema = Schema.attachPropertySignature(
    'kind',
    'cat'
  )(catSchema);

  type DogWithKind = Schema.Schema.To<typeof dogWithKindSchema>;
  type CatWithKind = Schema.Schema.To<typeof catWithKindSchema>;
  const dogsAndCats = yield* _(
    pg.query(
      `
    CREATE TEMPORARY TABLE cat (
      id SERIAL,
      name TEXT,
      lives INTEGER,
      created TIMESTAMP DEFAULT now()
    );
    INSERT INTO cat (name, lives) VALUES ('garfield', 9), ('felix', 2);
    SELECT * FROM dog;
    SELECT * FROM cat`,
      Schema.nonEmptyArray(dogWithKindSchema),
      Schema.nonEmptyArray(catWithKindSchema)
    )
  );

  yield* _(logResults<DogWithKind | CatWithKind>(dogsAndCats));

  const dogsAndCats2 = yield* _(
    pg.queryMany(
      `
    SELECT * FROM dog;
    SELECT * FROM cat`,
      Schema.nonEmptyArray(Schema.union(dogWithKindSchema, catWithKindSchema))
    )
  );

  yield* _(logResults<DogWithKind | CatWithKind>(dogsAndCats2));
});

Effect.runPromise(
  program.pipe(Effect.scoped, Effect.catchAllDefect(Effect.logFatal))
);
