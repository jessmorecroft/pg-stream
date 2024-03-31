import { Schema } from "@effect/schema";
import { makePgClient, ALL_ENABLED_PARSER_OPTIONS } from "../index";
import { Effect, Stream } from "effect";
import { inspect } from "util";

const program = Effect.gen(function* (_) {
  const pg = yield* _(
    makePgClient({
      username: "postgres",
      password: "topsecret",
      database: "postgres",
      host: "localhost",
      port: 5432,
      useSSL: true,
    }),
  );

  // Let's query something simple, no parsing.
  const dogs = yield* _(
    pg
      .queryStreamRaw(
        `
    CREATE TEMPORARY TABLE dog (
      id SERIAL,
      name TEXT,
      bark TEXT,
      created TIMESTAMP DEFAULT now()
    );
    INSERT INTO dog (name, bark) VALUES ('ralph', 'woof'), ('spot', 'arf');
    SELECT * FROM dog;`,
        {}, // no parsing
      )
      .pipe(Stream.runCollect),
  );

  const logResults = <A>(results: Iterable<readonly [A, number]>) =>
    Effect.forEach(results, ([row, index]) =>
      Effect.log(`row ${index}: ${inspect(row)}`),
    );

  yield* _(logResults(dogs));

  // Again, but let's parse everything this time. Note our dates lose their microsecond precision.
  const dogSchema = Schema.struct({
    id: Schema.number,
    name: Schema.string,
    bark: Schema.literal("woof", "arf"),
    created: Schema.DateFromSelf,
  });

  const dogs2 = yield* _(
    pg.queryStream("SELECT * FROM dog", dogSchema).pipe(Stream.runCollect),
  );

  yield* _(logResults(dogs2));

  // And again, but let's not parse dates.
  const dogSchema2 = Schema.struct({
    id: Schema.number,
    name: Schema.string,
    bark: Schema.literal("woof", "arf"),
    created: Schema.string,
  });

  const dogs3 = yield* _(
    pg
      .queryStream(
        {
          sql: "SELECT * FROM dog",
          parserOptions: {
            ...ALL_ENABLED_PARSER_OPTIONS,
            parseDates: false,
          },
        },
        dogSchema2,
      )
      .pipe(Stream.runCollect),
  );

  yield* _(logResults(dogs3));

  // Now let's try a couple of queries in the same stream.
  const catSchema = Schema.struct({
    id: Schema.number,
    name: Schema.string,
    lives: Schema.int()(Schema.number),
    created: Schema.DateFromSelf,
  });

  const dogsAndCats = yield* _(
    pg
      .queryStream(
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
        dogSchema.pipe(Schema.attachPropertySignature("kind", "dog")),
        catSchema.pipe(Schema.attachPropertySignature("kind", "cat")),
      )
      .pipe(
        Stream.tap(([animal]) => {
          if (animal.kind === "cat") {
            return Effect.log(`${animal.name} has ${animal.lives} lives!`);
          }
          return Effect.log(
            `${animal.name}'s bark sounds like "${animal.bark}"!`,
          );
        }),
        Stream.runCollect,
      ),
  );

  yield* _(logResults(dogsAndCats));
});

Effect.runPromise(
  program.pipe(Effect.scoped, Effect.catchAllDefect(Effect.logFatal)),
);
