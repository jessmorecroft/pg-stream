import { Command, Options } from '@effect/cli';
import { Console, Effect, LogLevel, Logger, Option, identity } from 'effect';
import { runMain } from '@effect/platform-node/NodeRuntime';
import * as NodeContext from '@effect/platform-node/NodeContext';
import {
  ALL_ENABLED_PARSER_OPTIONS,
  makePgClient,
} from '@jmorecroft67/pg-stream-core';

const hostOption = Options.text('host').pipe(
  Options.withDescription('postgres server host'),
  Options.withAlias('h'),
  Options.withDefault('localhost'),
);

const portOption = Options.integer('port').pipe(
  Options.withDescription('postgres server port'),
  Options.withAlias('p'),
  Options.withDefault(5432),
);

const databaseOption = Options.text('dbname').pipe(
  Options.withDescription('postgres database'),
  Options.withAlias('d'),
  Options.withDefault('postgres'),
);

const usernameOption = Options.text('username').pipe(
  Options.withDescription('database user username'),
  Options.withAlias('U'),
  Options.withDefault('postgres'),
);

const passwordOption = Options.text('password').pipe(
  Options.withDescription('databse user password'),
  Options.withAlias('W'),
);

const noSslOption = Options.boolean('noSSL').pipe(
  Options.withDescription('do not connect to postgres using SSL'),
  Options.withDefault(false),
);

const loggingOption = Options.choice('log', ['verbose', 'quiet']).pipe(
  Options.withDescription('logging level'),
  Options.optional,
);

const commonOptions = {
  host: hostOption,
  port: portOption,
  database: databaseOption,
  username: usernameOption,
  password: passwordOption,
  noSsl: noSslOption,
  log: loggingOption,
};

const sqlOption = Options.text('command').pipe(
  Options.withDescription('run some SQL and print the results'),
  Options.withAlias('c'),
);

const queryCommand = Command.make(
  'query',
  {
    ...commonOptions,
    sql: sqlOption,
  },
  ({ sql, noSsl, log, ...options }) =>
    Effect.gen(function* (_) {
      const pg = yield* _(
        makePgClient({ ...options, useSSL: !noSsl }).pipe(
          Effect.timeoutFail({
            onTimeout: () => new Error('connect timeout'),
            duration: '5 seconds',
          }),
        ),
      );

      const results = yield* _(pg.queryRaw(sql, ALL_ENABLED_PARSER_OPTIONS));

      for (const result of results) {
        if (result.length > 0) {
          yield* _(Console.table(result, Object.keys(result[0])));
        }
      }
    }).pipe(
      Effect.scoped,
      Option.isSome(log)
        ? log.value === 'quiet'
          ? Logger.withMinimumLogLevel(LogLevel.Error)
          : Logger.withMinimumLogLevel(LogLevel.Debug)
        : identity,
    ),
);

const run = Command.make('pg-stream').pipe(
  Command.withDescription('Query and subscribe to a postgres server'),
  Command.withSubcommands([queryCommand]),
  Command.run({
    name: 'pg-stream',
    version: '2.0.0',
  }),
);

export const cli = (argv: string[]) => {
  const program = Effect.suspend(() => run(argv)).pipe(
    Effect.provide(NodeContext.layer),
  );
  runMain(program);
};
