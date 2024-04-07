import { Command, Options } from '@effect/cli';
import {
  Config,
  ConfigProvider,
  Console,
  Effect,
  LogLevel,
  Logger,
  Option,
  identity,
  Types,
  Deferred,
  Cause,
  Chunk,
} from 'effect';
import * as NodeContext from '@effect/platform-node/NodeContext';
import {
  ALL_ENABLED_PARSER_OPTIONS,
  makePgClient,
  PgOutputDecoratedMessageTypes,
} from '@jmorecroft67/pg-stream-core';
import { isValidationError } from '@effect/cli/ValidationError';
import { defaultTeardown } from '@effect/platform/Runtime';
import packageJson from '../../package.json';

// eslint-disable-next-line @typescript-eslint/no-explicit-any
(BigInt.prototype as any)['toJSON'] = function () {
  return Number(this);
};

const hostOption = Options.text('host').pipe(
  Options.withDescription('postgres server host'),
  Options.withAlias('h'),
  Options.withFallbackConfig(Config.string('PG_HOST')),
  Options.withDefault('localhost'),
);

const portOption = Options.integer('port').pipe(
  Options.withDescription('postgres server port'),
  Options.withAlias('p'),
  Options.withFallbackConfig(Config.integer('PG_PORT')),
  Options.withDefault(5432),
);

const databaseOption = Options.text('dbname').pipe(
  Options.withDescription('postgres database'),
  Options.withAlias('d'),
  Options.withFallbackConfig(Config.string('PG_DATABASE')),
  Options.withDefault('postgres'),
);

const usernameOption = Options.text('username').pipe(
  Options.withDescription('database user username'),
  Options.withAlias('U'),
  Options.withFallbackConfig(Config.string('PG_USER')),
  Options.withDefault('postgres'),
);

const passwordOption = Options.text('password').pipe(
  Options.withDescription('databse user password'),
  Options.withAlias('W'),
  Options.withFallbackConfig(Config.string('PG_PASSWORD')),
);

const noSslOption = Options.boolean('noSSL').pipe(
  Options.withDescription('do not connect to postgres using SSL'),
  Options.withDefault(false),
);

const outputOption = Options.choice('output', ['table', 'json']).pipe(
  Options.withDescription('output display format'),
  Options.withDefault('table'),
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
  output: outputOption,
  log: loggingOption,
};

type CommonOptions = Types.Simplify<
  Command.Command.ParseConfig<typeof commonOptions>
>;

const pgStreamBinaryName = Object.keys(packageJson.bin)[0];

const pgStream = Command.make(pgStreamBinaryName, commonOptions);

const makePgClientWithTimeout = (
  options: CommonOptions & { replication?: boolean },
) =>
  makePgClient({ ...options, useSSL: !options.noSsl, replication: true }).pipe(
    Effect.timeoutFail({
      onTimeout: () => new Error('connect timeout'),
      duration: '5 seconds',
    }),
  );

const setLoggingAndConfig =
  ({ log }: CommonOptions) =>
  <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    effect.pipe(
      Option.isSome(log)
        ? log.value === 'quiet'
          ? Logger.withMinimumLogLevel(LogLevel.Error)
          : Logger.withMinimumLogLevel(LogLevel.Debug)
        : identity,
      Effect.withConfigProvider(ConfigProvider.fromEnv()),
    );

const JSON_FRIENDLY_PARSER_OPTIONS = {
  ...ALL_ENABLED_PARSER_OPTIONS,
  parseDates: false,
};

// query

const sqlOption = Options.text('command').pipe(
  Options.withDescription('SQL to run'),
  Options.withAlias('c'),
);

const queryCommand = Command.make(
  'query',
  {
    sql: sqlOption,
  },
  ({ sql }) =>
    Effect.flatMap(pgStream, (options) =>
      Effect.gen(function* (_) {
        const pg = yield* _(makePgClientWithTimeout(options));

        const results = yield* _(
          pg.queryRaw(
            sql,
            options.output === 'table'
              ? ALL_ENABLED_PARSER_OPTIONS
              : JSON_FRIENDLY_PARSER_OPTIONS,
          ),
        );

        for (const result of results) {
          if (result.length > 0) {
            if (options.output === 'table') {
              yield* _(Console.table(result, Object.keys(result[0])));
            } else {
              yield* _(Console.log(JSON.stringify(result)));
            }
          }
        }
      }).pipe(Effect.scoped, setLoggingAndConfig(options)),
    ),
).pipe(
  Command.withDescription(
    'Runs some SQL and prints the results. The SQL may be a single statement or a sequence of statements. ' +
      'When the SQL contains more than one SQL statement (separated by semicolons), those statements are ' +
      'executed as a single transaction, unless explicit transaction control commands are included to force ' +
      'a different behavior. Each result set will be printed separately, either in tabular form or as a ' +
      'single line of JSON, depending on the "output" selected.',
  ),
);

// recvlogical

const slotOption = Options.text('slot').pipe(
  Options.withDescription('slot name'),
  Options.withAlias('S'),
);

const publicationOption = Options.text('publication').pipe(
  Options.withDescription('publication name'),
  Options.withAlias('P'),
);

const createSlotIfNoneOption = Options.boolean('createSlotIfNone').pipe(
  Options.withDescription('create a slot if one does not exist'),
  Options.withDefault(false),
);

const createPublicationIffNoneOption = Options.boolean(
  'createPublicationIfNone',
).pipe(
  Options.withDescription('create a publication if one does not exist'),
  Options.withDefault(false),
);

const temporaryOption = Options.boolean('temporary').pipe(
  Options.withDescription(
    'if creating a slot or publication, make it temporary',
  ),
  Options.withDefault(false),
);

const recvlogicalCommand = Command.make(
  'recvlogical',
  {
    slot: slotOption,
    publication: publicationOption,
    createSlotIfNone: createSlotIfNoneOption,
    createPublicationIffNone: createPublicationIffNoneOption,
    temporary: temporaryOption,
  },
  ({
    slot,
    publication,
    createSlotIfNone,
    createPublicationIffNone,
    temporary,
  }) =>
    Effect.flatMap(pgStream, (options) =>
      Effect.gen(function* (_) {
        const pg = yield* _(
          makePgClientWithTimeout({ ...options, replication: true }),
        );

        if (createSlotIfNone) {
          const [slots] = yield* _(
            pg.queryRaw(
              `SELECT 1 FROM pg_replication_slots WHERE slot_name = '${slot}'`,
            ),
          );

          if (slots.length === 0) {
            yield* _(
              pg.queryRaw(
                `CREATE_REPLICATION_SLOT ${slot} ${temporary ? 'TEMPORARY ' : ''}LOGICAL pgoutput NOEXPORT_SNAPSHOT`,
              ),
            );
          }
        }

        if (createPublicationIffNone) {
          const [pubs] = yield* _(
            pg.queryRaw(
              `SELECT 1 FROM pg_publication WHERE pubname = '${publication}'`,
            ),
          );

          if (pubs.length === 0) {
            if (temporary) {
              yield* _(
                Effect.acquireRelease(
                  pg.queryRaw(
                    `CREATE PUBLICATION ${publication} FOR ALL TABLES`,
                  ),
                  () =>
                    pg.queryRaw(`DROP PUBLICATION ${publication}`).pipe(
                      Effect.tap(() => Effect.logDebug('dropped publication')),
                      Effect.ignore,
                    ),
                ),
              );
            } else {
              yield* _(
                pg.queryRaw(`CREATE PUBLICATION ${publication} FOR ALL TABLES`),
              );
            }
          }
        }

        const onSigInt = Effect.async<void>((emit) => {
          process.once('SIGINT', () => emit(Effect.unit));
        });
        const onSigTerm = Effect.async<void>((emit) => {
          process.once('SIGTERM', () => emit(Effect.unit));
        });

        const signal = yield* _(Deferred.make<void>());

        yield* _(
          Deferred.completeWith(signal, Effect.race(onSigInt, onSigTerm)),
          Effect.forkScoped,
        );

        yield* _(
          pg.recvlogical({
            slotName: slot,
            publicationNames: [publication],
            parserOptions:
              options.output === 'table'
                ? ALL_ENABLED_PARSER_OPTIONS
                : JSON_FRIENDLY_PARSER_OPTIONS,
            processor: {
              filter(msg): msg is PgOutputDecoratedMessageTypes {
                return true;
              },
              endOfChunk(msg) {
                return msg.type === 'Commit';
              },
              process(key, chunk) {
                const chunkArray = Chunk.toReadonlyArray(chunk);

                if (options.output === 'table') {
                  const keys = chunkArray.reduce((prev, record) => {
                    Object.keys(record).forEach((key) => prev.add(key));
                    return prev;
                  }, new Set<string>());
                  return Console.table(chunkArray, Array.from(keys.keys()));
                } else {
                  return Console.log(JSON.stringify(chunkArray));
                }
              },
            },
            signal,
          }),
        );
      }).pipe(Effect.scoped, setLoggingAndConfig(options)),
    ),
).pipe(
  Command.withDescription(
    'Starts a logical replication stream from the server and prints the events as they are received. Each ' +
      'batch of events associated with a single transaction is printed separately, either in tabular form or ' +
      'as a single line of JSON, depending on the "output" selected. The slot and publication may be optionally ' +
      'created on demand, and may optionally be designated "tempoary", in which case they will be removed on ' +
      'exit. The SIGINT (CTRL-C) or SIGTERM signals should be used to shut down the app.',
  ),
);

const run = pgStream.pipe(
  Command.withDescription(
    'Simple PostgreSQL client for querying and logical replication streaming.',
  ),
  Command.withSubcommands([queryCommand, recvlogicalCommand]),
  Command.run({
    name: pgStreamBinaryName,
    version: packageJson.version,
  }),
);

export const cli = (argv: string[]) =>
  Effect.suspend(() => run(argv)).pipe(
    Effect.tapErrorCause((e) =>
      Cause.isFailType(e) && isValidationError(e.error)
        ? Effect.unit
        : Effect.logError(e),
    ),
    Effect.provide(NodeContext.layer),
  );

export const runMain = (argv: string[]) => {
  const program = cli(argv);

  const fiber = Effect.runFork(program);
  fiber.addObserver((exit) => {
    defaultTeardown(exit, (code) => {
      process.exit(code);
    });
  });
};
