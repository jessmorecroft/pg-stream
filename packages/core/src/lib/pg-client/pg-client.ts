/* eslint-disable @typescript-eslint/no-explicit-any */
import {
  Chunk,
  Data,
  Deferred,
  Effect,
  Either,
  GroupBy,
  Option,
  Sink,
  Stream,
  pipe,
} from 'effect';
import * as E from 'fp-ts/Either';
import * as O from 'fp-ts/Option';
import * as P from 'parser-ts/Parser';
import {
  CopyData,
  CopyDone,
  DataRow,
  ErrorResponse,
  NoticeResponse,
  PgClientMessageTypes,
  RowDescription,
  XKeepAlive,
  XLogData,
  pgSSLRequestResponse,
  pgServerMessageParser,
} from '../pg-protocol/message-parsers';
import {
  MakeValueTypeParserOptions,
  ValueType,
  makePgClientMessage,
  makeValueTypeParser,
} from '../pg-protocol';
import { PgServerMessageTypes } from '../pg-protocol/message-parsers';
import { logBackendMessage } from './util';
import * as S from 'parser-ts/string';
import * as Schema from '@effect/schema/Schema';
import { formatErrors } from '@effect/schema/TreeFormatter';
import { walLsnFromString } from '../util/schemas';
import { startup } from './startup';
import {
  DecoratedBegin,
  NoTransactionContextError,
  PgOutputDecoratedMessageTypes,
  TableInfoMap,
  TableInfoNotFoundError,
  transformLogData,
} from './transform-log-data';
import {
  NoMoreMessagesError,
  ParseMessageError,
  ParseMessageGroupError,
  ReadableError,
  UnexpectedMessageError,
  WritableError,
  hasTypeOf,
} from '../stream';
import * as stream from '../stream';
import { Duplex, Readable, Writable } from 'stream';
import { connect, end, clientTlsConnect } from '../socket';

export interface Options {
  host: string;
  port: number;
  database: string;
  username: string;
  password: string;
  useSSL?: boolean;
  replication?: boolean;
}

export interface XLogProcessor<E, T extends PgOutputDecoratedMessageTypes> {
  filter(msg: PgOutputDecoratedMessageTypes): msg is T;
  key?(msg: T): string;
  process(key: string, chunk: Chunk.Chunk<T>): Effect.Effect<never, E, void>;
}

export type PgClient = Effect.Effect.Success<ReturnType<typeof make>>;

export class PgParseError extends Data.TaggedClass('PgParseError')<{
  message: string;
}> {}

export class PgServerError extends Data.TaggedClass('PgServerError')<{
  error: ErrorResponse;
}> {}

export class PgFailedAuth extends Data.TaggedClass('PgFailedAuth')<{
  reply: unknown;
  msg?: string;
}> {}

type MessageTypes = Exclude<
  PgServerMessageTypes,
  NoticeResponse | ErrorResponse
>;

export const item = P.item<MessageTypes>();

export const isReadyForQuery = hasTypeOf('ReadyForQuery');
export const isNoticeResponse = hasTypeOf('NoticeResponse');
export const isErrorResponse = hasTypeOf('ErrorResponse');
export const isCommandComplete = hasTypeOf('CommandComplete');
export const isDataRow = hasTypeOf('DataRow');
export const isRowDescription = hasTypeOf('RowDescription');
export const isParameterStatus = hasTypeOf('ParameterStatus');
export const isBackendKeyData = hasTypeOf('BackendKeyData');

export const read: (
  readable: Readable
) => Effect.Effect<
  never,
  Effect.Effect.Error<ReturnType<typeof stream.read>> | PgServerError,
  MessageTypes
> = (readable) =>
  Effect.filterOrElse(
    stream.read(readable, stream.decode(pgServerMessageParser)),
    (msg): msg is MessageTypes =>
      !isNoticeResponse(msg) && !isErrorResponse(msg),
    (msg) => {
      if (isNoticeResponse(msg)) {
        return Effect.flatMap(logBackendMessage(msg), () => read(readable));
      }
      if (isErrorResponse(msg)) {
        return Effect.flatMap(logBackendMessage(msg), () =>
          Effect.fail(new PgServerError({ error: msg }))
        );
      }
      return Effect.never;
    }
  );

export const readOrFail =
  (readable: Readable) =>
  <K extends MessageTypes['type']>(
    ...types: [K, ...K[]]
  ): Effect.Effect<
    never,
    Effect.Effect.Error<ReturnType<typeof read>> | UnexpectedMessageError,
    MessageTypes & { type: K }
  > =>
    stream.readOrFail(read(readable))(...types);

export const readUntilReady: (
  readable: Readable
) => <A>(
  parser: P.Parser<MessageTypes, A>
) => Effect.Effect<
  never,
  Effect.Effect.Error<ReturnType<typeof read>> | ParseMessageGroupError,
  A
> = (readable) => (parser) =>
  stream.readMany(read(readable))(
    parser,
    ({ type }) => type === 'ReadyForQuery'
  );

export const write: (
  writable: Writable
) => (
  message: PgClientMessageTypes
) => Effect.Effect<never, WritableError, void> = (writable) =>
  stream.write(writable, makePgClientMessage);

type SchemaTypes<A extends [...Schema.Schema<any>[]]> = {
  [K in keyof A]: Schema.Schema.To<A[K]>;
};

type SchemaTypesUnion<A extends [...Schema.Schema<any>[]]> =
  SchemaTypes<A>[number];

type NoneOneOrMany<T extends [...any]> = T extends [infer A]
  ? A
  : T extends []
  ? void
  : T;

const transformRowDescription: <OT extends MakeValueTypeParserOptions>(
  rowDescription: RowDescription,
  options?: OT
) => { name: string; parser: P.Parser<string, ValueType<OT>> }[] = (
  { fields },
  options = {
    parseBooleans: true,
    parseFloats: true,
    parseInts: true,
    parseJson: true,
    parseArrays: true,
    parseNumerics: true,
    parseDates: true,
    parseBigInts: true,
  } as any
) =>
  fields.map(({ name, dataTypeId }) => ({
    name,
    parser: makeValueTypeParser(dataTypeId, options),
  }));

const transformDataRow = <OT extends MakeValueTypeParserOptions>({
  rowParsers,
  dataRow,
}: {
  rowParsers: { name: string; parser: P.Parser<string, ValueType<OT>> }[];
  dataRow: DataRow;
}) =>
  rowParsers.reduce(
    (acc: Record<string, ValueType<OT>>, { name, parser }, index) => {
      const input = dataRow.values[index];
      if (input !== null) {
        const parsed = pipe(
          S.run(input)(parser),
          E.fold(
            (): ValueType<OT> => {
              // Failed to parse row value. Just use the original input.
              return input;
            },
            ({ value }) => value
          )
        );
        return { ...acc, [name]: parsed };
      }
      return { ...acc, [name]: input };
    },
    {}
  );

export const queryStreamRaw =
  (socket: Duplex) =>
  <O extends MakeValueTypeParserOptions>(
    sql: string,
    parserOptions?: O
  ): Stream.Stream<
    never,
    | WritableError
    | ReadableError
    | ParseMessageError
    | NoMoreMessagesError
    | PgServerError
    | PgParseError,
    [Record<string, ValueType<O>>, number]
  > => {
    type State = {
      decode?: (row: DataRow) => Record<string, ValueType<O>>;
      offset: number;
    };
    return Stream.fromEffect(write(socket)({ type: 'Query', sql })).pipe(
      Stream.flatMap(() =>
        stream
          .readStream(read(socket), () => false)
          .pipe(
            Stream.takeUntil(hasTypeOf('ReadyForQuery')),
            Stream.tap((_) =>
              _.type === 'CommandComplete'
                ? Effect.log(_.commandTag)
                : Effect.unit
            ),
            Stream.filter(hasTypeOf('RowDescription', 'DataRow')),
            Stream.zipWithIndex,
            Stream.mapAccumEffect(
              { offset: 0 } as State,
              (
                state,
                [msg, index]
              ): Effect.Effect<
                never,
                PgParseError,
                readonly [
                  State,
                  readonly [Record<string, ValueType<O>>, number] | undefined
                ]
              > => {
                if (msg.type === 'DataRow') {
                  if (!state.decode) {
                    return Effect.fail(
                      new PgParseError({
                        message: 'data row before row description',
                      })
                    );
                  }

                  return Effect.succeed([
                    state,
                    [state.decode(msg), index - state.offset] as const,
                  ]);
                }

                const rowParsers = transformRowDescription(msg, parserOptions);

                const decode = (dataRow: DataRow) =>
                  transformDataRow({ rowParsers, dataRow });

                return Effect.succeed([
                  { decode, offset: index + 1 },
                  undefined,
                ]);
              }
            ),
            Stream.filter(
              (_): _ is [Record<string, ValueType<O>>, number] => !!_
            )
          )
      )
    );
  };

export const queryStream =
  (socket: Duplex) =>
  <S extends [...Schema.Schema<any, any>[]]>(
    sqlOrOptions:
      | string
      | { sql: string; parserOptions?: MakeValueTypeParserOptions },
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
    readonly [SchemaTypesUnion<S>, number]
  > => {
    const { sql, parserOptions } =
      typeof sqlOrOptions === 'string'
        ? { sql: sqlOrOptions, parserOptions: undefined }
        : sqlOrOptions;

    type State = {
      left: Schema.Schema<any, any>[];
      schema?: Schema.Schema<any, any>;
    };
    return queryStreamRaw(socket)(sql, parserOptions).pipe(
      Stream.mapAccumEffect(
        { left: schemas } as State,
        (
          s,
          [msg, index]
        ): Effect.Effect<
          never,
          PgParseError,
          readonly [State, readonly [any, number]]
        > => {
          const { schema, left } =
            index === 0 ? { schema: s.left[0], left: s.left.slice(1) } : s;

          if (!schema) {
            return Effect.fail(
              new PgParseError({
                message: 'unexpected result set',
              })
            );
          }

          return Effect.map(
            Schema.parse(schema)(msg).pipe(
              Effect.mapError(
                (pe) =>
                  new PgParseError({
                    message: formatErrors(pe.errors),
                  })
              ),
              Effect.tapError((pe) => Effect.logError(`\n${pe.message}`))
            ),
            (a) => [{ schema, left }, [a, index]]
          );
        }
      )
    );
  };

const queryRaw =
  (socket: Duplex) =>
  <O extends MakeValueTypeParserOptions>(
    sql: string,
    parserOptions?: O
  ): Effect.Effect<
    never,
    | WritableError
    | ReadableError
    | ParseMessageError
    | NoMoreMessagesError
    | PgServerError
    | PgParseError
    | ParseMessageGroupError,
    Record<string, ValueType<O>>[][]
  > =>
    Effect.gen(function* (_) {
      yield* _(write(socket)({ type: 'Query', sql }));

      const results = yield* _(
        readUntilReady(socket)(
          pipe(
            item,
            P.filter(isRowDescription),
            P.map((rowDescription) =>
              transformRowDescription(rowDescription, parserOptions)
            ),
            P.chain((rowParsers) =>
              pipe(
                item,
                P.filter(isDataRow),
                P.map((dataRow) => transformDataRow({ rowParsers, dataRow })),
                P.many
              )
            ),
            P.optional,
            P.bindTo('rows'),
            P.bind('commandTag', () =>
              pipe(
                item,
                P.filter(isCommandComplete),
                P.map(({ commandTag }) => commandTag)
              )
            ),
            P.many,
            P.chainFirst(() =>
              pipe(
                item,
                P.filter(isReadyForQuery),
                P.chain(() => P.eof())
              )
            )
          )
        )
      );

      yield* _(
        Effect.forEach(results, ({ commandTag }) => Effect.log(commandTag))
      );

      return results
        .map(({ rows }) => rows)
        .filter(O.isSome)
        .map((rows) => rows.value);
    });

/*export const copyFrom =
  (socket: Duplex) =>
  (tableName: string) =>
  <E>(records: Stream.Stream<never, E, Record<string, string | null>>) => {};
*/

export const query =
  (socket: Duplex) =>
  <S extends [...Schema.Schema<any, any>[]]>(
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
  > => {
    const { sql, parserOptions } =
      typeof sqlOrOptions === 'string'
        ? { sql: sqlOrOptions, parserOptions: undefined }
        : sqlOrOptions;

    return queryRaw(socket)(sql, parserOptions).pipe(
      Effect.flatMap((rows) =>
        Schema.parse(Schema.tuple(...schemas))(rows).pipe(
          Effect.mapError(
            (pe) =>
              new PgParseError({
                message: formatErrors(pe.errors),
              })
          ),
          Effect.tapError((pe) => Effect.logError(`\n${pe.message}`))
        )
      ),
      Effect.map((parsed) => {
        if (parsed.length === 0) {
          return undefined;
        }
        if (parsed.length === 1) {
          return parsed[0];
        }
        return parsed;
      })
    );
  };

export const queryMany =
  (socket: Duplex) =>
  <S extends Schema.Schema<any, any>>(
    sqlOrOptions:
      | string
      | { sql: string; parserOptions: MakeValueTypeParserOptions },
    schema: S
  ): Effect.Effect<
    never,
    | WritableError
    | ReadableError
    | ParseMessageError
    | NoMoreMessagesError
    | PgServerError
    | PgParseError
    | ParseMessageGroupError,
    readonly Schema.Schema.To<S>[]
  > => {
    const { sql, parserOptions } =
      typeof sqlOrOptions === 'string'
        ? { sql: sqlOrOptions, parserOptions: undefined }
        : sqlOrOptions;

    return queryRaw(socket)(sql, parserOptions).pipe(
      Effect.flatMap((rows) =>
        Schema.parse(Schema.array(schema))(rows).pipe(
          Effect.mapError(
            (pe) =>
              new PgParseError({
                message: formatErrors(pe.errors),
              })
          ),
          Effect.tapError((pe) => Effect.logError(`\n${pe.message}`))
        )
      )
    );
  };

export const recvlogical =
  (socket: Duplex) =>
  <E, T extends PgOutputDecoratedMessageTypes>({
    slotName,
    publicationNames,
    processor,
    parserOptions,
    signal,
  }: {
    slotName: string;
    publicationNames: string[];
    processor: XLogProcessor<E, T>;
    parserOptions?: MakeValueTypeParserOptions;
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
  > =>
    Effect.gen(function* (_) {
      yield* _(
        write(socket)({
          type: 'Query',
          sql: `START_REPLICATION SLOT ${slotName} LOGICAL ${Schema.encodeSync(
            walLsnFromString
          )(0n)} (proto_version '1', publication_names '${publicationNames.join(
            ','
          )}')`,
        })
      );

      // wait for this before streaming
      yield* _(readOrFail(socket)('CopyBothResponse'));

      const processedLsns: [bigint, boolean][] = [];

      const [logData, keepalives] = yield* _(
        stream
          .readStream(read(socket), (e) => e._tag === 'NoMoreMessagesError')
          .pipe(
            Stream.takeUntil((_) => _.type === 'CopyDone'),
            Stream.filter(hasTypeOf('CopyData')),
            Stream.tap((msg) => {
              if (
                msg.payload.type === 'XLogData' &&
                msg.payload.walEnd !== 0n
              ) {
                processedLsns.push([msg.payload.walEnd, false]);
              } else if (msg.payload.type === 'XKeepAlive') {
                processedLsns.push([msg.payload.walEnd, true]);
              }
              return Effect.unit;
            }),
            Stream.partitionEither(({ payload }) => {
              return hasTypeOf('XLogData')(payload)
                ? Effect.succeed(Either.left(payload))
                : Effect.succeed(Either.right(payload));
            })
          )
      );

      const [filtered, skipped] = yield* _(
        logData.pipe(
          Stream.mapAccumEffect(
            [new Map(), undefined],
            ([map, begin]: [TableInfoMap, DecoratedBegin | undefined], log) =>
              transformLogData(
                map,
                log,
                parserOptions ?? {
                  parseArrays: true,
                  parseBigInts: true,
                  parseBooleans: true,
                  parseDates: true,
                  parseFloats: true,
                  parseJson: true,
                  parseInts: true,
                  parseNumerics: true,
                },
                begin
              ).pipe(
                Effect.map(
                  (
                    msg
                  ): [
                    [TableInfoMap, DecoratedBegin | undefined],
                    [PgOutputDecoratedMessageTypes, XLogData]
                  ] => [
                    [
                      map,
                      msg.type === 'Begin'
                        ? msg
                        : msg.type !== 'Commit'
                        ? begin
                        : undefined,
                    ],
                    [msg, log],
                  ]
                )
              )
          ),
          Stream.partitionEither(
            (
              msgAndLog
            ): Effect.Effect<
              never,
              never,
              Either.Either<[T, XLogData], XLogData>
            > =>
              processor.filter(msgAndLog[0])
                ? Effect.succeed(Either.left([msgAndLog[0], msgAndLog[1]]))
                : Effect.succeed(Either.right(msgAndLog[1]))
          )
        )
      );

      const dataStream = filtered.pipe(
        Stream.bufferChunks({ capacity: 16 }),
        Stream.groupByKey(
          ([msg]) =>
            processor.key?.(msg) ??
            ('namespace' in msg ? `${msg.namespace}.${msg.name}` : '')
        ),
        GroupBy.evaluate((key, stream) =>
          stream.pipe(
            Stream.mapChunksEffect((chunk) =>
              Effect.map(
                processor.process(
                  key,
                  Chunk.map(chunk, ([msg]) => msg)
                ),
                () => Chunk.map(chunk, ([, log]) => log.walEnd)
              )
            )
          )
        ),
        Stream.merge(Stream.map(skipped, (log) => log.walEnd)),
        Stream.mapChunks((chunk) => {
          Chunk.forEach(chunk, (wal) => {
            const item = processedLsns.find((_) => _[0] === wal && !_[1]);
            if (item) {
              item[1] = true;
            }
          });
          return Chunk.of<void>(undefined);
        })
      );

      const ticks = keepalives.pipe(
        Stream.filter(
          (msg): msg is XKeepAlive => msg.type === 'XKeepAlive' && msg.replyNow
        ),
        Stream.merge(Stream.tick('10 seconds'))
      );

      const source = Stream.merge(dataStream, ticks, {
        haltStrategy: 'left',
      }).pipe(
        Stream.flatMap((keepalive) => {
          let next: bigint | undefined;
          while (processedLsns.length > 0) {
            if (processedLsns[0][1]) {
              next = processedLsns.shift()?.[0];
            } else {
              break;
            }
          }
          if (next) {
            return Stream.succeed(next);
          }
          if (keepalive) {
            return Stream.succeed(0n);
          }
          return Stream.empty;
        }),
        Stream.scan(0n, (s, a) => (a > s ? a : s)),
        Stream.map((lsn): CopyData => {
          const timeStamp =
            BigInt(new Date().getTime() - Date.UTC(2000, 0, 1)) * 1000n;

          return {
            type: 'CopyData',
            payload: {
              type: 'XStatusUpdate',
              lastWalWrite: lsn,
              lastWalFlush: lsn,
              lastWalApply: 0n,
              replyNow: false,
              timeStamp,
            },
          };
        })
      );

      const bufferPush = stream.toSinkable(stream.push(socket));

      const push = (input: Option.Option<Chunk.Chunk<CopyData | CopyDone>>) => {
        if (Option.isNone(input)) {
          return bufferPush(Option.none());
        }

        const chunk = input.value;

        const [head, tail] = Chunk.splitWhere(
          chunk,
          (_) => _.type === 'CopyDone'
        );

        if (Chunk.isNonEmpty(tail)) {
          return bufferPush(
            Option.some(
              Chunk.map(
                Chunk.append(head, Chunk.headNonEmpty(tail)),
                makePgClientMessage
              )
            )
          ).pipe(
            Effect.flatMap(() =>
              Effect.fail([
                Either.right<void>(undefined),
                Chunk.empty<never>(),
              ] as const)
            )
          );
        }

        return bufferPush(Option.some(Chunk.map(chunk, makePgClientMessage)));
      };

      const writeSink = Sink.fromPush(Effect.succeed(push));

      yield* _(
        Stream.run(
          Stream.merge(
            source,
            Stream.fromEffect(
              (signal ? Deferred.await(signal) : Effect.never).pipe(
                Effect.map((): CopyDone => ({ type: 'CopyDone' }))
              )
            )
          ),
          Sink.flatMap(writeSink, () => Sink.drain)
        )
      );

      const { commandTags } = yield* _(
        readUntilReady(socket)(
          pipe(
            item,
            P.filter(isCommandComplete),
            P.map(({ commandTag }) => commandTag),
            P.many,
            P.bindTo('commandTags'),
            P.chainFirst(() =>
              pipe(
                item,
                P.filter(isReadyForQuery),
                P.chain(() => P.eof())
              )
            )
          )
        )
      );

      yield* _(Effect.forEach(commandTags, (_) => Effect.log(_)));
    }).pipe(Effect.scoped);

export const make = ({ useSSL, ...options }: Options) =>
  Effect.gen(function* (_) {
    let socket = yield* _(connect(options));

    if (useSSL || useSSL === undefined) {
      yield* _(write(socket)({ type: 'SSLRequest', requestCode: 80877103 }));

      const reply = yield* _(
        stream.read(socket, stream.decode(pgSSLRequestResponse))
      );

      if (reply.useSSL) {
        socket = yield* _(clientTlsConnect(socket));
      } else {
        if (useSSL) {
          return yield* _(
            Effect.fail(
              new PgFailedAuth({
                msg: 'Postgres server does not support SSL',
                reply,
              })
            )
          );
        }

        yield* _(Effect.logWarning('Postgres server does not support SSL'));
      }
    }

    yield* _(Effect.addFinalizer(() => end(socket)));

    const info = yield* _(startup({ socket, ...options }));

    return {
      query: query(socket),
      queryRaw: queryRaw(socket),
      queryStream: queryStream(socket),
      queryStreamRaw: queryStreamRaw(socket),
      queryMany: queryMany(socket),
      recvlogical: recvlogical(socket),
      ...info,
    };
  });
