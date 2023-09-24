import { Data, Effect, Either, Schedule, Scope, Stream, pipe } from 'effect';
import * as E from 'fp-ts/Either';
import * as P from 'parser-ts/Parser';
import {
  CopyData,
  DataRow,
  ErrorResponse,
  NoticeResponse,
  PgClientMessageTypes,
  RowDescription,
  pgSSLRequestResponse,
  pgServerMessageParser,
} from '../pg-protocol/message-parsers';
import {
  MakeValueTypeParserOptions,
  makePgClientMessage,
  makePgCopyData,
  makeValueTypeParser,
} from '../pg-protocol';
import { PgServerMessageTypes } from '../pg-protocol/message-parsers';
import { logBackendMessage } from './util';
import * as S from 'parser-ts/string';
import * as Schema from '@effect/schema/Schema';
import { formatErrors } from '@effect/schema/TreeFormatter';
import { walLsnFromString } from '../util/wal-lsn-from-string';
import { startup } from './startup';
import {
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

export const query: (
  socket: Duplex
) => <F, A extends readonly unknown[]>(
  sql: string,
  schema: Schema.Schema<F, A>,
  options?: MakeValueTypeParserOptions
) => Effect.Effect<
  never,
  | WritableError
  | ReadableError
  | ParseMessageError
  | NoMoreMessagesError
  | PgServerError
  | PgParseError
  | ParseMessageGroupError,
  A
> = (socket) => (sql, schema, options) =>
  Effect.gen(function* (_) {
    yield* _(write(socket)({ type: 'Query', sql }));

    const transformRowDescription = ({ fields }: RowDescription) =>
      fields.map(({ name, dataTypeId }) => ({
        name,
        parser: makeValueTypeParser(
          dataTypeId,
          options ?? {
            parseBigInts: true,
            parseDates: true,
            parseNumerics: true,
          }
        ),
      }));

    const transformDataRow = ({
      rowParsers,
      dataRow,
    }: {
      rowParsers: ReturnType<typeof transformRowDescription>;
      dataRow: DataRow;
    }) =>
      rowParsers.reduce((acc, { name, parser }, index) => {
        const input = dataRow.values[index];
        if (input !== null) {
          const parsed = pipe(
            S.run(input)(parser),
            E.fold(
              () => {
                // Failed to parse row value. Just use the original input.
                return input;
              },
              ({ value }) => value
            )
          );
          return { ...acc, [name]: parsed };
        }
        return { ...acc, [name]: input };
      }, {} as object);

    const { rows, commandTag } = yield* _(
      readUntilReady(socket)(
        pipe(
          item,
          P.filter(isRowDescription),
          P.map(transformRowDescription),
          P.chain((rowParsers) =>
            pipe(
              item,
              P.filter(isDataRow),
              P.map((dataRow) => transformDataRow({ rowParsers, dataRow })),
              P.many
            )
          ),
          P.bindTo('rows'),
          P.bind('commandTag', () =>
            pipe(
              item,
              P.filter(isCommandComplete),
              P.map(({ commandTag }) => commandTag)
            )
          ),
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

    yield* _(Effect.log(commandTag));

    return yield* _(
      Schema.parse(schema)(rows).pipe(
        Effect.mapError(
          (pe) => new PgParseError({ message: formatErrors(pe.errors) })
        ),
        Effect.tapError((pe) => Effect.logError(`\n${pe.message}`))
      )
    );
  });

export const command: (
  socket: Duplex
) => (
  sql: string
) => Effect.Effect<
  never,
  | WritableError
  | ReadableError
  | ParseMessageError
  | NoMoreMessagesError
  | PgServerError
  | ParseMessageGroupError,
  string
> = (socket) => (sql) =>
  Effect.gen(function* (_) {
    yield* _(write(socket)({ type: 'Query', sql }));

    const { commandTag } = yield* _(
      readUntilReady(socket)(
        pipe(
          item,
          P.filter(isCommandComplete),
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

    yield* _(Effect.log(commandTag));

    return commandTag;
  });

export const recvlogical: (
  socket: Duplex
) => <R, E>(options: {
  slotName: string;
  publicationNames: string[];
  process: (data: PgOutputDecoratedMessageTypes) => Effect.Effect<R, E, void>;
  key?: (data: PgOutputDecoratedMessageTypes) => string;
}) => Effect.Effect<
  Exclude<R, Scope.Scope>,
  | WritableError
  | ReadableError
  | ParseMessageError
  | NoMoreMessagesError
  | UnexpectedMessageError
  | PgServerError
  | PgParseError
  | ParseMessageGroupError
  | TableInfoNotFoundError
  | E,
  void
> =
  (socket) =>
  ({ slotName, publicationNames, process, key }) =>
    Effect.gen(function* (_) {
      const [{ confirmed_flush_lsn: startLsn }] = yield* _(
        query(socket)(
          `SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '${slotName}'`,
          Schema.tuple(
            Schema.struct({
              confirmed_flush_lsn: walLsnFromString,
            })
          )
        )
      );

      yield* _(
        write(socket)({
          type: 'Query',
          sql: `START_REPLICATION SLOT ${slotName} LOGICAL ${Schema.encodeSync(
            walLsnFromString
          )(
            startLsn
          )} (proto_version '1', publication_names '${publicationNames.join(
            ','
          )}')`,
        })
      );

      // wait for this before streaming
      yield* _(readOrFail(socket)('CopyBothResponse'));

      const messages = stream.readStream(
        read(socket),
        (e) => e._tag === 'NoMoreMessagesError'
      );

      const [logData, keepalives] = yield* _(
        messages.pipe(
          Stream.filter(hasTypeOf('CopyData')),
          Stream.partitionEither(({ payload }) => {
            return hasTypeOf('XLogData')(payload)
              ? Effect.succeed(Either.left(payload))
              : Effect.succeed(Either.right(payload));
          })
        )
      );

      const wals: [bigint, boolean][] = [];

      const dataStream = logData.pipe(
        Stream.tap((log) => {
          wals.push([log.walStart, false]);
          return Effect.unit;
        }),
        Stream.mapAccumEffect(new Map(), (s: TableInfoMap, log) =>
          transformLogData(s, log).pipe(
            Effect.map((msg) => [s, [msg, log] as const])
          )
        ),
        Stream.mapEffect(
          ([msg, log]) => Effect.map(process(msg), () => log.walStart),
          {
            key: ([msg]) =>
              key?.(msg) ??
              ('namespace' in msg ? `${msg.namespace}.${msg.name}` : ''),
          }
        ),
        Stream.flatMap((wal) => {
          const item = wals.find((_) => _[0] === wal);
          if (item) {
            item[1] = true;
          }
          let next: bigint | undefined;
          while (wals.length > 0) {
            if (wals[0][1]) {
              next = wals.shift()?.[0];
            } else {
              break;
            }
          }
          if (next) {
            return Stream.succeed(next);
          }
          return Stream.empty;
        })
      );

      const keepaliveStream = Stream.merge(
        keepalives.pipe(
          Stream.filter((_) => _.replyNow && _.type === 'XKeepAlive'),
          Stream.map(() => startLsn)
        ),
        Stream.schedule(
          Stream.repeatValue(startLsn),
          Schedule.fixed('5 seconds')
        )
      );

      const source = Stream.merge(dataStream, keepaliveStream, {
        haltStrategy: 'left',
      }).pipe(
        Stream.scan(startLsn, (s, a) => (a > s ? a : s)),
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

      const sink = stream.writeSink(socket, makePgCopyData);

      yield* _(Stream.run(source, sink));
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
      command: command(socket),
      query: query(socket),
      recvlogical: recvlogical(socket),
      ...info,
    };
  });
