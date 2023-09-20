import {
  Chunk,
  Data,
  Effect,
  Either,
  Option,
  Schedule,
  Stream,
  pipe,
} from 'effect';
import * as E from 'fp-ts/Either';
import * as P from 'parser-ts/Parser';
import {
  DataRow,
  ErrorResponse,
  PgClientMessageTypes,
  RowDescription,
  pgSSLRequestResponse,
  pgServerMessageParser,
} from '../pg-protocol/message-parsers';
import {
  MakeValueTypeParserOptions,
  makePgClientMessage,
  makeValueTypeParser,
} from '../pg-protocol';
import { BaseSocket, make as makeSocket } from '../socket/socket';
import { hasTypeOf, make as makeMessageSocket } from '../socket/message-socket';
import { PgServerMessageTypes } from '../pg-protocol/message-parsers';
import { createHash, randomBytes } from 'crypto';
import { Hi, hmacSha256, sha256, xorBuffers } from './util';
import { logBackendMessage } from './util';
import * as S from 'parser-ts/string';
import * as Schema from '@effect/schema/Schema';
import { formatErrors } from '@effect/schema/TreeFormatter';
import { walLsnFromString } from '../util/wal-lsn-from-string';
import {
  PgOutputDecoratedMessageTypes,
  TableInfoMap,
  transformLogData,
} from './transform-log-data';

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

export type PgSocket = Effect.Effect.Success<ReturnType<typeof makePgSocket>>;

export class PgParseError extends Data.TaggedClass('PgParseError')<{
  message: string;
}> {}

export class PgFailedAuth extends Data.TaggedClass('PgFailedAuth')<{
  reply: unknown;
  msg?: string;
}> {}

export class PgServerError extends Data.TaggedClass('PgServerError')<{
  error: ErrorResponse;
}> {}

const item = P.item<PgServerMessageTypes>();

const isNoticeResponse = hasTypeOf('NoticeResponse');
const isErrorResponse = hasTypeOf('ErrorResponse');
const isCommandComplete = hasTypeOf('CommandComplete');
const isReadyForQuery = hasTypeOf('ReadyForQuery');
const isDataRow = hasTypeOf('DataRow');
const isRowDescription = hasTypeOf('RowDescription');
const isParameterStatus = hasTypeOf('ParameterStatus');
const isBackendKeyData = hasTypeOf('BackendKeyData');

const makePgSocket = ({ socket }: { socket: BaseSocket }) =>
  Effect.gen(function* (_) {
    const messageSocket = yield* _(
      makeMessageSocket({
        socket,
        parser: pgServerMessageParser,
        encoder: makePgClientMessage,
      })
    );

    const { write } = messageSocket;

    const read: Effect.Effect<
      never,
      Effect.Effect.Error<typeof messageSocket.read> | PgServerError,
      PgServerMessageTypes
    > = Effect.filterOrElse(
      messageSocket.read,
      (msg) => !isNoticeResponse(msg) && !isErrorResponse(msg),
      (msg) => {
        if (isNoticeResponse(msg)) {
          return Effect.flatMap(logBackendMessage(msg), () => read);
        }
        if (isErrorResponse(msg)) {
          return Effect.flatMap(logBackendMessage(msg), () =>
            Effect.fail(new PgServerError({ error: msg }))
          );
        }
        return read; // should never be here
      }
    );

    const readOrFail = <K extends PgServerMessageTypes['type']>(
      type: K,
      ...types: K[]
    ) =>
      messageSocket.readOrFail({
        reader: read,
        types: [type, ...types],
      });

    const readUntilReady = <A>(parser: P.Parser<PgServerMessageTypes, A>) =>
      messageSocket.readMany({
        reader: read,
        parser,
        isLast: ({ type }) => type === 'ReadyForQuery',
      });

    const recvlogical = <R, E>({
      slotName,
      publicationNames,
      process,
      key,
    }: {
      slotName: string;
      publicationNames: string[];
      process: (
        data: PgOutputDecoratedMessageTypes
      ) => Effect.Effect<R, E, void>;
      key?: (data: PgOutputDecoratedMessageTypes) => string;
    }) =>
      Effect.gen(function* (_) {
        const [{ confirmed_flush_lsn: startLsn }] = yield* _(
          query({
            sql: `SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '${slotName}'`,
            schema: Schema.tuple(
              Schema.struct({
                confirmed_flush_lsn: walLsnFromString,
              })
            ),
          })
        );

        yield* _(
          write({
            type: 'Query',
            sql: `START_REPLICATION SLOT ${slotName} LOGICAL ${yield* _(
              Schema.encode(walLsnFromString)(startLsn)
            )} (proto_version '1', publication_names '${publicationNames.join(
              ','
            )}')`,
          })
        );

        // wait for this before streaming
        yield* _(readOrFail('CopyBothResponse'));

        const messages = Stream.fromPull(
          Effect.succeed(
            read.pipe(
              Effect.mapError((error) => {
                if (error._tag === 'NoMoreMessagesError') {
                  return Option.none();
                }
                return Option.some(error);
              }),
              Effect.map(Chunk.of)
            )
          )
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
          Stream.map((lsn): PgClientMessageTypes => {
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

        const sink = messageSocket.writeSink;

        yield* _(Stream.run(source, sink));
      }).pipe(Effect.scoped);

    const command = ({ sql }: { sql: string }) =>
      Effect.gen(function* (_) {
        yield* _(messageSocket.write({ type: 'Query', sql }));

        const { commandTag } = yield* _(
          readUntilReady(
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

    const query = <F, T, A extends readonly T[]>({
      sql,
      schema,
      options,
    }: {
      sql: string;
      schema: Schema.Schema<F, A>;
      options?: MakeValueTypeParserOptions;
    }) =>
      Effect.gen(function* (_) {
        yield* _(messageSocket.write({ type: 'Query', sql }));

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
          readUntilReady(
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

    return {
      query,
      command,
      recvlogical,
      read,
      readOrFail,
      readUntilReady,
      write,
    };
  });

const startup = ({
  socket,
  database,
  username,
  password,
  replication,
}: {
  socket: PgSocket;
  database: string;
  username: string;
  password: string;
  replication?: boolean;
}) =>
  Effect.gen(function* (_) {
    const { readOrFail, readUntilReady, write } = socket;

    const parameters = [
      {
        name: 'database',
        value: database,
      },
      {
        name: 'user',
        value: username,
      },
    ];
    if (replication) {
      parameters.push({
        name: 'replication',
        value: 'database',
      });
    }
    yield* _(
      write({
        type: 'StartupMessage',
        protocolVersion: 196608,
        parameters,
      })
    );

    const reply = yield* _(
      readOrFail(
        'AuthenticationCleartextPassword',
        'AuthenticationMD5Password',
        'AuthenticationSASL'
      )
    );

    // CLEARTEXT PASSWORD
    if (reply.type === 'AuthenticationCleartextPassword') {
      yield* _(
        write({
          type: 'PasswordMessage',
          password,
        })
      );
      // MD5 HASHED PASSWORD
    } else if (reply.type === 'AuthenticationMD5Password') {
      yield* _(
        write({
          type: 'PasswordMessage',
          password: createHash('md5')
            .update(
              createHash('md5')
                .update(password.concat(username))
                .digest('hex')
                .concat(Buffer.from(reply.salt).toString('hex'))
            )
            .digest('hex'),
        })
      );
      // SASL
    } else {
      const mechanism = reply.mechanisms.find(
        (item) => item === 'SCRAM-SHA-256'
      );

      if (!mechanism) {
        return yield* _(
          Effect.fail(
            new PgFailedAuth({
              reply,
              msg: 'SCRAM-SHA-256 not a supported mechanism',
            })
          )
        );
      }

      const clientNonce = randomBytes(18).toString('base64');
      const clientFirstMessageHeader = 'n,,';
      const clientFirstMessageBody = `n=*,r=${clientNonce}`;
      const clientFirstMessage = `${clientFirstMessageHeader}${clientFirstMessageBody}`;

      yield* _(
        write({
          type: 'SASLInitialResponse',
          mechanism,
          clientFirstMessage,
        })
      );

      const saslContinue = yield* _(readOrFail('AuthenticationSASLContinue'));

      const { iterationCount, salt, nonce, serverFirstMessage } = saslContinue;
      if (!nonce.startsWith(clientNonce)) {
        yield* _(
          Effect.fail(
            new PgFailedAuth({ reply: saslContinue, msg: 'bad nonce' })
          )
        );
      }

      const saltedPassword = Hi(password, salt, iterationCount);
      const clientKey = hmacSha256(saltedPassword, 'Client Key');
      const storedKey = sha256(clientKey);
      const clientFinalMessageWithoutProof = `c=${Buffer.from(
        clientFirstMessageHeader
      ).toString('base64')},r=${nonce}`;
      const authMessage = `${clientFirstMessageBody},${serverFirstMessage},${clientFinalMessageWithoutProof}`;

      const clientSignature = hmacSha256(storedKey, authMessage);
      const clientProofBytes = xorBuffers(clientKey, clientSignature);
      const clientProof = clientProofBytes.toString('base64');

      const serverKey = hmacSha256(saltedPassword, 'Server Key');
      const serverSignature = hmacSha256(serverKey, authMessage);
      const clientFinalMessage =
        clientFinalMessageWithoutProof + ',p=' + clientProof;

      yield* _(write({ type: 'SASLResponse', clientFinalMessage }));

      const saslFinal = yield* _(readOrFail('AuthenticationSASLFinal'));

      if (
        serverSignature.compare(
          saslFinal.serverFinalMessage.serverSignature
        ) !== 0
      ) {
        yield* _(
          Effect.fail(
            new PgFailedAuth({
              reply: saslFinal,
              msg: 'expected signature to match',
            })
          )
        );
      }
    }

    yield* _(readOrFail('AuthenticationOk'));

    const results = yield* _(
      readUntilReady(
        pipe(
          item,
          P.filter(hasTypeOf('ParameterStatus', 'BackendKeyData')),
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

    const serverParameters = new Map(
      results.filter(isParameterStatus).map(({ name, value }) => [name, value])
    );

    const backendKeyData = results.find(isBackendKeyData);

    return { serverParameters, backendKeyData };
  });

export const make = ({ useSSL, ...options }: Options) =>
  Effect.gen(function* (_) {
    const socket = yield* _(makeSocket(options));

    let base: BaseSocket;
    if (useSSL || useSSL === undefined) {
      const sslRequestReply = yield* _(
        makeMessageSocket({
          socket,
          parser: pgSSLRequestResponse,
          encoder: makePgClientMessage,
        }).pipe(
          Effect.flatMap(({ write, read }) =>
            write({ type: 'SSLRequest', requestCode: 80877103 }).pipe(
              Effect.flatMap(() => read)
            )
          ),
          Effect.scoped
        )
      );

      if (sslRequestReply.useSSL) {
        base = yield* _(socket.upgradeToSSL);
      } else {
        if (useSSL) {
          return yield* _(
            Effect.fail(
              new PgFailedAuth({
                msg: 'Postgres server does not support SSL',
                reply: sslRequestReply,
              })
            )
          );
        }

        yield* _(Effect.logWarning('Postgres server does not support SSL'));

        base = socket;
      }
    } else {
      base = socket;
    }

    const pgSocket = yield* _(makePgSocket({ socket: base }));

    const info = yield* _(startup({ socket: pgSocket, ...options }));

    return { ...pgSocket, info };
  });
