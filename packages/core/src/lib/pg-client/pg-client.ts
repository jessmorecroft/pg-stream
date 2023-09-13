import {
  Chunk,
  Data,
  Effect,
  Either,
  Option,
  Queue,
  Ref,
  Sink,
  Stream,
  identity,
  pipe,
} from 'effect';
import * as P from 'parser-ts/Parser';
import * as E from 'fp-ts/Either';
import * as B from '../parser/buffer';
import {
  BackendKeyData,
  DataRow,
  ErrorResponse,
  ParameterStatus,
  pgSSLRequestResponse,
  pgServerMessageParser,
} from '../pg-protocol/message-parsers';
import { ParseError } from 'parser-ts/lib/ParseResult';
import {
  MakeValueTypeParserOptions,
  makePgClientMessage,
  makeValueTypeParser,
} from '../pg-protocol';
import { BaseSocket, make as makeSocket } from '../socket/socket';
import {
  PgServerMessageTypes,
  NoticeResponse,
} from '../pg-protocol/message-parsers';
import { createHash, randomBytes } from 'crypto';
import { Hi, hmacSha256, sha256, xorBuffers } from './util';
import { logBackendMessage } from './util';
import * as S from 'parser-ts/string';
import * as Schema from '@effect/schema/Schema';
import { formatErrors } from '@effect/schema/TreeFormatter';
import { serverFirstMessageParser } from '../pg-protocol/sasl/message-parsers';

export interface Options {
  host: string;
  port: number;
  database: string;
  username: string;
  password: string;
  useSSL?: boolean;
}

export type PgClient = Effect.Effect.Success<ReturnType<typeof make>>;

export type PgSocket = Effect.Effect.Success<ReturnType<typeof makePgSocket>>;

export class PgParseError extends Data.TaggedClass('PgParseError')<{
  cause: ParseError<unknown>;
}> {}

export class PgRowParseError extends Data.TaggedClass('PgRowParseError')<{
  message: string;
}> {}

export class PgReadEnded extends Data.TaggedClass('PgReadEnded')<
  Record<string, never>
> {}

export class PgFailedAuth extends Data.TaggedClass('PgFailedAuth')<{
  reply: unknown;
  msg?: string;
}> {}

export class PgUnexpectedMessage extends Data.TaggedClass(
  'PgUnexpectedMessage'
)<{
  unexpected: unknown;
  msg?: string;
}> {}

export class PgServerError extends Data.TaggedClass('PgServerError')<{
  error: ErrorResponse;
}> {}

const decode =
  <T>(parser: P.Parser<number, T>) =>
  (buf: Buffer) => {
    return pipe(
      P.many1(parser)(B.stream(buf)),
      E.fold(
        (cause) => {
          if (cause.fatal) {
            return Effect.fail(new PgParseError({ cause }));
          }
          return Effect.succeed([Chunk.empty<T>(), buf] as const);
        },
        (result) => {
          const { cursor } = result.next;
          const unusedBuf = buf.subarray(cursor);
          return Effect.succeed([
            Chunk.fromIterable(result.value),
            unusedBuf,
          ] as const);
        }
      ),
      Effect.unified
    );
  };

export const makeMessageSocket = <I extends { type: string }, O>({
  socket,
  parser,
  encoder,
}: {
  socket: BaseSocket;
  parser: P.Parser<number, I>;
  encoder: (message: O) => Buffer;
}) =>
  Effect.gen(function* (_) {
    const { end, pullStream, push, pushSink } = socket;

    const readStream = pullStream.pipe(
      Stream.scanEffect(
        [Chunk.empty(), Buffer.from([])],
        (s: readonly [Chunk.Chunk<I>, Buffer], a: Buffer) =>
          decode(parser)(Buffer.concat([s[1], a]))
      ),
      Stream.flatMap(([chunk]) => Stream.fromChunks(chunk))
    );

    const readQueue = yield* _(
      Stream.toQueueOfElements(readStream, {
        capacity: 5,
      })
    );

    const writeSink = pushSink.pipe(Sink.contramap(encoder));

    const read = Effect.flatMap(Queue.take(readQueue), identity).pipe(
      Effect.mapError((e) => (Option.isNone(e) ? new PgReadEnded({}) : e.value))
    );

    const readOrFail = <T extends I['type']>(type: T, ...types: T[]) =>
      Effect.filterOrFail(
        read,
        (msg): msg is I & { type: T } =>
          type === msg.type || types.includes(msg.type as T),
        (msg) =>
          new PgUnexpectedMessage({
            unexpected: msg,
            msg: `expected message to be any of ${[type, ...types]}`,
          })
      );

    const readUntil =
      <T extends I['type']>(type: T, ...types: T[]) =>
      (isLast: (msg: I & { type: T }) => boolean) =>
        Effect.gen(function* (_) {
          const doneRef = yield* _(Ref.make(false));
          return yield* _(
            Stream.runCollect(
              Stream.repeatEffectOption(
                Effect.flatMap(Ref.get(doneRef), (done) =>
                  done
                    ? Effect.fail(Option.none())
                    : readOrFail(type, ...types).pipe(
                        Effect.mapError(Option.some),
                        Effect.flatMap((msg) =>
                          isLast(msg)
                            ? Effect.as(Ref.set(doneRef, true), msg)
                            : Effect.succeed(msg)
                        )
                      )
                )
              )
            )
          );
        });

    const write = (message: O) =>
      push(Option.some(Chunk.of(encoder(message)))).pipe(
        Effect.catchAll(([ret]) =>
          Either.isLeft(ret) ? Effect.fail(ret.left) : Effect.unit
        )
      );

    return {
      readQueue,
      read,
      readOrFail,
      readUntil,
      write,
      writeSink,
      end,
    };
  });

const makePgSocket = ({ socket }: { socket: BaseSocket }) =>
  Effect.gen(function* (_) {
    const clientSocket = yield* _(
      makeMessageSocket({
        socket,
        parser: pgServerMessageParser,
        encoder: makePgClientMessage,
      })
    );

    const logNotices = <T extends PgServerMessageTypes>(msgs: Iterable<T>) =>
      Effect.forEach(
        msgs,
        (
          msg
        ): Effect.Effect<
          never,
          PgServerError,
          Exclude<T, NoticeResponse | ErrorResponse>[]
        > => {
          switch (msg.type) {
            case 'NoticeResponse': {
              return Effect.as(logBackendMessage(msg), []);
            }
            case 'ErrorResponse': {
              return Effect.zipRight(
                logBackendMessage(msg),
                Effect.fail(new PgServerError({ error: msg }))
              );
            }
            default: {
              return Effect.succeed([
                msg as Exclude<T, NoticeResponse | ErrorResponse>,
              ]);
            }
          }
        }
      ).pipe(Effect.map((chunks) => chunks.flat()));

    const readOrFail = <T extends PgServerMessageTypes['type']>(
      type: T,
      ...types: T[]
    ) =>
      clientSocket.readOrFail(type, ...types).pipe(
        Effect.tapError((error) => {
          if (error._tag === 'PgUnexpectedMessage') {
            const unexpected = error.unexpected as PgServerMessageTypes;
            if (unexpected.type === 'ErrorResponse') {
              return logBackendMessage(unexpected);
            }
          }
          return Effect.unit;
        })
      );

    const executeSql = <F, T>({
      sql,
      schema,
      options = { parseBigInts: true, parseDates: true, parseNumerics: true },
    }: {
      sql: string;
      schema: Schema.Schema<F, T>;
      options?: MakeValueTypeParserOptions;
    }) =>
      Effect.gen(function* (_) {
        yield* _(clientSocket.write({ type: 'Query', sql }));

        const [rowDescription] = yield* _(
          clientSocket
            .readUntil(
              'ErrorResponse',
              'NoticeResponse',
              'RowDescription'
            )((msg) => msg.type === 'RowDescription')
            .pipe(Effect.flatMap(logNotices))
        );

        const parsers = rowDescription.fields.map(({ name, dataTypeId }) => ({
          name,
          parser: makeValueTypeParser(dataTypeId, options),
        }));

        const dataRows = (yield* _(
          clientSocket
            .readUntil(
              'ErrorResponse',
              'NoticeResponse',
              'DataRow',
              'ReadyForQuery'
            )((msg) => msg.type === 'ReadyForQuery')
            .pipe(Effect.flatMap(logNotices))
        )).filter((msg): msg is DataRow => msg.type === 'DataRow');

        const rows = dataRows.map((row) =>
          parsers.reduce((acc, { name, parser }, index) => {
            const input = row.values[index];
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
          }, {} as object)
        );

        return yield* _(
          Schema.parse(Schema.array(schema))(rows).pipe(
            Effect.mapError(
              (pe) => new PgRowParseError({ message: formatErrors(pe.errors) })
            ),
            Effect.tapError((pe) => Effect.logError(`\n${pe.message}`))
          )
        );
      });

    return { ...clientSocket, readOrFail, logNotices, executeSql };
  });

const startup = ({
  socket,
  database,
  username,
  password,
}: {
  socket: PgSocket;
  database: string;
  username: string;
  password: string;
}) =>
  Effect.gen(function* (_) {
    const { readOrFail, readUntil, logNotices, write } = socket;

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
      if (mechanism) {
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

        const { iterationCount, salt, nonce, serverFirstMessage } =
          saslContinue;
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
      } else {
        yield* _(
          Effect.fail(
            new PgFailedAuth({
              reply,
              msg: 'SCRAM-SHA-256 not a supported mechanism',
            })
          )
        );
      }
    }

    yield* _(readOrFail('AuthenticationOk'));

    const results = yield* _(
      readUntil(
        'ReadyForQuery',
        'ErrorResponse',
        'NoticeResponse',
        'BackendKeyData',
        'ParameterStatus'
      )((msg) => msg.type === 'ReadyForQuery').pipe(Effect.flatMap(logNotices))
    );

    const serverParameters = new Map(
      results
        .filter((msg): msg is ParameterStatus => msg.type === 'ParameterStatus')
        .map(({ name, value }) => [name, value])
    );

    const backendKeyData = results.find(
      (msg): msg is BackendKeyData => msg.type === 'BackendKeyData'
    );

    return { serverParameters, backendKeyData };
  });

export const make = ({ useSSL, ...options }: Options) =>
  Effect.gen(function* (_) {
    const socket = yield* _(makeSocket(options));

    let base: BaseSocket;
    if (useSSL) {
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

      if (!sslRequestReply.useSSL) {
        yield* _(
          Effect.fail(
            new PgFailedAuth({
              msg: 'server does not support SSL',
              reply: sslRequestReply,
            })
          )
        );
      }

      base = yield* _(socket.upgradeToSSL);
    } else {
      base = socket;
    }

    const pgSocket = yield* _(makePgSocket({ socket: base }));

    const info = yield* _(startup({ socket: pgSocket, ...options }));

    const { executeSql } = pgSocket;

    return { executeSql, info };
  });
