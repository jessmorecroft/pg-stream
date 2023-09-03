import {
  Chunk,
  Data,
  Effect,
  Either,
  Option,
  Queue,
  Sink,
  Stream,
  identity,
  pipe,
} from 'effect';
import * as P from 'parser-ts/Parser';
import * as E from 'fp-ts/Either';
import * as B from '../parser/buffer';
import * as socket from '../socket/socket';
import {
  AuthenticationCleartextPassword,
  AuthenticationMD5Password,
  AuthenticationOk,
  AuthenticationSASL,
  AuthenticationSASLContinue,
  AuthenticationSASLFinal,
  BackendKeyData,
  ErrorResponse,
  NoticeResponse,
  ParameterStatus,
  ReadyForQuery,
  pgSSLRequestResponse,
  pgServerMessageParser,
} from '../pg-protocol/message-parsers';
import { ParseError } from 'parser-ts/lib/ParseResult';
import { makePgClientMessage } from '../pg-protocol';
import { BaseSocket } from '../socket/socket';
import { PgServerMessageTypes } from '../pg-protocol/message-parsers';
import { createHash, randomBytes } from 'crypto';
import { Hi, hmacSha256, sha256, xorBuffers } from './util';
import { logBackendMessage } from './util';

export interface Options {
  host: string;
  port: number;
  database: string;
  username: string;
  password: string;
  useSSL?: boolean;
}

export type PgClient = Effect.Effect.Success<ReturnType<typeof make>>;

export type PgSocket = Effect.Effect.Success<
  ReturnType<typeof makeClientSocket>
>;
export class PgParseError extends Data.TaggedClass('PgParseError')<{
  cause: ParseError<unknown>;
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
  msg: unknown;
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

export const makeSocket = <I extends { type: string }, O>({
  socket,
  parser,
  encoder,
}: {
  socket: socket.BaseSocket;
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

    const readOrFail = <T extends I>(...type: T['type'][]) =>
      Effect.filterOrFail(
        read,
        (msg): msg is T => type.includes(msg.type),
        (msg) => new PgUnexpectedMessage({ msg })
      );

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
      write,
      writeSink,
      end,
    };
  });

const makeClientSocket = ({ socket }: { socket: BaseSocket }) =>
  Effect.gen(function* (_) {
    const clientSocket = yield* _(
      makeSocket({
        socket,
        parser: pgServerMessageParser,
        encoder: makePgClientMessage,
      })
    );

    const stripBackendLogs = <E, T extends PgServerMessageTypes>(
      stream: Stream.Stream<never, E, T>
    ) =>
      Stream.flatMap(stream, (msg) => {
        if (msg.type === 'NoticeResponse') {
          return Stream.zipRight(logBackendMessage(msg), Stream.empty);
        }
        if (msg.type === 'BackendKeyData') {
          return Stream.zipRight(
            Effect.log(
              `backend key data from server, pid: ${msg.pid}, secret key: ${msg.secretKey}`
            ),
            Stream.empty
          );
        }
        if (msg.type === 'ErrorResponse') {
          return Stream.zipRight(
            logBackendMessage(msg),
            Stream.fail(new PgServerError({ error: msg }))
          );
        }
        return Stream.succeed(
          msg as Exclude<T, NoticeResponse | BackendKeyData | ErrorResponse>
        );
      });

    const readOrFailUntilReady = <T extends PgServerMessageTypes>(
      ...type: T['type'][]
    ) =>
      Stream.runCollect(
        Stream.repeatEffectOption(
          Effect.flatMap(
            clientSocket
              .readOrFail<
                | ReadyForQuery
                | ErrorResponse
                | NoticeResponse
                | BackendKeyData
                | T
              >(
                ...([
                  'ReadyForQuery',
                  'ErrorResponse',
                  'NoticeResponse',
                  'BackendKeyData',
                  ...type,
                  // eslint-disable-next-line @typescript-eslint/no-explicit-any
                ] as any[])
              )
              .pipe(Effect.mapError(Option.some)),
            (msg) =>
              msg.type === 'ReadyForQuery'
                ? Effect.fail(Option.none())
                : Effect.succeed(msg)
          )
        ).pipe(stripBackendLogs)
      );

    return { ...clientSocket, readOrFailUntilReady };
  });

export const startup = ({
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
    const { readOrFail, readOrFailUntilReady, write } = socket;

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
      readOrFail<
        | AuthenticationCleartextPassword
        | AuthenticationMD5Password
        | AuthenticationSASL
      >(
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
    } else if (reply.type === 'AuthenticationSASL') {
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

        const saslContinue = yield* _(
          readOrFail<AuthenticationSASLContinue>('AuthenticationSASLContinue')
        );

        const { iterationCount, salt, nonce } = saslContinue.serverFirstMessage;
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
        const authMessage = `${clientFirstMessageBody},${saslContinue.serverFirstMessage},${clientFinalMessageWithoutProof}`;

        const clientSignature = hmacSha256(storedKey, authMessage);
        const clientProofBytes = xorBuffers(clientKey, clientSignature);
        const clientProof = clientProofBytes.toString('base64');

        const serverKey = hmacSha256(saltedPassword, 'Server Key');
        const serverSignature = hmacSha256(serverKey, authMessage);
        const clientFinalMessage =
          clientFinalMessageWithoutProof + ',p=' + clientProof;

        yield* _(write({ type: 'SASLResponse', clientFinalMessage }));

        const saslFinal = yield* _(
          readOrFail<AuthenticationSASLFinal>('AuthenticationSASLFinal')
        );

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

    yield* _(readOrFail<AuthenticationOk>('AuthenticationOk'));

    const results = yield* _(
      readOrFailUntilReady<ParameterStatus>('ParameterStatus')
    );

    const serverParameters = new Map(
      Chunk.map(results, ({ name, value }) => [name, value])
    );

    return { serverParameters };
  });

export const make = ({
  socket,
  useSSL,
  ...options
}: {
  socket: socket.Socket;
  useSSL?: boolean;
  username: string;
  password: string;
  database: string;
}) =>
  Effect.gen(function* (_) {
    let sock: socket.BaseSocket;
    if (useSSL) {
      const { useSSL } = yield* _(
        makeSocket({
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

      if (!useSSL) {
        yield* _(Effect.fail(new Error('server does not support ssl')));
      }

      sock = yield* _(socket.upgradeToSSL);
    } else {
      sock = socket;
    }

    const pgSocket = yield* _(makeClientSocket({ socket: sock }));

    const info = yield* _(startup({ socket: pgSocket, ...options }));

    return { ...pgSocket, ...info };
  });

export const connect = (options: Options) =>
  Effect.flatMap(socket.connect(options), (socket) =>
    make({
      socket,
      ...options,
    })
  );
