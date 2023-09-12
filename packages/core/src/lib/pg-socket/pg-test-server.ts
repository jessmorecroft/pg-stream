import * as pgSocket from './pg-socket';
import * as serverSocket from '../socket/server';
import {
  StartupMessage,
  makePgServerMessage,
  pgClientMessageParser,
} from '../pg-protocol';
import { Data, Effect, Stream } from 'effect';
import { BaseSocket } from '../socket/socket';
import { WritableError } from '../stream/push';

export type PgTestServerSocket = Effect.Effect.Success<
  ReturnType<typeof makeSocket>
>;

export type Options = Omit<pgSocket.Options, 'useSSL' | 'host' | 'port'> &
  serverSocket.Options;

export class PgTestServerError extends Data.TaggedClass('PgTestServerError')<{
  msg?: string;
}> {}

export const startup = (
  socket: PgTestServerSocket,
  initial: StartupMessage,
  options: Options
) =>
  Effect.gen(function* (_) {
    const { write, readOrFail } = socket;

    const { parameters } = initial;

    if (
      parameters.find(({ name }) => name === 'database')?.value !==
        options.database ||
      parameters.find(({ name }) => name === 'user')?.value !== options.username
    ) {
      yield* _(
        write({
          type: 'ErrorResponse',
          errors: [
            {
              type: 'V',
              value: 'FATAL',
            },
            {
              type: 'M',
              value: 'bad parameters',
            },
          ],
        })
      );

      yield* _(Effect.fail(new PgTestServerError({ msg: 'bad parameters' })));
    }

    yield* _(write({ type: 'AuthenticationCleartextPassword' }));

    const { password } = yield* _(readOrFail('PasswordMessage'));

    if (password === options.password) {
      yield* _(write({ type: 'AuthenticationOk' }));
    } else {
      yield* _(
        write({
          type: 'ErrorResponse',
          errors: [
            {
              type: 'V',
              value: 'FATAL',
            },
            {
              type: 'M',
              value: 'bad password',
            },
          ],
        })
      );

      yield* _(Effect.fail(new PgTestServerError({ msg: 'bad password' })));
    }

    yield* _(
      write({
        type: 'NoticeResponse',
        notices: [
          {
            type: 'V',
            value: 'WARNING',
          },
          {
            type: 'M',
            value: 'this is a test server',
          },
        ],
      })
    );

    for (const { name, value } of parameters) {
      yield* _(write({ type: 'ParameterStatus', name, value }));
    }

    yield* _(
      write({ type: 'ParameterStatus', name: 'password', value: password })
    );

    yield* _(write({ type: 'ReadyForQuery', transactionStatus: 'T' }));
  });

export const makeSocket = ({ socket }: { socket: BaseSocket }) =>
  pgSocket.makeSocket({
    socket,
    parser: pgClientMessageParser,
    encoder: makePgServerMessage,
  });

const makeServerSocket = ({
  socket,
  ...options
}: { socket: serverSocket.ServerSocket } & Options) =>
  Effect.gen(function* (_) {
    const msg = yield* _(
      makeSocket({ socket }).pipe(
        Effect.flatMap((pgSocket) =>
          pgSocket.readOrFail('StartupMessage', 'SSLRequest').pipe(
            Effect.flatMap(
              (
                msg
              ): Effect.Effect<never, WritableError, StartupMessage | void> => {
                if (msg.type === 'SSLRequest') {
                  return pgSocket.write({
                    type: 'SSLRequestResponse',
                    useSSL: !!options.ssl,
                  });
                }
                return Effect.succeed(msg);
              }
            )
          )
        ),
        Effect.scoped
      )
    );

    if (msg) {
      const pgSocket = yield* _(makeSocket({ socket }));
      yield* _(startup(pgSocket, msg, options));
      return pgSocket;
    }
    if (!options.ssl) {
      yield* _(
        Effect.fail(new PgTestServerError({ msg: 'client requested SSL' }))
      );
    }
    const tlsSocket = yield* _(socket.upgradeToSSL);
    const pgSocket = yield* _(makeSocket({ socket: tlsSocket }));
    const msg2 = yield* _(pgSocket.readOrFail('StartupMessage'));
    yield* _(startup(pgSocket, msg2, options));
    return pgSocket;
  });

export const make = (options: Options) => {
  const ss = serverSocket.make(options);

  const listen = Effect.map(ss.listen, ({ sockets, address }) => ({
    sockets: Stream.mapEffect(sockets, (input) =>
      Effect.map(input, (s) => makeServerSocket({ socket: s, ...options }))
    ),
    address,
  }));

  return { listen };
};
