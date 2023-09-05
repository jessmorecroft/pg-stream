import * as pgSocket from './pg-socket';
import * as socket from '../socket/socket';
import * as serverSocket from '../socket/server';
import { makePgServerMessage, pgClientMessageParser } from '../pg-protocol';
import { Data, Effect, Stream } from 'effect';

export type PgServerSocket = Effect.Effect.Success<
  ReturnType<typeof makeSocket>
>;

export type Options = Omit<pgSocket.Options, 'useSSL' | 'host' | 'port'> & {
  host?: string;
  port?: number;
};

export class PgTestServerError extends Data.TaggedClass('PgTestServerError')<{
  msg?: string;
}> {}

export const startup = ({
  socket,
  ...options
}: { socket: PgServerSocket } & Options) =>
  Effect.gen(function* (_) {
    const { write, readOrFail } = socket;

    const { parameters } = yield* _(readOrFail('StartupMessage'));

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

export const makeSocket = ({ socket }: { socket: socket.Socket }) =>
  pgSocket.makeSocket({
    socket,
    parser: pgClientMessageParser,
    encoder: makePgServerMessage,
  });

export const makeServerSocket = ({
  socket,
  ...options
}: { socket: socket.Socket } & Options) =>
  Effect.gen(function* (_) {
    const sock = yield* _(makeSocket({ socket }));

    yield* _(startup({ socket: sock, ...options }));

    return sock;
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
