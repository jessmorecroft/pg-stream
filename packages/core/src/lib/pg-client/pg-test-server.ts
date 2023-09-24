import * as pgClient from './pg-client';
import { makeServer, serverTlsConnect, SSLOptions } from '../socket';
import {
  StartupMessage,
  makePgServerMessage,
  pgClientMessageParser,
} from '../pg-protocol';
import { Data, Effect, Stream } from 'effect';
import * as stream from '../stream';
import { Duplex, Readable, Writable } from 'stream';
import { Socket } from 'net';
import { end } from '../socket/socket';
import { FileSystem } from '@effect/platform-node/FileSystem';

export type Options = Omit<pgClient.Options, 'useSSL' | 'host' | 'port'> & {
  host?: string;
  port?: number;
  ssl?: SSLOptions;
};

export class PgTestServerError extends Data.TaggedClass('PgTestServerError')<{
  msg?: string;
}> {}

export const read = (socket: Readable) =>
  stream.read(socket, stream.decode(pgClientMessageParser));

export const readOrFail = (socket: Readable) => stream.readOrFail(read(socket));

export const write = (socket: Writable) =>
  stream.write(socket, makePgServerMessage);

export const startup = (
  socket: Duplex,
  initial: StartupMessage,
  options: Omit<Options, 'host' | 'port'>
) =>
  Effect.gen(function* (_) {
    const { parameters } = initial;

    if (
      parameters.find(({ name }) => name === 'database')?.value !==
        options.database ||
      parameters.find(({ name }) => name === 'user')?.value !== options.username
    ) {
      yield* _(
        write(socket)({
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

    yield* _(write(socket)({ type: 'AuthenticationCleartextPassword' }));

    const { password } = yield* _(readOrFail(socket)('PasswordMessage'));

    if (password === options.password) {
      yield* _(write(socket)({ type: 'AuthenticationOk' }));
    } else {
      yield* _(
        write(socket)({
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
      write(socket)({
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
      yield* _(write(socket)({ type: 'ParameterStatus', name, value }));
    }

    yield* _(
      write(socket)({
        type: 'ParameterStatus',
        name: 'password',
        value: password,
      })
    );

    yield* _(write(socket)({ type: 'ReadyForQuery', transactionStatus: 'T' }));
  });

export const make = ({ ssl, host, port, ...options }: Options) => {
  const server = makeServer({ host, port });

  const listen = FileSystem.pipe(
    Effect.flatMap((fs) =>
      server.listen.pipe(
        Effect.map(({ sockets, address }) => ({
          sockets: sockets.pipe(
            Stream.map((socket) =>
              Effect.gen(function* (_) {
                let sock: Socket = socket;

                const msg = yield* _(
                  readOrFail(sock)('StartupMessage', 'SSLRequest')
                );

                if (msg.type === 'SSLRequest') {
                  yield* _(
                    write(sock)({ type: 'SSLRequestResponse', useSSL: !!ssl })
                  );

                  if (ssl) {
                    sock = yield* _(serverTlsConnect(sock, ssl));

                    const msg2 = yield* _(readOrFail(sock)('StartupMessage'));

                    yield* _(startup(sock, msg2, options));
                  } else {
                    return yield* _(
                      Effect.fail(new Error('SSL requested but not supported'))
                    );
                  }
                } else {
                  yield* _(startup(sock, msg, options));
                }

                yield* _(Effect.addFinalizer(() => end(sock)));

                return sock;
              }).pipe(Effect.provideService(FileSystem, fs))
            )
          ),
          address,
        }))
      )
    )
  );

  return { listen };
};
