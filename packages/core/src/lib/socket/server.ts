import { Chunk, Effect, Stream } from 'effect';
import * as net from 'net';
import { make as makeSocket } from './socket';

export type ServerSocket = ReturnType<typeof make>;

interface Options {
  host?: string;
  port?: number;
}

const listeners = (server: net.Server) => ({
  listening: Effect.asyncInterrupt<never, never, void>((cb, signal) => {
    const fn = () => cb(Effect.unit);
    if (server.listening) {
      fn();
      return;
    }
    server.once('listening', fn);
    signal.onabort = () => {
      server.removeListener('listening', fn);
    };
  }),
  connection: Stream.asyncScoped<never, never, net.Socket>((cb) => {
    const fn = (socket: net.Socket) => cb(Effect.succeed(Chunk.of(socket)));
    server.on('connection', fn);
    return Effect.addFinalizer(() => {
      server.removeListener('connection', fn);
      return Effect.async<never, never, void>((cb) => {
        server.close((error) => {
          if (error) {
            cb(
              Effect.as(
                Effect.logWarning(`error on server close: ${error.message}`),
                undefined
              )
            );
            return;
          }
          cb(Effect.as(Effect.logInfo('server closed'), undefined));
        });
      });
    });
  }),
  close: Effect.asyncInterrupt<never, never, void>((cb, signal) => {
    const fn = () => cb(Effect.unit);
    server.once('close', fn);
    signal.onabort = () => {
      server.removeListener('close', fn);
    };
  }),
  error: Effect.asyncInterrupt<never, Error, never>((cb, signal) => {
    const fn = (error: Error) => cb(Effect.fail(error));
    server.once('error', fn);
    signal.onabort = () => {
      server.removeListener('error', fn);
    };
    return Effect.sync(() => server.removeListener('error', fn));
  }),
});

export const make = ({ host, port }: Options) => {
  const server = net.createServer();

  const { connection, error, close, listening } = listeners(server);

  const listen = Effect.suspend(() => {
    server.listen({ host, port });
    return Effect.raceAll([listening, error]);
  }).pipe(
    Effect.map(() => ({
      sockets: connection.pipe(
        Stream.merge(Stream.fromEffect(error)),
        Stream.merge(Stream.fromEffect(close).pipe(Stream.drain), {
          haltStrategy: 'right',
        }),
        Stream.map(makeSocket)
      ),
      address: server.address() as net.AddressInfo,
    })),
    Effect.tap(({ address }) =>
      Effect.logInfo(
        `server listening on address ${address.address}, port ${address.port}`
      )
    )
  );

  return { listen };
};
