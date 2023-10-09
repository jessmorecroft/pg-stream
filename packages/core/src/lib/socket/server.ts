import { Chunk, Data, Effect, Option, Scope, Stream } from 'effect';
import * as net from 'net';
import * as tls from 'tls';
import { listen } from '../util/util';
import { FileSystem } from '@effect/platform-node/FileSystem';
import { PlatformError } from '@effect/platform-node/Error';

export type Server = ReturnType<typeof make>;

export type StreamSuccess<T> = T extends Stream.Stream<
  unknown,
  unknown,
  infer A
>
  ? A
  : never;

export interface Options {
  host?: string;
  port?: number;
}

export interface SSLOptions {
  keyFile: string;
  certFile: string;
}

export class ServerError extends Data.TaggedClass('ServerError')<{
  cause: Error;
}> {}

const onListening: (server: net.Server) => Effect.Effect<never, never, void> = (
  server
) =>
  listen({
    emitter: server,
    event: 'listening',
    onEvent: () => Effect.unit,
    get: (_) => (_.listening ? Option.some<void>(undefined) : Option.none()),
  });

const onClose: (server: net.Server) => Effect.Effect<never, never, void> = (
  server
) =>
  listen({
    emitter: server,
    event: 'close',
    onEvent: () => Effect.unit,
    get: () => Option.none(),
  });

const onError: (
  server: net.Server
) => Effect.Effect<never, ServerError, never> = (server) =>
  listen({
    emitter: server,
    event: 'error',
    onEvent: (cause: Error) => Effect.fail(new ServerError({ cause })),
    get: () => Option.none(),
  });

const onConnection = (server: net.Server) =>
  Stream.asyncScoped<Scope.Scope, never, net.Socket>((emit) => {
    const fn = (socket: net.Socket) => emit(Effect.succeed(Chunk.of(socket)));
    server.on('connection', fn);
    return Effect.addFinalizer(() => {
      server.off('connection', fn);
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
  });

export const tlsConnect = (
  socket: net.Socket,
  { keyFile, certFile }: SSLOptions
): Effect.Effect<FileSystem, PlatformError, tls.TLSSocket> =>
  Effect.flatMap(FileSystem, (fs) =>
    Effect.all({
      key: fs.readFile(keyFile),
      cert: fs.readFile(certFile),
    })
  ).pipe(
    Effect.map(
      ({ cert, key }) =>
        new tls.TLSSocket(socket, {
          isServer: true,
          enableTrace: false,
          key: Buffer.from(key),
          cert: Buffer.from(cert),
        })
    ),
    Effect.tap(() => Effect.log('server socket upgrading to SSL'))
  );

export const make = ({ host, port }: Options) => {
  const server = net.createServer({ allowHalfOpen: false });

  const listen = Effect.suspend(() => {
    server.listen({ host, port });
    return Effect.raceAll([onListening(server), onError(server)]);
  }).pipe(
    Effect.map(() => ({
      sockets: onConnection(server).pipe(
        Stream.merge(Stream.fromEffect(onError(server))),
        Stream.merge(Stream.fromEffect(onClose(server)).pipe(Stream.drain), {
          haltStrategy: 'right',
        })
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
