import { Chunk, Data, Effect, Option, Ref, Stream } from 'effect';
import * as net from 'net';
import * as tls from 'tls';
import * as socket from './socket';
import { listen } from '../util/util';
import { FileSystem } from '@effect/platform-node/FileSystem';

export type Server = ReturnType<typeof make>;

export type StreamSuccess<T> = T extends Stream.Stream<
  unknown,
  unknown,
  infer A
>
  ? A
  : never;

export type ServerSocket = Effect.Effect.Success<
  StreamSuccess<Effect.Effect.Success<Server['listen']>['sockets']>
>;

interface Options {
  host?: string;
  port?: number;
  ssl?: {
    keyFile: string;
    certFile: string;
  };
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
  Stream.asyncScoped<never, never, net.Socket>((cb) => {
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
  });

const tlsConnect = (
  base: net.Socket,
  { keyFile, certFile }: NonNullable<Options['ssl']>
) =>
  Effect.acquireRelease(
    Effect.flatMap(FileSystem, (fs) =>
      Effect.all({
        key: fs.readFile(keyFile),
        cert: fs.readFile(certFile),
      })
    ).pipe(
      Effect.flatMap(({ cert, key }) => {
        const tlsSocket = new tls.TLSSocket(base, {
          isServer: true,
          enableTrace: false,
          key: Buffer.from(key),
          cert: Buffer.from(cert),
        });
        return socket.makeBaseSocket(tlsSocket);
      })
    ),
    (socket) => socket.end
  );

export const make = ({ host, port, ssl }: Options) => {
  const server = net.createServer();

  const listen = Effect.suspend(() => {
    server.listen({ host, port });
    return Effect.raceAll([onListening(server), onError(server)]);
  }).pipe(
    Effect.map(() => ({
      sockets: onConnection(server).pipe(
        Stream.merge(Stream.fromEffect(onError(server))),
        Stream.merge(Stream.fromEffect(onClose(server)).pipe(Stream.drain), {
          haltStrategy: 'right',
        }),
        Stream.map((sock) =>
          Effect.acquireRelease(
            Effect.flatMap(socket.makeBaseSocket(sock), (baseSocket) =>
              Effect.gen(function* (_) {
                const upgraded = yield* _(Ref.make(false));

                const upgradeToSSL = Effect.zipLeft(
                  Effect.if(!!ssl, {
                    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
                    onTrue: Effect.suspend(() => tlsConnect(sock, ssl!)),
                    onFalse: Effect.fail(
                      new ServerError({
                        cause: new Error('ssl key/cert not specified'),
                      })
                    ),
                  }).pipe(
                    Effect.tapError((e) => {
                      if (
                        e._tag === 'BadArgument' ||
                        e._tag === 'SystemError'
                      ) {
                        return Effect.logWarning(
                          'upgrade SSL failed: ' + e.message
                        );
                      }
                      return Effect.logWarning(
                        'upgrade SSL failed: ' + e.cause.message
                      );
                    })
                  ),
                  Ref.set(upgraded, true)
                );

                return {
                  ...baseSocket,
                  upgradeToSSL,
                  upgraded,
                };
              })
            ),
            ({ upgraded, end }) =>
              Effect.if(Ref.get(upgraded), {
                onTrue: Effect.unit,
                onFalse: end,
              })
          )
        )
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
