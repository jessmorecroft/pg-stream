import { Chunk, Effect, Stream } from 'effect';
import { Options, make as makeServer } from './server';
import { BaseSocket, make as makeClient } from './socket';
import { layer } from '@effect/platform-node/FileSystem';

const echo = (socket: BaseSocket) => {
  return Stream.runDrain(Stream.run(socket.pullStream, socket.pushSink));
};

it.each<Options>([
  {
    host: 'localhost',
  },
  {
    host: 'localhost',
    ssl: {
      certFile: __dirname + '/resources/server-cert.pem',
      keyFile: __dirname + '/resources/server-key.pem',
    },
  },
])('should listen', async (options) => {
  const { listen } = makeServer(options);

  const program = Effect.flatMap(listen, ({ sockets, address }) =>
    Effect.zipRight(
      Stream.runDrain(
        Stream.take(sockets, 1).pipe(
          Stream.mapEffect((socket) =>
            Effect.flatMap(socket, (socket) => {
              if (options.ssl) {
                return socket.upgradeToSSL.pipe(Effect.flatMap(echo));
              }
              return echo(socket);
            }).pipe(Effect.scoped)
          )
        )
      ).pipe(Effect.tap(() => Effect.logInfo('non-SSL echo is done'))),
      makeClient({ host: address.address, port: address.port }).pipe(
        Effect.flatMap((socket) =>
          options.ssl ? socket.upgradeToSSL : Effect.succeed(socket)
        ),
        Effect.flatMap((socket) => {
          return Effect.zipRight(
            Stream.runDrain(
              Stream.run(
                Stream.fromIterable(
                  'the quick brown fox jumped over the lazy dog!'.split(' ')
                ).pipe(Stream.map(Buffer.from)),
                socket.pushSink
              )
            ).pipe(Effect.tap(() => socket.end)),
            Stream.runCollect(
              socket.pullStream.pipe(Stream.map((_) => _.toString()))
            ).pipe(Effect.map((_) => Chunk.toReadonlyArray(_).join(''))),
            {
              concurrent: true,
            }
          );
        })
      ),
      {
        concurrent: true,
      }
    )
  );

  const result = await Effect.runPromise(
    program.pipe(Effect.scoped, Effect.provideLayer(layer))
  );

  expect(result).toEqual('thequickbrownfoxjumpedoverthelazydog!');
});
