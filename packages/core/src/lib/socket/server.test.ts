import { Chunk, Duration, Effect, Stream } from 'effect';
import { ServerSocket, make as makeServer } from './server';
import { BaseSocket, make as makeClient } from './socket';
import { layer } from '@effect/platform-node/FileSystem';

const echo = (socket: BaseSocket) => {
  return Stream.runDrain(Stream.run(socket.pullStream, socket.pushSink));
};

it('should listen', async () => {
  const { listen } = makeServer({ host: 'localhost' });

  const program = Effect.flatMap(listen, ({ sockets, address }) =>
    Effect.zipRight(
      Stream.runDrain(
        Stream.take(sockets, 1).pipe(
          Stream.mapEffect((socket) =>
            Effect.flatMap(socket, echo).pipe(Effect.scoped)
          )
        )
      ).pipe(Effect.tap(() => Effect.logInfo('non-SSL echo is done'))),
      makeClient({ host: address.address, port: address.port }).pipe(
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
            ).pipe(Effect.map((_) => Chunk.toReadonlyArray(_).join())),
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

  const result = await Effect.runPromise(program.pipe(Effect.scoped));

  expect(result).toEqual('thequickbrownfoxjumpedoverthelazydog!');
});

it('should listen, then upgrade to SSL', async () => {
  const upgradeThenEcho = (socket: ServerSocket) =>
    Effect.flatMap(socket.upgradeToSSL, echo);

  const { listen } = makeServer({
    host: 'localhost',
    ssl: {
      certFile: __dirname + '/resources/server-cert.pem',
      keyFile: __dirname + '/resources/server-key.pem',
    },
  });

  const program = Effect.flatMap(listen, ({ sockets, address }) =>
    Effect.zipRight(
      Stream.runDrain(
        Stream.take(sockets, 1).pipe(
          Stream.mapEffect((socket) =>
            Effect.flatMap(socket, upgradeThenEcho).pipe(Effect.scoped)
          )
        )
      ).pipe(Effect.tap(() => Effect.logInfo('SSL echo is done'))),
      makeClient({ host: address.address, port: address.port }).pipe(
        Effect.flatMap((_) => {
          return Effect.flatMap(_.upgradeToSSL, (socket) =>
            Effect.zipRight(
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
            )
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
