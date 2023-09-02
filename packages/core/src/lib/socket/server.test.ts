import { Chunk, Effect, Stream } from 'effect';
import { make } from './server';
import { connect, Socket } from './socket';

const echo = (socket: Socket) => {
  return Stream.runDrain(Stream.run(socket.pullStream, socket.pushSink));
};

it('should listen', async () => {
  const { listen } = make({ host: 'localhost' });

  const program = Effect.flatMap(listen, ({ sockets, address }) =>
    Effect.zipRight(
      Stream.runDrain(
        Stream.take(sockets, 1).pipe(
          Stream.mapEffect((socket) =>
            Effect.flatMap(socket, echo).pipe(Effect.scoped)
          )
        )
      ).pipe(Effect.tap(() => Effect.logInfo('echo is absolutely done'))),
      connect({ host: address.address, port: address.port }).pipe(
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
