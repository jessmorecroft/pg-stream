import { makePush } from './make-push';
import { FileSystem, layer } from '@effect/platform-node/FileSystem';
import { Effect, Option, Chunk, Either, Sink, Stream } from 'effect';
import { createWriteStream } from 'fs';

it('should push "manually"', async () => {
  const program = FileSystem.pipe(
    Effect.flatMap((fs) =>
      fs.makeTempFileScoped().pipe(
        Effect.flatMap((filename) =>
          makePush<Buffer>(() => createWriteStream(filename), {
            endOnClose: true,
          }).pipe(
            Effect.flatMap((push) =>
              Effect.gen(function* (_) {
                yield* _(push(Option.some(Chunk.of(Buffer.from('hello')))));
                yield* _(
                  push(
                    Option.some(
                      Chunk.fromIterable([
                        Buffer.from('world'),
                        Buffer.from('!'),
                      ])
                    )
                  )
                );
                yield* _(push(Option.none()));
              })
            ),
            Effect.scoped,
            Effect.catchAll(([either]) =>
              Either.isLeft(either) ? Effect.fail(either.left) : Effect.unit
            ),
            Effect.flatMap(() =>
              Effect.map(fs.readFile(filename), (_) =>
                Buffer.from(_).toString()
              )
            )
          )
        )
      )
    ),
    Effect.scoped
  );

  const result = await Effect.runPromise(
    program.pipe(Effect.provideLayer(layer))
  );

  expect(result).toEqual('helloworld!');
});

it('should push as sink', async () => {
  const program = FileSystem.pipe(
    Effect.flatMap((fs) =>
      fs.makeTempFileScoped().pipe(
        Effect.flatMap((filename) => {
          const sink = Sink.fromPush(
            makePush<Buffer>(() => createWriteStream(filename), {
              endOnClose: true,
            })
          );

          return Stream.runDrain(
            Stream.transduce(
              Stream.map(Stream.fromIterable(['how', 'you', 'doing?']), (_) =>
                Buffer.from(_)
              ),
              sink
            )
          ).pipe(
            Effect.flatMap(() =>
              Effect.map(fs.readFile(filename), (_) =>
                Buffer.from(_).toString()
              )
            )
          );
        }),
        Effect.scoped
      )
    )
  );

  const result = await Effect.runPromise(
    program.pipe(Effect.provideLayer(layer))
  );

  expect(result).toEqual('howyoudoing?');
});
