import { Chunk, Effect, Option, Stream } from 'effect';
import { pull, toStreamable } from './readable';
import { Readable } from 'stream';

it('should pull from iterable', async () => {
  const program = pull<string>(
    Readable.from(['one', 'two'], { objectMode: true }),
    {
      waitForClose: true,
    }
  ).pipe(
    Effect.flatMap((reader) =>
      Effect.gen(function* (_) {
        const one = yield* _(reader);
        const two = yield* _(reader);
        const end = yield* _(
          Effect.catchAll(reader, (e) =>
            Option.isSome(e) ? Effect.fail(e.value) : Effect.succeed('done')
          )
        );

        return { one, two, end };
      })
    ),
    Effect.scoped
  );

  const { one, two, end } = await Effect.runPromise(program);

  expect(one).toEqual('one');
  expect(two).toEqual('two');
  expect(end).toEqual('done');
});

it('should pull as stream', async () => {
  const stream = Stream.fromPull(
    pull(
      Readable.from([Buffer.from('one'), Buffer.from('two')], {
        objectMode: true,
      }),
      {
        waitForClose: true,
      }
    ).pipe(Effect.map(toStreamable))
  );

  const program = Stream.runCollect(stream).pipe(Effect.scoped);
  const chunks = await Effect.runPromise(program);

  expect(Chunk.toReadonlyArray(chunks)).toEqual([
    Buffer.from('one'),
    Buffer.from('two'),
  ]);
});

it('should be simpler if not waiting for a close', async () => {
  const stream = Stream.fromPull(
    Effect.succeed(
      toStreamable(
        pull(
          Readable.from([Buffer.from('one'), Buffer.from('two')], {
            objectMode: true,
          })
        )
      )
    )
  );

  const program = Stream.runCollect(stream).pipe(Effect.scoped);
  const chunks = await Effect.runPromise(program);

  expect(Chunk.toReadonlyArray(chunks)).toEqual([
    Buffer.from('one'),
    Buffer.from('two'),
  ]);
});
