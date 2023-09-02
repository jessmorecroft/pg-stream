import { Chunk, Data, Effect, Either, Option } from 'effect';
import { LazyArg } from 'effect/Function';
import { Writable } from 'stream';

export class WritableError extends Data.TaggedClass('WritableError')<{
  cause: Error;
}> {}

const listeners = (writable: Writable) => ({
  drain: Effect.asyncInterrupt<never, never, void>((cb, signal) => {
    const fn = () => cb(Effect.unit);
    if (!writable.writableNeedDrain) {
      fn();
      return;
    }
    writable.once('drain', fn);
    signal.onabort = () => {
      writable.removeListener('drain', fn);
    };
  }),
  error: Effect.asyncInterrupt<never, WritableError, never>((cb, signal) => {
    const fn = (cause: Error) => cb(Effect.fail(new WritableError({ cause })));
    if (writable.errored) {
      fn(writable.errored);
      return;
    }
    writable.once('error', fn);
    signal.onabort = () => {
      writable.removeListener('error', fn);
    };
  }),
});

export interface FromWritableOptions {
  readonly endOnClose?: boolean;
  readonly encoding?: BufferEncoding;
}

export const makePush = <A>(
  evaluate: LazyArg<Writable>,
  options?: FromWritableOptions
) =>
  Effect.acquireRelease(Effect.sync(evaluate), (writable) => {
    if (writable.errored) {
      return Effect.unit;
    }
    if (options?.endOnClose && !writable.writableEnded) {
      const { error } = listeners(writable);
      return Effect.raceAll([
        Effect.async<never, never, void>((cb) => {
          writable.end(() => cb(Effect.unit));
        }),
        error.pipe(Effect.ignore),
      ]);
    }
    return Effect.unit;
  }).pipe(
    Effect.map((writable) => {
      const { drain, error } = listeners(writable);
      const drainOrError = Effect.raceAll([drain, error]).pipe(
        Effect.mapError((e) => [Either.left(e), Chunk.empty()] as const)
      );
      return (input: Option.Option<Chunk.Chunk<A>>) =>
        Effect.suspend(
          (): Effect.Effect<
            never,
            readonly [Either.Either<WritableError, void>, Chunk.Chunk<never>],
            void
          > => {
            if (writable.errored) {
              return Effect.fail([
                Either.left(new WritableError({ cause: writable.errored })),
                Chunk.empty(),
              ] as const);
            }

            if (Option.isNone(input)) {
              const done = Effect.fail([
                Either.right(undefined),
                Chunk.empty(),
              ] as const);
              if (writable.writableNeedDrain) {
                return Effect.flatMap(drainOrError, () => done);
              }
              return done;
            }

            const chunks = Chunk.toReadonlyArray(input.value);

            const go = (
              _: readonly A[]
            ): Effect.Effect<
              never,
              readonly [
                Either.Either<WritableError, never>,
                Chunk.Chunk<never>
              ],
              void
            > => {
              for (const [index, chunk] of _.entries()) {
                if (writable.writableNeedDrain) {
                  return Effect.flatMap(drainOrError, () => go(_.slice(index)));
                }
                if (options?.encoding) {
                  writable.write(chunk, options.encoding);
                } else {
                  writable.write(chunk);
                }
              }
              return Effect.unit;
            };

            return go(chunks);
          }
        );
    })
  );
