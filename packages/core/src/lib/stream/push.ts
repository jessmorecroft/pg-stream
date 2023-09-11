import { Chunk, Data, Effect, Either, Option } from 'effect';
import { LazyArg } from 'effect/Function';
import { Writable } from 'stream';
import { listen } from '../util/util';

export class WritableError extends Data.TaggedClass('WritableError')<{
  cause: Error;
}> {}

const onDrain: (writable: Writable) => Effect.Effect<never, never, void> = (
  writable
) =>
  listen({
    emitter: writable,
    event: 'drain',
    onEvent: () => Effect.unit,
    get: (_) =>
      !_.writableNeedDrain ? Option.some<void>(undefined) : Option.none(),
  });

const onError: (
  writable: Writable
) => Effect.Effect<never, WritableError, never> = (writable) =>
  listen({
    emitter: writable,
    event: 'error',
    onEvent: (cause: Error) => Effect.fail(new WritableError({ cause })),
    get: (_) => (_.errored ? Option.some(_.errored) : Option.none()),
  });

export interface FromWritableOptions {
  readonly endOnClose?: boolean;
  readonly encoding?: BufferEncoding;
}

export const push = <A>(
  evaluate: LazyArg<Writable>,
  options?: FromWritableOptions
) =>
  Effect.acquireRelease(Effect.sync(evaluate), (writable) => {
    if (writable.errored) {
      return Effect.unit;
    }
    if (options?.endOnClose && !writable.writableEnded) {
      return Effect.raceAll([
        Effect.async<never, never, void>((cb) => {
          writable.end(() => cb(Effect.unit));
        }),
        onError(writable).pipe(Effect.ignore),
      ]);
    }
    return Effect.unit;
  }).pipe(
    Effect.map((writable) => {
      const drainOrError = Effect.raceAll([
        onDrain(writable),
        onError(writable),
      ]).pipe(Effect.mapError((e) => [Either.left(e), Chunk.empty()] as const));
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
