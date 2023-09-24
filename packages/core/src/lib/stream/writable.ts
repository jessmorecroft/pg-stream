/* eslint-disable @typescript-eslint/no-explicit-any */
import { Chunk, Data, Effect, Either, Option, Scope, Sink } from 'effect';
import { Writable } from 'stream';
import { listen } from '../util/util';

export class WritableError extends Data.TaggedClass('WritableError')<{
  cause: Error;
}> {}

export interface Encode<T> {
  (message: T): Buffer;
}

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

export type Push<A> = (
  input: Option.Option<Chunk.Chunk<A>>
) => Effect.Effect<never, WritableError, void>;

export type SinkablePush<A> = (
  input: Option.Option<Chunk.Chunk<A>>
) => Effect.Effect<
  never,
  readonly [Either.Either<WritableError, void>, Chunk.Chunk<never>],
  void
>;

export const push: {
  <A = Buffer>(
    writable: Writable,
    options: {
      endOnClose: true;
      encoding?: BufferEncoding;
    }
  ): Effect.Effect<Scope.Scope, never, Push<A>>;
  <A = Buffer>(
    writable: Writable,
    options?: {
      endOnClose?: false | undefined;
      encoding?: BufferEncoding;
    }
  ): Push<A>;
} = (writable, options): any => {
  const drainOrError = Effect.raceAll([onDrain(writable), onError(writable)]);

  const push: Push<any> = (input: Option.Option<Chunk.Chunk<any>>) =>
    Effect.suspend((): Effect.Effect<never, WritableError, void> => {
      if (writable.errored) {
        return Effect.fail(new WritableError({ cause: writable.errored }));
      }

      if (Option.isNone(input)) {
        if (writable.writableNeedDrain) {
          return drainOrError;
        }
        return Effect.unit;
      }

      const chunks = Chunk.toReadonlyArray(input.value);

      const go = (
        _: readonly any[]
      ): Effect.Effect<never, WritableError, void> => {
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
    });

  if (!options?.endOnClose) {
    return push;
  }

  return Effect.acquireRelease(Effect.succeed(writable), (writable) => {
    if (!writable.errored && !writable.writableEnded) {
      return Effect.raceAll([
        Effect.async<never, never, void>((cb) => {
          writable.end(() => cb(Effect.unit));
        }),
        onError(writable).pipe(Effect.ignore),
      ]);
    }
    return Effect.unit;
  }).pipe(Effect.map(() => push));
};

export const toSinkable =
  <A>(push: Push<A>): SinkablePush<A> =>
  (input) =>
    push(input).pipe(
      Effect.mapError((e) => [Either.left(e), Chunk.empty<never>()] as const),
      Effect.flatMap((): ReturnType<SinkablePush<A>> => {
        if (Option.isNone(input)) {
          return Effect.fail([Either.right(undefined), Chunk.empty<never>()]);
        }
        return Effect.unit;
      })
    );

export const write: <T>(
  writable: Writable,
  encode: Encode<T>
) => (message: T) => Effect.Effect<never, WritableError, void> =
  (writable, encode) => (message) =>
    push(writable)(Option.some(Chunk.of(encode(message))));

export const writeSink: <T>(
  writable: Writable,
  encode: Encode<T>
) => Sink.Sink<never, WritableError, T, never, void> = (writable, encode) =>
  Sink.fromPush(Effect.succeed(toSinkable(push(writable)))).pipe(
    Sink.contramap(encode)
  );
