/* eslint-disable @typescript-eslint/no-explicit-any */
import { Chunk, Data, Effect, Either, Option, Scope, Sink } from "effect";
import { Writable } from "stream";
import { listen } from "../util/util";

export class WritableError extends Data.TaggedError("WritableError")<{
  cause: Error;
}> {}

export interface Encode<T> {
  (message: T): Buffer;
}

const onDrain: (writable: Writable) => Effect.Effect<void, never> = (
  writable,
) =>
  listen({
    emitter: writable,
    event: "drain",
    onEvent: () => Effect.unit,
    get: (_) =>
      !_.writableNeedDrain ? Option.some<void>(undefined) : Option.none(),
  });

const onError: (
  writable: Writable,
) => Effect.Effect<never, WritableError> = (writable) =>
  listen({
    emitter: writable,
    event: "error",
    onEvent: (cause: Error) => Effect.fail(new WritableError({ cause })),
    get: (_) => (_.errored ? Option.some(_.errored) : Option.none()),
  });

export type Push<A> = (
  input: Option.Option<Chunk.Chunk<A>>,
) => Effect.Effect<void, WritableError>;

export type SinkablePush<A> = (
  input: Option.Option<Chunk.Chunk<A>>,
) => Effect.Effect<
  void,
  readonly [Either.Either<void, WritableError>, Chunk.Chunk<never>]
>;

export const push: {
  <A = Buffer>(
    writable: Writable,
    options: {
      endOnClose: true;
      encoding?: BufferEncoding;
    },
  ): Effect.Effect<Push<A>, never, Scope.Scope>;
  <A = Buffer>(
    writable: Writable,
    options?: {
      endOnClose?: false | undefined;
      encoding?: BufferEncoding;
    },
  ): Push<A>;
} = (writable, options): any => {
  const drainOrError = Effect.raceAll([onDrain(writable), onError(writable)]);

  const push: Push<any> = (input: Option.Option<Chunk.Chunk<any>>) =>
    Effect.suspend((): Effect.Effect<void, WritableError> => {
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

      const go = (_: readonly any[]): Effect.Effect<void, WritableError> => {
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
        Effect.async<void, never>((cb) => {
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
      }),
    );

export const write: <T>(
  writable: Writable,
  encode: Encode<T>,
) => (message: T) => Effect.Effect<void, WritableError> =
  (writable, encode) => (message) =>
    push(writable)(Option.some(Chunk.of(encode(message))));

export const writeSink: <T>(
  writable: Writable,
  encode: Encode<T>,
) => Sink.Sink<void, T, never, WritableError> = (writable, encode) =>
  Sink.fromPush(Effect.succeed(toSinkable(push(writable)))).pipe(
    Sink.mapInput(encode),
  );
