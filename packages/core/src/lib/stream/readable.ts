/* eslint-disable @typescript-eslint/no-explicit-any */
import {
  Chunk,
  Data,
  Effect,
  Option,
  Predicate,
  Ref,
  Scope,
  Stream,
  pipe,
} from "effect";
import { Readable } from "stream";
import { listen } from "../util/util";
import { ParseError, ParseSuccess } from "parser-ts/lib/ParseResult";
import * as P from "parser-ts/Parser";
import * as E from "fp-ts/Either";
import * as B from "../parser/buffer";
import * as S from "parser-ts/Stream";

export class ReadableError extends Data.TaggedError("ReadableError")<{
  cause: Error;
}> {}

export class ParseMessageError extends Data.TaggedError("ParseMessageError")<{
  cause: ParseError<number>;
}> {}

export class ParseMessageGroupError extends Data.TaggedError(
  "ParseMessageGroupError",
)<{
  cause: ParseError<unknown>;
}> {
  override toString() {
    return JSON.stringify(this, (key, value) =>
      typeof value === "bigint" ? value.toString() : value,
    );
  }
}

export class NoMoreMessagesError extends Data.TaggedError(
  "NoMoreMessagesError",
)<Record<string, never>> {}

export class UnexpectedMessageError extends Data.TaggedError(
  "UnexpectedMessageError",
)<{
  unexpected: unknown;
  msg?: string;
}> {}

export const hasTypeOf =
  <K extends string>(type: K, ...types: K[]) =>
  <T extends { type: string }>(msg: T): msg is T & { type: K } =>
    msg.type === type || !!types.find((_) => _ === msg.type);

const onClose: (readable: Readable) => Effect.Effect<void> = (readable) =>
  listen({
    emitter: readable,
    event: "close",
    onEvent: () => Effect.unit,
    get: (_) => (_.closed ? Option.some<void>(undefined) : Option.none()),
  });

const onReady: (readable: Readable) => Effect.Effect<void> = (readable) =>
  listen({
    emitter: readable,
    event: "readable",
    onEvent: () => Effect.unit,
    get: (_) =>
      _.readableLength > 0 ? Option.some<void>(undefined) : Option.none(),
  });

const onEnd: (readable: Readable) => Effect.Effect<void> = (readable) =>
  listen({
    emitter: readable,
    event: "end",
    onEvent: () => Effect.unit,
    get: (_) =>
      _.readableEnded ? Option.some<void>(undefined) : Option.none(),
  });

const onError: (
  readable: Readable,
) => Effect.Effect<never, ReadableError, never> = (readable) =>
  listen({
    emitter: readable,
    event: "error",
    onEvent: (cause: Error) => Effect.fail(new ReadableError({ cause })),
    get: (_) => (_.errored ? Option.some(_.errored) : Option.none()),
  });

export type Pull<A> = Effect.Effect<A, Option.Option<ReadableError>>;

export type StreamablePull<A> = Effect.Effect<
  Chunk.Chunk<A>,
  Option.Option<ReadableError>
>;

export interface Decode<T> {
  (
    buf: Buffer,
  ): Effect.Effect<
    readonly [T | undefined, Buffer | undefined],
    ParseMessageError
  >;
}

export const decode =
  <T>(parser: P.Parser<number, T>): Decode<T> =>
  (buf: Buffer) => {
    return pipe(
      parser(B.stream(buf)),
      E.fold(
        (cause): ReturnType<Decode<T>> => {
          if (cause.fatal) {
            return Effect.fail(new ParseMessageError({ cause }));
          }
          return Effect.succeed([undefined, buf]);
        },
        (result): ReturnType<Decode<T>> => {
          const { cursor } = result.next;
          const leftovers =
            cursor < buf.length ? buf.subarray(cursor) : undefined;
          return Effect.succeed([result.value, leftovers]);
        },
      ),
    );
  };

export const pull: {
  <A = Buffer>(
    readable: Readable,
    options: {
      waitForClose: true;
    },
  ): Effect.Effect<Pull<A>, never, Scope.Scope>;
  <A = Buffer>(
    readable: Readable,
    options?: {
      waitForClose: false | undefined;
    },
  ): Pull<A>;
} = (readable, options): any => {
  const read: Pull<any> = Effect.suspend(() => {
    const go = (): Effect.Effect<
      Chunk.Chunk<any>,
      Option.Option<ReadableError>
    > => {
      if (readable.errored) {
        return Effect.fail(
          Option.some(new ReadableError({ cause: readable.errored })),
        );
      }

      if (readable.readableEnded) {
        return Effect.fail(Option.none());
      }

      const buf = readable.read();
      if (buf !== null) {
        return Effect.succeed(buf);
      }

      return Effect.raceAll([
        onReady(readable),
        onEnd(readable),
        onError(readable),
      ]).pipe(Effect.mapError(Option.some), Effect.flatMap(go));
    };

    return go();
  });

  if (!options?.waitForClose) {
    return read;
  }

  return Effect.acquireRelease(Effect.succeed(readable), () => {
    if (!readable.closed && !readable.errored) {
      return Effect.raceAll([
        onClose(readable),
        onError(readable).pipe(Effect.ignore),
      ]);
    }
    return Effect.unit;
  }).pipe(Effect.map(() => read));
};

export const toStreamable = <A>(pull: Pull<A>): StreamablePull<A> =>
  Effect.map(pull, Chunk.of);

export const read: <T>(
  readable: Readable,
  decode: Decode<T>,
) => Effect.Effect<
  T,
  ReadableError | ParseMessageError | NoMoreMessagesError
> = (readable, decode) => {
  const readPrepend = <T>(
    decode: Decode<T>,
    prepend?: Buffer,
  ): Effect.Effect<
    T,
    ReadableError | ParseMessageError | NoMoreMessagesError
  > =>
    pull(readable).pipe(
      Effect.mapError((e) =>
        Option.isSome(e) ? e.value : new NoMoreMessagesError({}),
      ),
      Effect.flatMap((_) => {
        const buf = prepend ? Buffer.concat([prepend, _]) : _;
        return decode(buf).pipe(
          Effect.flatMap(([msg, leftovers]) => {
            if (msg) {
              if (leftovers) {
                readable.unshift(leftovers);
              }
              return Effect.succeed(msg);
            }
            return readPrepend(decode, leftovers);
          }),
        );
      }),
    );

  return readPrepend(decode);
};

export const readStream = <E, T>(
  read: Effect.Effect<T, E>,
  isDone: (e: E) => boolean,
): Stream.Stream<T, E> =>
  Stream.fromPull(
    Effect.succeed(
      read.pipe(
        Effect.map(Chunk.of),
        Effect.mapError((e) => (isDone(e) ? Option.none() : Option.some(e))),
      ),
    ),
  );

export const readOrFail: <E, T extends { type: string }>(
  read: Effect.Effect<T, E>,
) => <K extends T["type"]>(
  ...types: [K, ...K[]]
) => Effect.Effect<T & { type: K }, E | UnexpectedMessageError> =
  (read) =>
  (...types) =>
    Effect.filterOrFail(
      read,
      hasTypeOf(...types),
      (unexpected) =>
        new UnexpectedMessageError({
          unexpected,
          msg: `expected one of ${types}`,
        }),
    );

export const readMany =
  <E, T>(read: Effect.Effect<T, E>) =>
  <A>(
    parser: P.Parser<T, A>,
    isLast: Predicate.Predicate<T>,
  ): Effect.Effect<A, E | ParseMessageGroupError> => {
    return Effect.gen(function* (_) {
      const doneRef = yield* _(Ref.make(false));

      const msgs = yield* _(
        Stream.runCollect(
          Stream.repeatEffectOption(
            Effect.flatMap(Ref.get(doneRef), (done) =>
              done
                ? Effect.fail(Option.none())
                : read.pipe(
                    Effect.mapError(Option.some),
                    Effect.flatMap((msg) =>
                      isLast(msg)
                        ? Effect.as(Ref.set(doneRef, true), msg)
                        : Effect.succeed(msg),
                    ),
                  ),
            ),
          ),
        ),
      );

      return yield* _(
        pipe(
          parser(S.stream(Chunk.toReadonlyArray(msgs) as T[])),
          E.fold<
            ParseError<T>,
            ParseSuccess<T, A>,
            Effect.Effect<A, ParseMessageGroupError>
          >(
            (cause) => Effect.fail(new ParseMessageGroupError({ cause })),
            ({ value }) => Effect.succeed(value),
          ),
        ),
      );
    });
  };
