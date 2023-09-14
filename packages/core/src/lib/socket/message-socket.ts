import {
  Chunk,
  Data,
  Effect,
  Either,
  Option,
  Predicate,
  Queue,
  Ref,
  Sink,
  Stream,
  identity,
  pipe,
} from 'effect';
import * as P from 'parser-ts/Parser';
import * as S from 'parser-ts/Stream';
import * as E from 'fp-ts/Either';
import * as B from '../parser/buffer';
import { BaseSocket } from './socket';
import { ParseError } from 'parser-ts/lib/ParseResult';

export class ParseMessageError extends Data.TaggedClass('ParseMessageError')<{
  cause: ParseError<number>;
}> {}

export class ParseMessageGroupError extends Data.TaggedClass(
  'ParseMessageGroupError'
)<{
  cause: ParseError<unknown>;
}> {}

export class NoMoreMessagesError extends Data.TaggedClass(
  'NoMoreMessagesError'
)<Record<string, never>> {}

export class UnexpectedMessageError extends Data.TaggedClass(
  'UnexpectedMessageError'
)<{
  unexpected: unknown;
  msg?: string;
}> {}

export const hasTypeOf =
  <K extends string>(type: K, ...types: K[]) =>
  <T extends { type: string }>(msg: T): msg is T & { type: K } =>
    msg.type === type || !!types.find((_) => _ === msg.type);

const decode =
  <T>(parser: P.Parser<number, T>) =>
  (buf: Buffer) => {
    return pipe(
      P.many1(parser)(B.stream(buf)),
      E.fold(
        (cause) => {
          if (cause.fatal) {
            return Effect.fail(new ParseMessageError({ cause }));
          }
          return Effect.succeed([Chunk.empty<T>(), buf] as const);
        },
        (result) => {
          const { cursor } = result.next;
          const unusedBuf = buf.subarray(cursor);
          return Effect.succeed([
            Chunk.fromIterable(result.value),
            unusedBuf,
          ] as const);
        }
      ),
      Effect.unified
    );
  };

export const make = <I extends { type: string }, O>({
  socket,
  parser,
  encoder,
}: {
  socket: BaseSocket;
  parser: P.Parser<number, I>;
  encoder: (message: O) => Buffer;
}) =>
  Effect.gen(function* (_) {
    const { end, pullStream, push, pushSink } = socket;

    const readStream = pullStream.pipe(
      Stream.scanEffect(
        [Chunk.empty(), Buffer.from([])],
        (s: readonly [Chunk.Chunk<I>, Buffer], a: Buffer) =>
          decode(parser)(Buffer.concat([s[1], a]))
      ),
      Stream.flatMap(([chunk]) => Stream.fromChunks(chunk))
    );

    const readQueue = yield* _(
      Stream.toQueueOfElements(readStream, {
        capacity: 5,
      })
    );

    const writeSink = pushSink.pipe(Sink.contramap(encoder));

    const read = Effect.flatMap(Queue.take(readQueue), identity).pipe(
      Effect.mapError((e) =>
        Option.isNone(e) ? new NoMoreMessagesError({}) : e.value
      )
    );

    const readOrFail = <K extends I['type'], R, E>({
      reader = read,
      types,
    }: {
      reader: Effect.Effect<R, E | Effect.Effect.Error<typeof read>, I>;
      types: [K, ...K[]];
    }) =>
      Effect.filterOrFail(
        reader,
        hasTypeOf<K>(...types),
        (unexpected) =>
          new UnexpectedMessageError({
            unexpected,
            msg: `expected one of ${types}`,
          })
      );

    const readMany = <R, E, A>({
      reader = read,
      parser,
      isLast,
    }: {
      reader: Effect.Effect<R, E | Effect.Effect.Error<typeof read>, I>;
      parser: P.Parser<I, A>;
      isLast: Predicate.Predicate<I>;
    }) =>
      Effect.gen(function* (_) {
        const doneRef = yield* _(Ref.make(false));
        const msgs = yield* _(
          Stream.runCollect(
            Stream.repeatEffectOption(
              Effect.flatMap(Ref.get(doneRef), (done) =>
                done
                  ? Effect.fail(Option.none())
                  : (reader ?? read).pipe(
                      Effect.mapError(Option.some),
                      Effect.flatMap((msg) =>
                        isLast(msg)
                          ? Effect.as(Ref.set(doneRef, true), msg)
                          : Effect.succeed(msg)
                      )
                    )
              )
            )
          )
        );

        return yield* _(
          pipe(
            parser(S.stream(Chunk.toReadonlyArray(msgs) as I[])),
            E.fold(
              (cause): Effect.Effect<never, ParseMessageGroupError, A> =>
                Effect.fail(new ParseMessageGroupError({ cause })),
              ({ value }) => Effect.succeed(value)
            )
          )
        );
      });

    const write = (message: O) =>
      push(Option.some(Chunk.of(encoder(message)))).pipe(
        Effect.catchAll(([ret]) =>
          Either.isLeft(ret) ? Effect.fail(ret.left) : Effect.unit
        )
      );

    return {
      readQueue,
      read,
      readOrFail,
      readMany,
      write,
      writeSink,
      end,
    };
  });
