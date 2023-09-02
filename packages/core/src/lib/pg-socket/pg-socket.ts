import {
  Chunk,
  Data,
  Effect,
  Either,
  Option,
  Queue,
  Sink,
  Stream,
  identity,
  pipe,
} from 'effect';
import * as P from 'parser-ts/Parser';
import * as E from 'fp-ts/Either';
import * as B from '../parser/buffer';
import * as socket from '../socket/socket';
import { pgServerMessageParser } from '../pg-protocol/message-parsers';
import { ParseError } from 'parser-ts/lib/ParseResult';
import { makePgClientMessage } from '../pg-protocol';

export interface Options {
  host: string;
  port: number;
}

export type PgSocket = Effect.Effect.Success<ReturnType<typeof connect>>;

export class PgParseError extends Data.TaggedClass('PgParseError')<{
  cause: ParseError<unknown>;
}> {}

export class PgReadEnded extends Data.TaggedClass('PgReadEnded')<
  Record<string, never>
> {}

const decode =
  <T>(parser: P.Parser<number, T>) =>
  (buf: Buffer) => {
    return pipe(
      P.many1(parser)(B.stream(buf)),
      E.fold(
        (cause) => {
          if (cause.fatal) {
            return Effect.fail(new PgParseError({ cause }));
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

export const make = <I, O>(
  { pullStream, pushSink, push, end }: socket.BaseSocket,
  parser: P.Parser<number, I>,
  encoder: (message: O) => Buffer
) =>
  Effect.gen(function* (_) {
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
      Effect.mapError((e) => (Option.isNone(e) ? new PgReadEnded({}) : e.value))
    );

    const write = (message: O) =>
      push(Option.some(Chunk.of(encoder(message)))).pipe(
        Effect.catchAll(([ret]) =>
          Either.isLeft(ret) ? Effect.fail(ret.left) : Effect.unit
        )
      );

    return { readQueue, read, write, writeSink, end };
  });

export const connect = (options: Options) =>
  Effect.flatMap(socket.connect(options), (s) =>
    make(s, pgServerMessageParser, makePgClientMessage)
  );
