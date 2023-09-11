import { Chunk, Data, Effect, Option } from 'effect';
import { LazyArg } from 'effect/Function';
import { Readable } from 'stream';
import { listen } from '../util/util';

export type Pull<A> = Effect.Effect.Success<ReturnType<typeof pull<A>>>;

export class ReadableError extends Data.TaggedClass('ReadableError')<{
  cause: Error;
}> {}

const onClose: (readable: Readable) => Effect.Effect<never, never, void> = (
  readable
) =>
  listen({
    emitter: readable,
    event: 'close',
    onEvent: () => Effect.unit,
    get: (_) => (_.closed ? Option.some<void>(undefined) : Option.none()),
  });

const onReady: (readable: Readable) => Effect.Effect<never, never, void> = (
  readable
) =>
  listen({
    emitter: readable,
    event: 'readable',
    onEvent: () => Effect.unit,
    get: (_) =>
      _.readableLength > 0 ? Option.some<void>(undefined) : Option.none(),
  });

const onEnd: (readable: Readable) => Effect.Effect<never, never, void> = (
  readable
) =>
  listen({
    emitter: readable,
    event: 'end',
    onEvent: () => Effect.unit,
    get: (_) =>
      _.readableEnded ? Option.some<void>(undefined) : Option.none(),
  });

const onError: (
  readable: Readable
) => Effect.Effect<never, ReadableError, never> = (readable) =>
  listen({
    emitter: readable,
    event: 'error',
    onEvent: (cause: Error) => Effect.fail(new ReadableError({ cause })),
    get: (_) => (_.errored ? Option.some(_.errored) : Option.none()),
  });

export interface FromReadableOptions {
  readonly waitForClose?: boolean;
}

export const pull = <A = Buffer>(
  evaluate: LazyArg<Readable>,
  options?: FromReadableOptions
) =>
  Effect.acquireRelease(Effect.sync(evaluate), (readable) => {
    if (options?.waitForClose && !readable.closed && !readable.errored) {
      return Effect.raceAll([
        onClose(readable),
        onError(readable).pipe(Effect.ignore),
      ]);
    }
    return Effect.unit;
  }).pipe(
    Effect.map((readable) => {
      return Effect.suspend(() => {
        const go = (): Effect.Effect<
          never,
          Option.Option<ReadableError>,
          Chunk.Chunk<A>
        > => {
          if (readable.errored) {
            return Effect.fail(
              Option.some(new ReadableError({ cause: readable.errored }))
            );
          }

          if (readable.readableEnded) {
            return Effect.fail(Option.none());
          }

          const buf: A | null = readable.read();
          if (buf !== null) {
            return Effect.succeed(Chunk.of(buf));
          }

          return Effect.raceAll([
            onReady(readable),
            onEnd(readable),
            onError(readable),
          ]).pipe(Effect.mapError(Option.some), Effect.flatMap(go));
        };

        return go();
      });
    })
  );
