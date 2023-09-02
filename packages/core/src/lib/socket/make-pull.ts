import { Chunk, Data, Effect, Option } from 'effect';
import { LazyArg } from 'effect/Function';
import { Readable } from 'stream';

export type Pull<A> = Effect.Effect.Success<ReturnType<typeof makePull<A>>>;

export class ReadableError extends Data.TaggedClass('ReadableError')<{
  cause: Error;
}> {}

const listeners = (readable: Readable) => ({
  closed: Effect.asyncInterrupt<never, never, void>((cb, signal) => {
    const fn = () => cb(Effect.unit);
    if (readable.closed) {
      fn();
      return;
    }
    readable.once('close', fn);
    signal.onabort = () => {
      readable.removeListener('close', fn);
    };
  }),
  ready: Effect.asyncInterrupt<never, never, void>((cb, signal) => {
    const fn = () => cb(Effect.unit);
    if (readable.readableLength > 0) {
      fn();
      return;
    }
    readable.once('readable', fn);
    signal.onabort = () => {
      readable.removeListener('readable', fn);
    };
  }),
  end: Effect.asyncInterrupt<never, never, void>((cb, signal) => {
    const fn = () => cb(Effect.unit);
    if (readable.readableEnded) {
      fn();
      return;
    }
    readable.once('end', fn);
    signal.onabort = () => {
      readable.removeListener('end', fn);
    };
  }),
  error: Effect.asyncInterrupt<never, ReadableError, never>((cb, signal) => {
    const fn = (cause: Error) => cb(Effect.fail(new ReadableError({ cause })));
    if (readable.errored) {
      fn(readable.errored);
      return;
    }
    readable.once('error', fn);
    signal.onabort = () => {
      readable.removeListener('error', fn);
    };
  }),
});

export interface FromReadableOptions {
  readonly waitForClose?: boolean;
}

export const makePull = <A = Buffer>(
  evaluate: LazyArg<Readable>,
  options?: FromReadableOptions
) =>
  Effect.acquireRelease(Effect.sync(evaluate), (readable) => {
    if (options?.waitForClose && !readable.closed && !readable.errored) {
      const { error, closed } = listeners(readable);
      return Effect.raceAll([closed, error.pipe(Effect.ignore)]);
    }
    return Effect.unit;
  }).pipe(
    Effect.map((readable) => {
      const { error, ready, end } = listeners(readable);
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

          return Effect.raceAll([ready, end, error]).pipe(
            Effect.mapError(Option.some),
            Effect.flatMap(go)
          );
        };

        return go();
      });
    })
  );
