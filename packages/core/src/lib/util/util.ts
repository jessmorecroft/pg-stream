import { Effect, Option } from "effect";
import { EventEmitter } from "stream";

export const listen = <T extends EventEmitter, A, E, B>({
  emitter,
  event,
  onEvent,
  get,
}: {
  emitter: T;
  event: string;
  onEvent: (_: A) => Effect.Effect<B, E>;
  get: (emitter: T) => Option.Option<A>;
}) =>
  Effect.async<B, E>((cb, signal) => {
    const fn = (_: A) => cb(onEvent(_));
    const _ = get(emitter);
    if (Option.isSome(_)) {
      fn(_.value);
      return;
    }
    emitter.once(event, fn);
    signal.onabort = () => {
      emitter.off(event, fn);
    };
  });
