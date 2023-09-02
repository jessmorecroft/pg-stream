import { Effect, Option } from 'effect';
import * as net from 'net';

export const listen = <T extends net.Socket, A, E, B>({
  socket,
  event,
  onEvent,
  get,
}: {
  socket: T;
  event: string;
  onEvent: (_: A) => Effect.Effect<never, E, B>;
  get: () => Option.Option<A>;
}) =>
  Effect.asyncInterrupt<never, E, B>((cb, signal) => {
    const fn = (_: A) => cb(onEvent(_));
    const _ = get();
    if (Option.isSome(_)) {
      fn(_.value);
      return;
    }
    socket.once(event, fn);
    signal.onabort = () => {
      socket.off(event, fn);
    };
  });
