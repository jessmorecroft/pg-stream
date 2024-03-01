import { Data, Effect, Option } from 'effect';
import * as net from 'net';
import * as tls from 'tls';
import { listen } from '../util/util';

interface Options {
  host: string;
  port: number;
}

export class SocketError extends Data.TaggedError('SocketError')<{
  cause: Error;
}> {}

const onConnect: (socket: net.Socket) => Effect.Effect<void> = (
  socket
) =>
  listen({
    emitter: socket,
    event: 'connect',
    onEvent: () => Effect.unit,
    get: (_) => (!_.connecting ? Option.some<void>(undefined) : Option.none()),
  });

export const onError: (
  socket: net.Socket
) => Effect.Effect<never, SocketError> = (socket) =>
  listen({
    emitter: socket,
    event: 'error',
    onEvent: (cause: Error) => Effect.fail(new SocketError({ cause })),
    get: (_) => (_.errored ? Option.some(_.errored) : Option.none()),
  });

const onFinish: (socket: net.Socket) => Effect.Effect<void> = (
  socket
) =>
  listen({
    emitter: socket,
    event: 'finish',
    onEvent: () => Effect.unit,
    get: (_) =>
      _.writableFinished ? Option.some<void>(undefined) : Option.none(),
  });

export const onSecureConnect: (
  socket: tls.TLSSocket
) => Effect.Effect<void> = (socket) =>
  listen({
    emitter: socket,
    event: 'secureConnect',
    onEvent: () => Effect.unit,
    get: () => Option.none(),
  });

export const end = (socket: net.Socket) =>
  Effect.async<void>((cb) => {
    if (socket.errored || socket.writableFinished) {
      cb(Effect.unit);
      return;
    }
    if (!socket.writableEnded) {
      socket.end();
    }
    cb(Effect.raceAll([onFinish(socket), onError(socket)]).pipe(Effect.ignore));
  });

export const tlsConnect = (socket: net.Socket) =>
  Effect.suspend(() => {
    const tlsSocket = tls.connect({ socket });
    return Effect.raceAll([
      onSecureConnect(tlsSocket),
      onError(tlsSocket),
    ]).pipe(Effect.map(() => tlsSocket));
  });

export const connect = ({ host, port }: Options) =>
  Effect.suspend(() => {
    const socket = net.connect({ host, port, allowHalfOpen: false });
    return Effect.raceAll([onConnect(socket), onError(socket)]).pipe(
      Effect.map(() => socket)
    );
  });
