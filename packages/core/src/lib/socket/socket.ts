import { Data, Effect, Option, Sink, Stream, Ref } from 'effect';
import * as net from 'net';
import { push as streamPush } from '../stream/push';
import { pull as streamPull } from '../stream/pull';
import * as tls from 'tls';
import { listen } from '../util/util';

export type Socket = Effect.Effect.Success<ReturnType<typeof make>>;

export type SSLSocket = Effect.Effect.Success<ReturnType<typeof tlsConnect>>;

export type BaseSocket = Effect.Effect.Success<
  ReturnType<typeof makeBaseSocket>
>;

interface Options {
  host: string;
  port: number;
}

export class SocketError extends Data.TaggedClass('SocketError')<{
  cause: Error;
}> {}

const onConnect: (socket: net.Socket) => Effect.Effect<never, never, void> = (
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
) => Effect.Effect<never, SocketError, never> = (socket) =>
  listen({
    emitter: socket,
    event: 'error',
    onEvent: (cause: Error) => Effect.fail(new SocketError({ cause })),
    get: (_) => (_.errored ? Option.some(_.errored) : Option.none()),
  });

const onFinish: (socket: net.Socket) => Effect.Effect<never, never, void> = (
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
) => Effect.Effect<never, never, void> = (socket) =>
  listen({
    emitter: socket,
    event: 'secureConnect',
    onEvent: () => Effect.unit,
    get: () => Option.none(),
  });

export const makeBaseSocket = (socket: net.Socket) =>
  Effect.gen(function* (_) {
    const end = Effect.async<never, never, void>((cb) => {
      if (socket.errored || socket.writableFinished) {
        cb(Effect.unit);
        return;
      }
      if (!socket.writableEnded) {
        socket.end();
      }
      cb(
        Effect.raceAll([onFinish(socket), onError(socket)]).pipe(Effect.ignore)
      );
    });

    const pull = yield* _(streamPull<Buffer>(() => socket));
    const push = yield* _(streamPush<Buffer>(() => socket));

    const pullStream = Stream.fromPull(Effect.succeed(pull));
    const pushSink = Sink.fromPush(Effect.succeed(push));

    return { pull, push, end, pullStream, pushSink };
  });

const tlsConnect = (socket: net.Socket) =>
  Effect.acquireRelease(
    Effect.suspend(() => {
      const tlsSocket = tls.connect({ socket });
      return Effect.raceAll([
        onSecureConnect(tlsSocket),
        onError(tlsSocket),
      ]).pipe(Effect.flatMap(() => makeBaseSocket(tlsSocket)));
    }),
    (socket) => socket.end
  );

export const make = ({ host, port }: Options) =>
  Effect.acquireRelease(
    Effect.suspend(() => {
      const socket = net.connect({ host, port, allowHalfOpen: false });
      return Effect.raceAll([onConnect(socket), onError(socket)]).pipe(
        Effect.flatMap(() => makeBaseSocket(socket)),
        Effect.flatMap((baseSocket) =>
          Effect.gen(function* (_) {
            const upgraded = yield* _(Ref.make(false));

            const upgradeToSSL = Effect.zipLeft(
              tlsConnect(socket),
              Ref.set(upgraded, true)
            );

            return {
              ...baseSocket,
              upgradeToSSL,
              upgraded,
            };
          })
        )
      );
    }),
    ({ upgraded, end }) =>
      Effect.if(Ref.get(upgraded), {
        onTrue: Effect.unit,
        onFalse: end,
      })
  );
