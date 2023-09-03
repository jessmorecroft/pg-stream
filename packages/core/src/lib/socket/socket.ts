import { Effect, Option, Ref, Sink, Stream } from 'effect';
import * as net from 'net';
import { makePush } from './make-push';
import { makePull } from './make-pull';
import * as tls from 'tls';
import { listen } from './util';

export type Socket = Effect.Effect.Success<ReturnType<typeof make>>;

export type SSLSocket = Effect.Effect.Success<ReturnType<typeof makeSSLSocket>>;

export type BaseSocket = Effect.Effect.Success<ReturnType<typeof makeSocket>>;

interface Options {
  host: string;
  port: number;
}

const listeners = (socket: net.Socket) => ({
  connect: listen({
    socket,
    event: 'connect',
    onEvent: () => Effect.unit,
    get: () =>
      !socket.connecting ? Option.some<void>(undefined) : Option.none(),
  }),

  close: listen({
    socket,
    event: 'close',
    onEvent: () => Effect.unit,
    get: () => (socket.closed ? Option.some<void>(undefined) : Option.none()),
  }),

  error: listen({
    socket,
    event: 'error',
    onEvent: (error: Error) => Effect.fail(error),
    get: () => (socket.errored ? Option.some(socket.errored) : Option.none()),
  }),
});

const sslListeners = (socket: tls.TLSSocket) => ({
  ...listeners(socket),
  secureConnect: listen({
    socket,
    event: 'secureConnect',
    onEvent: () => Effect.unit,
    get: () =>
      !socket.connecting ? Option.some<void>(undefined) : Option.none(),
  }),
});

const makeSSLSocket = (options: { socket: net.Socket }) =>
  Effect.suspend(() => {
    const { socket } = options;
    const sslSocket = tls.connect({ socket });
    const { error, secureConnect } = sslListeners(sslSocket);
    return Effect.raceAll([secureConnect, error]).pipe(
      Effect.flatMap(() => Ref.make(true)),
      Effect.flatMap((endOnDoneRef) =>
        makeSocket({ socket: sslSocket, endOnDoneRef })
      )
    );
  });

const makeSocket = (options: {
  socket: net.Socket;
  endOnDoneRef: Ref.Ref<boolean>;
}) =>
  Effect.gen(function* (_) {
    const { socket, endOnDoneRef } = options;

    const end = Effect.async<never, never, void>((cb) => {
      if (socket.closed || socket.errored) {
        cb(Effect.unit);
        return;
      }
      socket.end();
      const { close, error } = listeners(socket);
      cb(Effect.raceAll([close, error]).pipe(Effect.ignore));
    });

    yield* _(
      Effect.addFinalizer(() =>
        Effect.flatMap(Ref.get(endOnDoneRef), (endOnDone) => {
          if (!endOnDone || socket.errored || socket.writableEnded) {
            return Effect.unit;
          }
          return end;
        })
      )
    );

    const pull = yield* _(makePull<Buffer>(() => options.socket));
    const push = yield* _(makePush<Buffer>(() => options.socket));

    const pullStream = Stream.fromPull(Effect.succeed(pull));
    const pushSink = Sink.fromPush(Effect.succeed(push));

    return { pull, push, end, pullStream, pushSink };
  });

export const make = (socket: net.Socket) => {
  return Effect.gen(function* (_) {
    const endOnDoneRef = yield* _(Ref.make(true));

    const upgradeToSSL = makeSSLSocket({ socket }).pipe(
      Effect.tap(() => Ref.set(endOnDoneRef, false))
    );

    return yield* _(
      makeSocket({
        socket,
        endOnDoneRef,
      }).pipe(Effect.map((_) => ({ ..._, upgradeToSSL })))
    );
  });
};

export const connect = ({ host, port }: Options) =>
  Effect.async<never, Error, net.Socket>((cb) => {
    const _ = net.connect({ host, port });
    const { connect, error } = listeners(_);
    cb(Effect.as(Effect.raceAll([connect, error]), _));
  }).pipe(Effect.flatMap(make));
