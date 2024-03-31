import { Chunk, Effect, Sink, Stream } from "effect";
import {
  SSLOptions,
  make as makeServer,
  tlsConnect as serverTlsConnect,
} from "./server";
import { pull, toStreamable, push, toSinkable } from "../stream";
import {
  connect,
  tlsConnect as clientTlsConnect,
  end,
  SocketError,
} from "./socket";
import * as net from "net";
import { layer } from "@effect/platform-node/NodeFileSystem";

const echo = (socket: net.Socket) => {
  return Stream.runDrain(
    Stream.run(
      Stream.fromPull(Effect.succeed(toStreamable(pull(socket)))),
      Sink.fromPush(Effect.succeed(toSinkable(push(socket)))),
    ),
  );
};

it.each<SSLOptions | undefined>([
  undefined,
  {
    certFile: __dirname + "/resources/server-cert.pem",
    keyFile: __dirname + "/resources/server-key.pem",
  },
])("should listen", async (sslOptions) => {
  const { listen } = makeServer({ host: "localhost" });

  const program = Effect.flatMap(listen, ({ sockets, address }) => {
    const server = Stream.runDrain(
      Stream.take(sockets, 1).pipe(
        Stream.mapEffect((socket) => {
          if (sslOptions) {
            return serverTlsConnect(socket, sslOptions).pipe(
              Effect.flatMap(echo),
            );
          }
          return echo(socket);
        }),
      ),
    ).pipe(Effect.tap(() => Effect.logInfo("non-SSL echo is done")));

    const client = connect({ host: address.address, port: address.port }).pipe(
      Effect.flatMap(
        (socket): Effect.Effect<net.Socket, SocketError> =>
          !sslOptions ? Effect.succeed(socket) : clientTlsConnect(socket),
      ),
      Effect.flatMap((socket) => {
        return Effect.zipRight(
          Stream.runDrain(
            Stream.run(
              Stream.fromIterable(
                "the quick brown fox jumped over the lazy dog!".split(" "),
              ).pipe(Stream.map(Buffer.from)),
              Sink.fromPush(Effect.succeed(toSinkable(push(socket)))),
            ),
          ).pipe(Effect.tap(() => end(socket))),
          Stream.runCollect(
            Stream.fromPull(Effect.succeed(toStreamable(pull(socket)))).pipe(
              Stream.map((_) => _.toString()),
            ),
          ).pipe(Effect.map((_) => Chunk.toReadonlyArray(_).join(""))),
          {
            concurrent: true,
          },
        );
      }),
    );

    return Effect.zipRight(server, client, {
      concurrent: true,
    });
  });

  const result = await Effect.runPromise(
    program.pipe(Effect.scoped, Effect.provide(layer)),
  );

  expect(result).toEqual("thequickbrownfoxjumpedoverthelazydog!");
});
