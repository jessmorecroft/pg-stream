import { Effect, Stream } from 'effect';
import { connect, PgClient } from './pg-socket';
import {
  make as makeTestServer,
  Options,
  PgTestServerSocket,
} from './pg-test-server';
import { layer } from '@effect/platform-node/FileSystem';

it.each<Options>([
  {
    host: 'localhost',
    database: 'testdb',
    username: 'testuser',
    password: 'password',
  },
  {
    host: 'localhost',
    database: 'testdb',
    username: 'testuer',
    password: 'password',
    ssl: {
      certFile: __dirname + '/../socket/resources/server-cert.pem',
      keyFile: __dirname + '/../socket/resources/server-key.pem',
    },
  },
])('should handle an sql request', async (options) => {
  const { listen } = makeTestServer(options);

  const server = (socket: PgTestServerSocket) =>
    Effect.gen(function* (_) {
      yield* _(socket.readOrFail('Query'));

      yield* _(
        socket.write({
          type: 'RowDescription',
          fields: [
            {
              columnId: 1,
              dataTypeId: 1043,
              dataTypeModifier: 1,
              dataTypeSize: 1,
              format: 0,
              name: 'column1',
              tableId: 1,
            },
            {
              columnId: 1,
              dataTypeId: 23,
              dataTypeModifier: 1,
              dataTypeSize: 1,
              format: 0,
              name: 'column2',
              tableId: 1,
            },
          ],
        })
      );
      yield* _(
        socket.write({
          type: 'DataRow',
          values: ['hello', '42'],
        })
      );

      yield* _(
        socket.write({
          type: 'NoticeResponse',
          notices: [
            {
              type: 'V',
              value: 'INFO',
            },
            {
              type: 'M',
              value: 'your test is running!',
            },
          ],
        })
      );

      yield* _(
        socket.write({
          type: 'DataRow',
          values: ['world', '99'],
        })
      );

      yield* _(
        socket.write({
          type: 'ReadyForQuery',
          transactionStatus: 'I',
        })
      );
    });

  const handler = (client: PgClient) =>
    Effect.gen(function* (_) {
      return yield* _(client.executeSql({ sql: 'select * from greeting' }));
    });

  const program = Effect.flatMap(listen, ({ sockets, address }) =>
    Effect.zipRight(
      Stream.runDrain(
        Stream.take(sockets, 1).pipe(
          Stream.mapEffect((socket) =>
            Effect.flatMap(socket, server).pipe(Effect.scoped)
          )
        )
      ).pipe(Effect.tap(() => Effect.logInfo('server is done'))),
      connect({
        ...options,
        host: address.address,
        port: address.port,
        useSSL: !!options.ssl,
      }).pipe(Effect.flatMap(handler)),
      {
        concurrent: true,
      }
    )
  );

  const rows = await Effect.runPromise(
    program.pipe(
      Effect.scoped,
      Effect.provideLayer(layer),
      Effect.tapErrorCause(Effect.logFatal)
    )
  );

  expect(rows).toEqual([
    {
      column1: 'hello',
      column2: 42,
    },
    {
      column1: 'world',
      column2: 99,
    },
  ]);
});
