import { Effect, Stream } from 'effect';
import { connect, PgClient } from './pg-socket';
import {
  make as makeTestServer,
  Options,
  PgTestServerSocket,
} from './pg-test-server';
import { layer } from '@effect/platform-node/FileSystem';
import * as Schema from '@effect/schema/Schema';
import { PgTypes } from '../pg-protocol';

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
              dataTypeId: PgTypes['varchar'].baseTypeOid,
              dataTypeModifier: 1,
              dataTypeSize: 1,
              format: 0,
              name: 'column1',
              tableId: 1,
            },
            {
              columnId: 1,
              dataTypeId: PgTypes['int4'].baseTypeOid,
              dataTypeModifier: 1,
              dataTypeSize: 1,
              format: 0,
              name: 'column2',
              tableId: 1,
            },
            {
              columnId: 1,
              dataTypeId: PgTypes['timestamp'].baseTypeOid,
              dataTypeModifier: 1,
              dataTypeSize: 1,
              format: 0,
              name: 'column3',
              tableId: 1,
            },
            {
              columnId: 1,
              dataTypeId: PgTypes['int8'].baseTypeOid,
              dataTypeModifier: 1,
              dataTypeSize: 1,
              format: 0,
              name: 'column4',
              tableId: 1,
            },
            {
              columnId: 1,
              dataTypeId: PgTypes['int4'].arrayTypeOid,
              dataTypeModifier: 1,
              dataTypeSize: 1,
              format: 0,
              name: 'column5',
              tableId: 1,
            },
          ],
        })
      );
      yield* _(
        socket.write({
          type: 'DataRow',
          values: ['hello', '42', '2022-02-01T10:11:12.002Z', '66', '{1,2,3}'],
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
          values: ['world', '99', '2023-01-01T10:11:12.001Z', '77', '{4,5,6}'],
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
      return yield* _(
        client.executeSql({
          sql: 'select * from greeting',
          schema: Schema.struct({
            column1: Schema.string,
            column2: Schema.number,
            column3: Schema.DateFromSelf,
            column4: Schema.bigint,
            column5: Schema.array(Schema.number),
          }),
        })
      );
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
    program.pipe(Effect.scoped, Effect.provideLayer(layer))
  );

  expect(rows).toEqual([
    {
      column1: 'hello',
      column2: 42,
      column3: new Date(Date.UTC(2022, 1, 1, 10, 11, 12, 2)),
      column4: 66n,
      column5: [1, 2, 3],
    },
    {
      column1: 'world',
      column2: 99,
      column3: new Date(Date.UTC(2023, 0, 1, 10, 11, 12, 1)),
      column4: 77n,
      column5: [4, 5, 6],
    },
  ]);
});
