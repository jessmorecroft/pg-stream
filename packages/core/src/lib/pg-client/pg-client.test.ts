import { Effect, Stream } from 'effect';
import { make, PgClient } from './pg-client';
import {
  make as makeTestServer,
  readOrFail,
  write,
  Options,
} from './pg-test-server';
import { layer } from '@effect/platform-node/FileSystem';
import * as Schema from '@effect/schema/Schema';
import { PgTypes } from '../pg-protocol';
import { Socket } from 'net';

it.each<Options>([
  {
    database: 'testdb',
    username: 'testuser',
    password: 'password',
  },
  {
    database: 'testdb',
    username: 'testuer',
    password: 'password',
    ssl: {
      certFile: __dirname + '/../socket/resources/server-cert.pem',
      keyFile: __dirname + '/../socket/resources/server-key.pem',
    },
  },
])('should handle an sql request', async (options) => {
  const { listen } = makeTestServer({ ...options, host: 'localhost' });

  const server = (socket: Socket) =>
    Effect.gen(function* (_) {
      yield* _(readOrFail(socket)('Query'));

      yield* _(
        write(socket)({
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
        write(socket)({
          type: 'DataRow',
          values: ['hello', '42', '2022-02-01T10:11:12.002Z', '66', '{1,2,3}'],
        })
      );

      yield* _(
        write(socket)({
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
        write(socket)({
          type: 'DataRow',
          values: ['world', '99', '2023-01-01T10:11:12.001Z', '77', '{4,5,6}'],
        })
      );

      yield* _(
        write(socket)({
          type: 'CommandComplete',
          commandTag: 'done',
        })
      );

      yield* _(
        write(socket)({
          type: 'ReadyForQuery',
          transactionStatus: 'I',
        })
      );
    });

  const handler = (client: PgClient) =>
    Effect.gen(function* (_) {
      return yield* _(
        client.query(
          'select * from greeting',
          Schema.nonEmptyArray(
            Schema.struct({
              column1: Schema.string,
              column2: Schema.number,
              column3: Schema.DateFromSelf,
              column4: Schema.bigintFromSelf,
              column5: Schema.array(Schema.number),
            })
          )
        )
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
      make({
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
    program.pipe(Effect.scoped, Effect.provide(layer))
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
