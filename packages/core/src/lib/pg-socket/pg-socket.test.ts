import { Chunk, Effect, Stream } from 'effect';
import { connect, PgSocket } from './pg-socket';
import { make as makeServer, Options, PgServerSocket } from './pg-server';
import { identity } from 'fp-ts/lib/function';
import { Query } from '../pg-protocol';

it('should listen and exchange messages', async () => {
  const options: Options = {
    host: 'localhost',
    database: 'testdb',
    username: 'testuser',
    password: 'password1',
  };

  const { listen } = makeServer(options);

  const server = (socket: PgServerSocket) =>
    Effect.gen(function* (_) {
      yield* _(socket.readOrFail<Query>('Query'));

      yield* _(
        socket.write({
          type: 'RowDescription',
          fields: [
            {
              columnId: 1,
              dataTypeId: 1,
              dataTypeModifier: 1,
              dataTypeSize: 1,
              format: 0,
              name: 'column1',
              tableId: 1,
            },
          ],
        })
      );
      yield* _(
        socket.write({
          type: 'DataRow',
          values: ['hello'],
        })
      );
    });

  const client = (socket: PgSocket) =>
    Effect.gen(function* (_) {
      yield* _(socket.write({ type: 'Query', sql: 'select * from greeting' }));

      return yield* _(
        Stream.runCollect(
          Stream.take(
            Stream.fromQueue(socket.readQueue).pipe(Stream.mapEffect(identity)),
            2
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
      ).pipe(Effect.tap(() => Effect.logInfo('serve is absolutely done'))),
      connect({ ...options, host: address.address, port: address.port }).pipe(
        Effect.flatMap(client)
      ),
      {
        concurrent: true,
      }
    )
  );

  const result = await Effect.runPromise(program.pipe(Effect.scoped));

  expect(Chunk.toReadonlyArray(result)).toEqual([
    {
      type: 'RowDescription',
      fields: [
        {
          columnId: 1,
          dataTypeId: 1,
          dataTypeModifier: 1,
          dataTypeSize: 1,
          format: 0,
          name: 'column1',
          tableId: 1,
        },
      ],
    },
    {
      type: 'DataRow',
      values: ['hello'],
    },
  ]);
});
