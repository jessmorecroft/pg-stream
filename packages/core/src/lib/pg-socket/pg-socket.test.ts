import { Chunk, Effect, Stream } from 'effect';
import { connect, PgSocket } from './pg-socket';
import { make as makeServer, PgServerSocket } from './pg-server';
import { identity } from 'fp-ts/lib/function';

it('should listen and exchange messages', async () => {
  const { listen } = makeServer({ host: 'localhost' });

  const server = (socket: PgServerSocket) =>
    Effect.gen(function* (_) {
      //yield* _(Effect.delay(Effect.unit, Duration.seconds(1)));

      console.log('waiting for req');
      const req = yield* _(socket.read);

      if (req.type !== 'PasswordMessage' || req.password !== 'password1') {
        yield* _(Effect.fail(new Error('bad msg')));
      }

      console.log('here2');
      yield* _(
        socket.write({
          type: 'AuthenticationOk',
        })
      );

      console.log('here3');
      const req2 = yield* _(socket.read);
      if (req2.type !== 'Query') {
        yield* _(Effect.fail(new Error('bad msg')));
      }

      console.log('here4');
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
      //yield* _(Effect.delay(Effect.unit, Duration.seconds(1)));

      yield* _(
        socket.write({
          type: 'PasswordMessage',
          password: 'password1',
        })
      );
      console.log('write2');

      const resp = yield* _(socket.read);
      if (resp.type !== 'AuthenticationOk') {
        return Chunk.empty();
      }

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
      connect({ host: address.address, port: address.port }).pipe(
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
