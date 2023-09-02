import * as pgSocket from './pg-socket';
import * as socket from '../socket/socket';
import * as serverSocket from '../socket/server';
import { makePgServerMessage, pgClientMessageParser } from '../pg-protocol';
import { Effect, Stream } from 'effect';

export type PgServerSocket = Effect.Effect.Success<
  ReturnType<typeof makeSocket>
>;

export const makeSocket = (socket: socket.Socket) =>
  pgSocket.make(socket, pgClientMessageParser, makePgServerMessage);

export const make = (options: Partial<pgSocket.Options>) => {
  const ss = serverSocket.make(options);

  const listen = Effect.map(ss.listen, ({ sockets, address }) => ({
    sockets: Stream.mapEffect(sockets, (input) =>
      Effect.map(input, makeSocket)
    ),
    address,
  }));

  return { listen };
};
