import { Effect, Stream, flow } from 'effect';
import { pgSSLRequestResponse } from '../pg-protocol/message-parsers';
import { authenticate } from './authenticate';
import * as stream from '../stream';
import { connect, end, clientTlsConnect } from '../socket';
import { PgClientError, PgFailedAuth, write } from './util';
import { query } from './query';
import { queryRaw } from './query-raw';
import { queryStream } from './query-stream';
import { queryStreamRaw } from './query-stream-raw';
import { queryMany } from './query-many';
import { recvlogical } from './recvlogical';

export interface Options {
  host: string;
  port: number;
  database: string;
  username: string;
  password: string;
  useSSL?: boolean;
  replication?: boolean;
}

export type PgClient = Effect.Effect.Success<ReturnType<typeof make>>;

const mapPgClientError = (cause: PgClientError['cause']): PgClientError =>
  new PgClientError({ cause });

export const make = ({ useSSL, ...options }: Options) =>
  Effect.gen(function* (_) {
    let socket = yield* _(connect(options));

    if (useSSL || useSSL === undefined) {
      yield* _(write(socket)({ type: 'SSLRequest', requestCode: 80877103 }));

      const reply = yield* _(
        stream.read(socket, stream.decode(pgSSLRequestResponse))
      );

      if (reply.useSSL) {
        socket = yield* _(clientTlsConnect(socket));
      } else {
        if (useSSL) {
          return yield* _(
            new PgFailedAuth({
              msg: 'Postgres server does not support SSL',
              reply,
            })
          );
        }

        yield* _(Effect.logWarning('Postgres server does not support SSL'));
      }
    }

    yield* _(Effect.addFinalizer(() => end(socket)));

    const info = yield* _(authenticate({ socket, ...options }));

    return {
      query: flow(query(socket), Effect.mapError(mapPgClientError)),
      queryRaw: flow(queryRaw(socket), Effect.mapError(mapPgClientError)),
      queryStream: flow(queryStream(socket), Stream.mapError(mapPgClientError)),
      queryStreamRaw: flow(
        queryStreamRaw(socket),
        Stream.mapError(mapPgClientError)
      ),
      queryMany: flow(queryMany(socket), Effect.mapError(mapPgClientError)),
      recvlogical: flow(
        recvlogical(socket),
        Effect.mapError((e) => {
          if (e._tag === 'XLogProcessorError') {
            return e;
          }
          return mapPgClientError(e);
        })
      ),
      ...info,
    };
  }).pipe(Effect.mapError((cause) => new PgClientError({ cause })));
