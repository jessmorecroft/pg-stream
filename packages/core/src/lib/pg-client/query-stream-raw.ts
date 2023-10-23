import { Effect, Stream } from 'effect';
import { DataRow } from '../pg-protocol/message-parsers';
import { MakeValueTypeParserOptions, ValueType } from '../pg-protocol';
import * as stream from '../stream';
import { Duplex } from 'stream';
import {
  read,
  write,
  PgParseError,
  transformRowDescription,
  transformDataRow,
  PgServerError,
} from './util';
import {
  hasTypeOf,
  ReadableError,
  ParseMessageError,
  NoMoreMessagesError,
} from '../stream/readable';
import { WritableError } from '../stream/writable';

export const queryStreamRaw =
  (socket: Duplex) =>
  <O extends MakeValueTypeParserOptions>(
    sql: string,
    parserOptions?: O
  ): Stream.Stream<
    never,
    | ReadableError
    | WritableError
    | ParseMessageError
    | NoMoreMessagesError
    | PgServerError
    | PgParseError,
    [Record<string, ValueType<O>>, number]
  > => {
    type State = {
      decode?: (row: DataRow) => Record<string, ValueType<O>>;
      offset: number;
    };
    return Stream.fromEffect(write(socket)({ type: 'Query', sql })).pipe(
      Stream.flatMap(() =>
        stream
          .readStream(read(socket), () => false)
          .pipe(
            Stream.takeUntil(hasTypeOf('ReadyForQuery')),
            Stream.tap((_) =>
              _.type === 'CommandComplete'
                ? Effect.log(_.commandTag)
                : Effect.unit
            ),
            Stream.filter(hasTypeOf('RowDescription', 'DataRow')),
            Stream.zipWithIndex,
            Stream.mapAccumEffect(
              { offset: 0 } as State,
              (
                state,
                [msg, index]
              ): Effect.Effect<
                never,
                PgParseError,
                readonly [
                  State,
                  readonly [Record<string, ValueType<O>>, number] | undefined
                ]
              > => {
                if (msg.type === 'DataRow') {
                  if (!state.decode) {
                    return Effect.fail(
                      new PgParseError({
                        message: 'data row before row description',
                      })
                    );
                  }

                  return Effect.succeed([
                    state,
                    [state.decode(msg), index - state.offset] as const,
                  ]);
                }

                const rowParsers = transformRowDescription(msg, parserOptions);

                const decode = (dataRow: DataRow) =>
                  transformDataRow({ rowParsers, dataRow });

                return Effect.succeed([
                  { decode, offset: index + 1 },
                  undefined,
                ]);
              }
            ),
            Stream.filter(
              (_): _ is [Record<string, ValueType<O>>, number] => !!_
            )
          )
      )
    );
  };
