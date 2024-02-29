import { Effect, pipe } from 'effect';
import * as P from 'parser-ts/Parser';
import * as O from 'fp-ts/Option';
import { MakeValueTypeParserOptions, ValueType } from '../pg-protocol';
import {
  NoMoreMessagesError,
  ParseMessageError,
  ParseMessageGroupError,
  ReadableError,
  WritableError,
} from '../stream';
import { Duplex } from 'stream';
import {
  isCommandComplete,
  isDataRow,
  isReadyForQuery,
  isRowDescription,
  item,
  readUntilReady,
  transformDataRow,
  transformRowDescription,
  write,
  PgServerError,
} from './util';

export const queryRaw =
  (socket: Duplex) =>
  <O extends MakeValueTypeParserOptions>(
    sql: string,
    parserOptions?: O
  ): Effect.Effect<Record<string, ValueType<O>>[][], | ReadableError
  | WritableError
  | NoMoreMessagesError
  | ParseMessageError
  | ParseMessageGroupError
  | PgServerError> =>
    Effect.gen(function* (_) {
      yield* _(write(socket)({ type: 'Query', sql }));

      const results = yield* _(
        readUntilReady(socket)(
          pipe(
            item,
            P.filter(isRowDescription),
            P.map((rowDescription) =>
              transformRowDescription(rowDescription, parserOptions)
            ),
            P.chain((rowParsers) =>
              pipe(
                item,
                P.filter(isDataRow),
                P.map((dataRow) => transformDataRow({ rowParsers, dataRow })),
                P.many
              )
            ),
            P.optional,
            P.bindTo('rows'),
            P.bind('commandTag', () =>
              pipe(
                item,
                P.filter(isCommandComplete),
                P.map(({ commandTag }) => commandTag)
              )
            ),
            P.many,
            P.chainFirst(() =>
              pipe(
                item,
                P.filter(isReadyForQuery),
                P.chain(() => P.eof())
              )
            )
          )
        )
      );

      yield* _(
        Effect.forEach(results, ({ commandTag }) => Effect.log(commandTag))
      );

      return results
        .map(({ rows }) => rows)
        .filter(O.isSome)
        .map((rows) => rows.value);
    });
