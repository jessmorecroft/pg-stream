/* eslint-disable @typescript-eslint/no-explicit-any */
import { Effect } from 'effect';
import { MakeValueTypeParserOptions } from '../pg-protocol';
import {
  NoMoreMessagesError,
  ParseMessageError,
  ParseMessageGroupError,
  ReadableError,
  WritableError,
} from '../stream';
import { Duplex } from 'stream';
import { PgParseError, PgServerError, SchemaTypes } from './util';
import * as Schema from '@effect/schema/Schema';
import { queryRaw } from './query-raw';
import { TreeFormatter } from '@effect/schema';

type NoneOneOrMany<T extends [...any]> = T extends [infer A]
  ? A
  : T extends []
  ? void
  : T;

export const query =
  (socket: Duplex) =>
  <S extends [...Schema.Schema<any>[]]>(
    sqlOrOptions:
      | string
      | { sql: string; parserOptions: MakeValueTypeParserOptions },
    ...schemas: S
  ): Effect.Effect<
    NoneOneOrMany<SchemaTypes<S>>,
    | ReadableError
    | WritableError
    | NoMoreMessagesError
    | ParseMessageError
    | ParseMessageGroupError
    | PgServerError
    | PgParseError
  > => {
    const { sql, parserOptions } =
      typeof sqlOrOptions === 'string'
        ? { sql: sqlOrOptions, parserOptions: undefined }
        : sqlOrOptions;

    return queryRaw(socket)(sql, parserOptions).pipe(
      Effect.flatMap((rows) =>
        Schema.decodeUnknown(Schema.tuple(...schemas))(rows).pipe(
          Effect.mapError(
            (pe) =>
              new PgParseError({
                message: TreeFormatter.formatError(pe),
              })
          ),
          Effect.tapError((pe) => Effect.logError(`\n${pe.message}`))
        )
      ),
      Effect.map((parsed) => {
        if (parsed.length === 0) {
          return undefined;
        }
        if (parsed.length === 1) {
          return parsed[0];
        }
        return parsed;
      })
    );
  };
