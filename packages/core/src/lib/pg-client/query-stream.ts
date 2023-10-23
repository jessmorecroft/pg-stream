/* eslint-disable @typescript-eslint/no-explicit-any */
import { Effect, Stream } from 'effect';
import { MakeValueTypeParserOptions } from '../pg-protocol';
import { Duplex } from 'stream';
import { PgParseError, PgServerError, SchemaTypes } from './util';
import {
  ReadableError,
  ParseMessageError,
  NoMoreMessagesError,
} from '../stream/readable';
import { WritableError } from '../stream/writable';
import * as Schema from '@effect/schema/Schema';
import { queryStreamRaw } from './query-stream-raw';
import { formatErrors } from '@effect/schema/TreeFormatter';

type SchemaTypesUnion<A extends [...Schema.Schema<any>[]]> =
  SchemaTypes<A>[number];

export const queryStream =
  (socket: Duplex) =>
  <S extends [...Schema.Schema<any, any>[]]>(
    sqlOrOptions:
      | string
      | { sql: string; parserOptions?: MakeValueTypeParserOptions },
    ...schemas: S
  ): Stream.Stream<
    never,
    | ReadableError
    | WritableError
    | ParseMessageError
    | NoMoreMessagesError
    | PgServerError
    | PgParseError,
    readonly [SchemaTypesUnion<S>, number]
  > => {
    const { sql, parserOptions } =
      typeof sqlOrOptions === 'string'
        ? { sql: sqlOrOptions, parserOptions: undefined }
        : sqlOrOptions;

    type State = {
      left: Schema.Schema<any, any>[];
      schema?: Schema.Schema<any, any>;
    };
    return queryStreamRaw(socket)(sql, parserOptions).pipe(
      Stream.mapAccumEffect(
        { left: schemas } as State,
        (
          s,
          [msg, index]
        ): Effect.Effect<
          never,
          PgParseError,
          readonly [State, readonly [any, number]]
        > => {
          const { schema, left } =
            index === 0 ? { schema: s.left[0], left: s.left.slice(1) } : s;

          if (!schema) {
            return Effect.fail(
              new PgParseError({
                message: 'unexpected result set',
              })
            );
          }

          return Effect.map(
            Schema.parse(schema)(msg).pipe(
              Effect.mapError(
                (pe) =>
                  new PgParseError({
                    message: formatErrors(pe.errors),
                  })
              ),
              Effect.tapError((pe) => Effect.logError(`\n${pe.message}`))
            ),
            (a) => [{ schema, left }, [a, index]]
          );
        }
      )
    );
  };
