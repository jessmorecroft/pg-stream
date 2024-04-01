/* eslint-disable @typescript-eslint/no-explicit-any */
import { Effect, Stream } from "effect";
import { MakeValueTypeParserOptions } from "../pg-protocol";
import { Duplex } from "stream";
import { PgParseError, PgServerError, SchemaTypes } from "./util";
import {
  ReadableError,
  ParseMessageError,
  NoMoreMessagesError,
} from "../stream/readable";
import { WritableError } from "../stream/writable";
import * as Schema from "@effect/schema/Schema";
import { queryStreamRaw } from "./query-stream-raw";
import { formatError } from "@effect/schema/TreeFormatter";

type SchemaTypesUnion<A extends [...Schema.Schema<any, any, any>[]]> =
  SchemaTypes<A>[number];

export const queryStream =
  (socket: Duplex) =>
  <S extends Schema.Schema<any, any, any>[]>(
    sqlOrOptions:
      | string
      | { sql: string; parserOptions?: MakeValueTypeParserOptions },
    ...schemas: [...S]
  ): Stream.Stream<
    readonly [SchemaTypesUnion<S>, number],
    | ReadableError
    | WritableError
    | ParseMessageError
    | NoMoreMessagesError
    | PgServerError
    | PgParseError,
    Schema.Schema.Context<S[number]>
  > => {
    const { sql, parserOptions } =
      typeof sqlOrOptions === "string"
        ? { sql: sqlOrOptions, parserOptions: undefined }
        : sqlOrOptions;

    type State = {
      remaining: Schema.Schema<any, any, any>[];
      schema?: Schema.Schema<any, any, any>;
    };
    return queryStreamRaw(socket)(sql, parserOptions).pipe(
      Stream.mapAccumEffect(
        { remaining: schemas } as State,
        (
          s,
          [msg, index],
        ): Effect.Effect<
          readonly [State, readonly [any, number]],
          PgParseError,
          any
        > => {
          const { schema, remaining } =
            index === 0
              ? { schema: s.remaining[0], remaining: s.remaining.slice(1) }
              : s;

          if (!schema) {
            return Effect.fail(
              new PgParseError({
                message: "unexpected result set",
              }),
            );
          }

          return Effect.map(
            Schema.decodeUnknown(schema)(msg).pipe(
              Effect.mapError(
                (pe) =>
                  new PgParseError({
                    message: formatError(pe),
                  }),
              ),
              Effect.tapError((pe) => Effect.logError(`\n${pe.message}`)),
            ),
            (a) => [{ schema, remaining }, [a, index]],
          );
        },
      ),
    );
  };
