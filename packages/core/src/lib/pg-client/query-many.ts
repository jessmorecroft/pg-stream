/* eslint-disable @typescript-eslint/no-explicit-any */
import { Effect } from "effect";
import { MakeValueTypeParserOptions } from "../pg-protocol";
import {
  NoMoreMessagesError,
  ParseMessageError,
  ParseMessageGroupError,
  ReadableError,
  WritableError,
} from "../stream";
import { Duplex } from "stream";
import { PgParseError, PgServerError } from "./util";
import * as Schema from "@effect/schema/Schema";
import { queryRaw } from "./query-raw";
import { formatError } from "@effect/schema/TreeFormatter";

export const queryMany =
  (socket: Duplex) =>
  <S extends Schema.Schema<any, any, any>>(
    sqlOrOptions:
      | string
      | { sql: string; parserOptions: MakeValueTypeParserOptions },
    schema: S,
  ): Effect.Effect<
    readonly Schema.Schema.Type<S>[],
    | ReadableError
    | WritableError
    | NoMoreMessagesError
    | ParseMessageError
    | ParseMessageGroupError
    | PgServerError
    | PgParseError,
    Schema.Schema.Context<S>
  > => {
    const { sql, parserOptions } =
      typeof sqlOrOptions === "string"
        ? { sql: sqlOrOptions, parserOptions: undefined }
        : sqlOrOptions;

    return queryRaw(socket)(sql, parserOptions).pipe(
      Effect.flatMap((rows) =>
        Schema.decodeUnknown(Schema.array(schema))(rows).pipe(
          Effect.mapError(
            (pe) =>
              new PgParseError({
                message: formatError(pe),
              }),
          ),
          Effect.tapError((pe) => Effect.logError(`\n${pe.message}`)),
        ),
      ),
    );
  };
