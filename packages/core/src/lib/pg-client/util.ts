/* eslint-disable @typescript-eslint/no-explicit-any */
import { Chunk, Data, Effect, pipe } from "effect";
import * as Schema from "@effect/schema/Schema";
import * as P from "parser-ts/Parser";
import * as S from "parser-ts/string";
import * as E from "fp-ts/Either";
import {
  DataRow,
  ErrorResponse,
  NoticeResponse,
  PgClientMessageTypes,
  RowDescription,
  pgServerMessageParser,
} from "../pg-protocol/message-parsers";
import {
  ALL_ENABLED_PARSER_OPTIONS,
  MakeValueTypeParserOptions,
  ValueType,
  makePgClientMessage,
  makeValueTypeParser,
} from "../pg-protocol";
import { PgServerMessageTypes } from "../pg-protocol/message-parsers";
import {
  NoTransactionContextError,
  PgOutputDecoratedMessageTypes,
  TableInfoNotFoundError,
} from "./transform-log-data";
import {
  NoMoreMessagesError,
  ParseMessageError,
  ParseMessageGroupError,
  ReadableError,
  UnexpectedMessageError,
  WritableError,
  hasTypeOf,
} from "../stream";
import * as stream from "../stream";
import { Readable, Writable } from "stream";
import { SocketError } from "../socket/socket";

export interface XLogProcessor<E, T extends PgOutputDecoratedMessageTypes> {
  key?: ((msg: T) => string) | "serial" | "table";
  filter(msg: PgOutputDecoratedMessageTypes): msg is T;
  process(key: string, chunk: Chunk.Chunk<T>): Effect.Effect<void, E>;
}

export class PgParseError extends Data.TaggedError("PgParseError")<{
  message: string;
}> {}

export class PgServerError extends Data.TaggedError("PgServerError")<{
  error: ErrorResponse;
}> {}

export class PgFailedAuth extends Data.TaggedError("PgFailedAuth")<{
  reply: unknown;
  msg?: string;
}> {}

export class XLogProcessorError<E> extends Data.TaggedError(
  "XLogProcessorError",
)<{
  cause: E;
}> {}

export class PgClientError extends Data.TaggedError("PgClientError")<{
  cause:
    | WritableError
    | ReadableError
    | SocketError
    | ParseMessageError
    | ParseMessageGroupError
    | NoMoreMessagesError
    | PgServerError
    | PgParseError
    | PgFailedAuth
    | TableInfoNotFoundError
    | NoTransactionContextError
    | UnexpectedMessageError;
  msg?: string;
}> {}

type MessageTypes = Exclude<
  PgServerMessageTypes,
  NoticeResponse | ErrorResponse
>;

export type SchemaTypes<A extends [...Schema.Schema<any, any, any>[]]> = {
  [K in keyof A]: Schema.Schema.Type<A[K]>;
};

export const item = P.item<MessageTypes>();

export const isReadyForQuery = hasTypeOf("ReadyForQuery");
export const isNoticeResponse = hasTypeOf("NoticeResponse");
export const isErrorResponse = hasTypeOf("ErrorResponse");
export const isCommandComplete = hasTypeOf("CommandComplete");
export const isDataRow = hasTypeOf("DataRow");
export const isRowDescription = hasTypeOf("RowDescription");
export const isParameterStatus = hasTypeOf("ParameterStatus");
export const isBackendKeyData = hasTypeOf("BackendKeyData");

export const read: (
  readable: Readable,
) => Effect.Effect<
  MessageTypes,
  Effect.Effect.Error<ReturnType<typeof stream.read>> | PgServerError
> = (readable) =>
  Effect.filterOrElse(
    stream.read(readable, stream.decode(pgServerMessageParser)),
    (msg): msg is MessageTypes =>
      !isNoticeResponse(msg) && !isErrorResponse(msg),
    (msg) => {
      if (isNoticeResponse(msg)) {
        return Effect.flatMap(logBackendMessage(msg), () => read(readable));
      }
      if (isErrorResponse(msg)) {
        return Effect.flatMap(logBackendMessage(msg), () =>
          Effect.fail(new PgServerError({ error: msg })),
        );
      }
      return Effect.never;
    },
  );

export const readOrFail =
  (readable: Readable) =>
  <K extends MessageTypes["type"]>(
    ...types: [K, ...K[]]
  ): Effect.Effect<
    MessageTypes & { type: K },
    Effect.Effect.Error<ReturnType<typeof read>> | UnexpectedMessageError
  > =>
    stream.readOrFail(read(readable))(...types);

export const readUntilReady: (
  readable: Readable,
) => <A>(
  parser: P.Parser<MessageTypes, A>,
) => Effect.Effect<
  A,
  Effect.Effect.Error<ReturnType<typeof read>> | ParseMessageGroupError
> = (readable) => (parser) =>
  stream.readMany(read(readable))(
    parser,
    ({ type }) => type === "ReadyForQuery",
  );

export const write: (
  writable: Writable,
) => (message: PgClientMessageTypes) => Effect.Effect<void, WritableError> = (
  writable,
) => stream.write(writable, makePgClientMessage);

export const transformRowDescription: <OT extends MakeValueTypeParserOptions>(
  rowDescription: RowDescription,
  options?: OT,
) => { name: string; parser: P.Parser<string, ValueType<OT>> }[] = (
  { fields },
  options = ALL_ENABLED_PARSER_OPTIONS as any,
) =>
  fields.map(({ name, dataTypeId }) => ({
    name,
    parser: makeValueTypeParser(dataTypeId, options),
  }));

export const transformDataRow = <OT extends MakeValueTypeParserOptions>({
  rowParsers,
  dataRow,
}: {
  rowParsers: { name: string; parser: P.Parser<string, ValueType<OT>> }[];
  dataRow: DataRow;
}) =>
  rowParsers.reduce(
    (acc: Record<string, ValueType<OT>>, { name, parser }, index) => {
      const input = dataRow.values[index];
      if (input !== null) {
        const parsed = pipe(
          S.run(input)(parser),
          E.fold(
            (): ValueType<OT> => {
              // Failed to parse row value. Just use the original input.
              return input;
            },
            ({ value }) => value,
          ),
        );
        return { ...acc, [name]: parsed };
      }
      return { ...acc, [name]: input };
    },
    {},
  );

export const logBackendMessage = (message: NoticeResponse | ErrorResponse) => {
  const pairs =
    message.type === "NoticeResponse" ? message.notices : message.errors;

  const { log, fields, msg } = pairs.reduce(
    ({ log, fields, msg }, { type, value }) => {
      switch (type) {
        case "V": {
          const log2 =
            {
              ERROR: Effect.logError,
              FATAL: Effect.logFatal,
              PANIC: Effect.logFatal,
              WARNING: Effect.logWarning,
              NOTICE: Effect.logInfo,
              DEBUG: Effect.logDebug,
              INFO: Effect.logInfo,
              LOG: Effect.log,
            }[value] ?? log;
          return { log: log2, fields, msg };
        }
        case "C": {
          return { log, fields: { ...fields, code: value }, msg };
        }
        case "M": {
          return { log, fields, msg: `POSTGRES: ${value}` };
        }
        case "D": {
          return { log, fields: { ...fields, detail: value }, msg };
        }
        case "H": {
          return { log, fields: { ...fields, advice: value }, msg };
        }
        case "P": {
          return { log, fields: { ...fields, queryPosition: value }, msg };
        }
        case "p": {
          return {
            log,
            fields: { ...fields, internalQueryPosition: value },
            msg,
          };
        }
        case "q": {
          return { log, fields: { ...fields, internalQuery: value }, msg };
        }
        case "W": {
          return { log, fields: { ...fields, context: value }, msg };
        }
        case "s": {
          return { log, fields: { ...fields, schema: value }, msg };
        }
        case "t": {
          return { log, fields: { ...fields, table: value }, msg };
        }
        case "c": {
          return { log, fields: { ...fields, column: value }, msg };
        }
        case "d": {
          return { log, fields: { ...fields, dataType: value }, msg };
        }
        case "n": {
          return { log, fields: { ...fields, constraint: value }, msg };
        }
        case "F": {
          return { log, fields: { ...fields, file: value }, msg };
        }
        case "L": {
          return { log, fields: { ...fields, line: value }, msg };
        }
        case "R": {
          return { log, fields: { ...fields, routine: value }, msg };
        }
      }
      return { log, fields, msg };
    },
    {
      log: Effect.log,
      fields: {},
      msg: "Message received from backend.",
    },
  );

  return log(JSON.stringify({ msg, fields }));
};
