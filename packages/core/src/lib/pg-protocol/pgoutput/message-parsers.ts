import { pipe } from "fp-ts/lib/function";
import * as P from "parser-ts/Parser";
import * as O from "fp-ts/Option";
import * as B from "../../parser/buffer";
import * as P2 from "../../parser/parser";
import { ValueOf, repeat, oneOfArray } from "../../parser/parser";

export const begin = pipe(
  B.buffer("B"),
  P.chain(() => B.int64BE()),
  P.bindTo("finalLsn"),
  P.bind("timeStamp", () => B.int64BE()),
  P.bind("tranId", () => B.int32BE()),
  P.map(({ finalLsn, timeStamp, tranId }) => ({
    type: "Begin" as const,
    finalLsn,
    timeStamp,
    tranId,
  })),
);

export const message = pipe(
  B.buffer("M"),
  P.chain(() => B.boolean),
  P.bindTo("isTransactional"),
  P.bind("lsn", () => B.int64BE()),
  P.bind("prefix", () => B.nullString()),
  P.bind("content", () =>
    pipe(
      B.int32BE(),
      P.chain((length) => B.string()(length)),
    ),
  ),
  P.map(({ isTransactional, lsn, prefix, content }) => ({
    type: "Message" as const,
    isTransactional,
    lsn,
    prefix,
    content,
  })),
);

export const commit = pipe(
  B.buffer("C"),
  P.chain(() => P.sat<number>((c) => c === 0)),
  P.chain(() => B.int64BE()),
  P.bindTo("lsn"),
  P.bind("endLsn", () => B.int64BE()),
  P.bind("timeStamp", () => B.int64BE()),
  P.map(({ lsn, endLsn, timeStamp }) => ({
    type: "Commit" as const,
    lsn,
    endLsn,
    timeStamp,
  })),
);

export const origin = pipe(
  B.buffer("O"),
  P.chain(() => B.int64BE()),
  P.bindTo("lsn"),
  P.bind("name", () => B.nullString()),
  P.map(({ lsn, name }) => ({
    type: "Origin" as const,
    lsn,
    name,
  })),
);

export const relation = pipe(
  B.buffer("R"),
  P.chain(() => B.int32BE()),
  P.bindTo("id"),
  P.bind("namespace", () => B.nullString()),
  P.bind("name", () => B.nullString()),
  P.bind("replIdent", () =>
    B.keyOf({
      d: null,
      n: null,
      f: null,
      i: null,
    })(1),
  ),
  P.bind("columns", () =>
    pipe(
      B.int16BE(),
      P.bindTo("count"),
      P.chain(({ count }) =>
        repeat(count)(
          pipe(
            B.boolean,
            P.bindTo("isKey"),
            P.bind("name", () => B.nullString()),
            P.bind("dataTypeId", () => B.int32BE()),
            P.bind("typeMod", () => B.int32BE()),
          ),
        ),
      ),
    ),
  ),
  P.map(({ id, namespace, name, replIdent, columns }) => ({
    type: "Relation" as const,
    id,
    namespace,
    name,
    replIdent,
    columns,
  })),
);

export const type = pipe(
  B.buffer("Y"),
  P.chain(() => B.int32BE()),
  P.bindTo("id"),
  P.bind("namespace", () => B.nullString()),
  P.bind("name", () => B.nullString()),
  P.map(({ id, namespace, name }) => ({
    type: "Type" as const,
    id,
    namespace,
    name,
  })),
);

const tupleValueParsers: P.Parser<number, O.Option<null | string | Buffer>>[] =
  [
    pipe(
      B.buffer("n"),
      P.map(() => O.some(null)),
    ),
    pipe(
      B.buffer("u"),
      P.map(() => O.none),
    ),
    pipe(
      B.buffer("t"),
      P.chain(() => B.int32BE()),
      P.chain((length) => B.string()(length)),
      P.map((val) => O.some(val)),
    ),
    pipe(
      B.buffer("b"),
      P.chain(() => B.int32BE()),
      P.chain((length) => P2.items<number>(length)),
      P.map((buf) => O.some(buf as unknown as Buffer)),
    ),
  ];

const tupleData = pipe(
  B.int16BE(),
  P.chain((count) => repeat(count)(oneOfArray(tupleValueParsers))),
);

export const insert = pipe(
  B.buffer("I"),
  P.chain(() => B.int32BE()),
  P.bindTo("relationId"),
  P.chainFirst(() => B.buffer("N")),
  P.bind("newRecord", () => tupleData),
  P.map(({ relationId, newRecord }) => ({
    type: "Insert" as const,
    relationId,
    newRecord,
  })),
);

export const update = pipe(
  B.buffer("U"),
  P.chain(() => B.int32BE()),
  P.bindTo("relationId"),
  P.bind("oldKey", () =>
    P.optional(
      pipe(
        B.buffer("K"),
        P.chain(() => tupleData),
      ),
    ),
  ),
  P.bind("oldRecord", () =>
    P.optional(
      pipe(
        B.buffer("O"),
        P.chain(() => tupleData),
      ),
    ),
  ),
  P.chainFirst(() => B.buffer("N")),
  P.bind("newRecord", () => tupleData),
  P.map(({ relationId, oldKey, oldRecord, newRecord }) => ({
    type: "Update" as const,
    relationId,
    oldKey,
    oldRecord,
    newRecord,
  })),
);

export const destroy = pipe(
  B.buffer("D"),
  P.chain(() => B.int32BE()),
  P.bindTo("relationId"),
  P.bind("oldKey", () =>
    P.optional(
      pipe(
        B.buffer("K"),
        P.chain(() => tupleData),
      ),
    ),
  ),
  P.bind("oldRecord", () =>
    P.optional(
      pipe(
        B.buffer("O"),
        P.chain(() => tupleData),
      ),
    ),
  ),
  P.map(({ relationId, oldKey, oldRecord }) => ({
    type: "Delete" as const,
    relationId,
    oldKey,
    oldRecord,
  })),
);

export const truncate = pipe(
  B.buffer("T"),
  P.chain(() => B.int32BE()),
  P.bindTo("count"),
  P.bind("options", () => P.item<number>()),
  P.bind("relationIds", ({ count }) => P2.repeat(count)(B.int32BE())),
  P.map(({ options, relationIds }) => ({
    type: "Truncate" as const,
    options,
    relationIds,
  })),
);

export type TupleData = ValueOf<typeof tupleData>;

export type Begin = ValueOf<typeof begin>;
export type Message = ValueOf<typeof message>;
export type Commit = ValueOf<typeof commit>;
export type Origin = ValueOf<typeof origin>;
export type Relation = ValueOf<typeof relation>;
export type Type = ValueOf<typeof type>;
export type Insert = ValueOf<typeof insert>;
export type Update = ValueOf<typeof update>;
export type Delete = ValueOf<typeof destroy>;
export type Truncate = ValueOf<typeof truncate>;

export type PgOutputMessageTypes =
  | Begin
  | Message
  | Commit
  | Origin
  | Relation
  | Type
  | Insert
  | Update
  | Delete
  | Truncate;

export type PgOutputMessageParser = P.Parser<number, PgOutputMessageTypes>;

export const pgOutputMessageParsers: PgOutputMessageParser[] = [
  begin,
  message,
  commit,
  origin,
  relation,
  type,
  insert,
  update,
  destroy,
  truncate,
];

export const pgOutputMessageParser = oneOfArray(pgOutputMessageParsers);
