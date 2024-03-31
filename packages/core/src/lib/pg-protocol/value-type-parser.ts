/* eslint-disable @typescript-eslint/no-explicit-any */
import * as E from "fp-ts/Either";
import * as S from "parser-ts/string";
import * as C from "parser-ts/char";
import * as P from "parser-ts/Parser";
import { pipe } from "fp-ts/lib/function";
import Decimal from "decimal.js";

/**
 * These types were obtained using the following query, which evolved from the one used by the brianc/node-pg-types lib:
 *
SELECT json_object_agg(
  PT.typname,
    json_build_object(
      'baseTypeOid', PT.oid::int4,
       'arrayTypeOid', PTA.oid::int4
    ) order by PT.oid)
FROM pg_type PT
INNER JOIN pg_type PTA
  ON PT.oid = PTA.typelem AND
     PTA.typcategory = 'A' AND
     PTA.typname NOT LIKE '%vector'
WHERE PT.typnamespace = (SELECT pgn.oid FROM pg_namespace pgn
                         WHERE nspname = 'pg_catalog') -- Take only builting Postgres types with stable OID (extension types are not guaranted to be stable)
  AND PT.typtype = 'b' -- Only basic types
  AND PT.typelem = 0
  AND PT.typisdefined
 */
export const PgTypes = {
  bool: { baseTypeOid: 16, arrayTypeOid: 1000 },
  bytea: { baseTypeOid: 17, arrayTypeOid: 1001 },
  char: { baseTypeOid: 18, arrayTypeOid: 1002 },
  int8: { baseTypeOid: 20, arrayTypeOid: 1016 },
  int2: { baseTypeOid: 21, arrayTypeOid: 1005 },
  int4: { baseTypeOid: 23, arrayTypeOid: 1007 },
  regproc: { baseTypeOid: 24, arrayTypeOid: 1008 },
  text: { baseTypeOid: 25, arrayTypeOid: 1009 },
  oid: { baseTypeOid: 26, arrayTypeOid: 1028 },
  tid: { baseTypeOid: 27, arrayTypeOid: 1010 },
  xid: { baseTypeOid: 28, arrayTypeOid: 1011 },
  cid: { baseTypeOid: 29, arrayTypeOid: 1012 },
  json: { baseTypeOid: 114, arrayTypeOid: 199 },
  xml: { baseTypeOid: 142, arrayTypeOid: 143 },
  path: { baseTypeOid: 602, arrayTypeOid: 1019 },
  polygon: { baseTypeOid: 604, arrayTypeOid: 1027 },
  cidr: { baseTypeOid: 650, arrayTypeOid: 651 },
  float4: { baseTypeOid: 700, arrayTypeOid: 1021 },
  float8: { baseTypeOid: 701, arrayTypeOid: 1022 },
  circle: { baseTypeOid: 718, arrayTypeOid: 719 },
  macaddr8: { baseTypeOid: 774, arrayTypeOid: 775 },
  money: { baseTypeOid: 790, arrayTypeOid: 791 },
  macaddr: { baseTypeOid: 829, arrayTypeOid: 1040 },
  inet: { baseTypeOid: 869, arrayTypeOid: 1041 },
  aclitem: { baseTypeOid: 1033, arrayTypeOid: 1034 },
  bpchar: { baseTypeOid: 1042, arrayTypeOid: 1014 },
  varchar: { baseTypeOid: 1043, arrayTypeOid: 1015 },
  date: { baseTypeOid: 1082, arrayTypeOid: 1182 },
  time: { baseTypeOid: 1083, arrayTypeOid: 1183 },
  timestamp: { baseTypeOid: 1114, arrayTypeOid: 1115 },
  timestamptz: { baseTypeOid: 1184, arrayTypeOid: 1185 },
  interval: { baseTypeOid: 1186, arrayTypeOid: 1187 },
  timetz: { baseTypeOid: 1266, arrayTypeOid: 1270 },
  bit: { baseTypeOid: 1560, arrayTypeOid: 1561 },
  varbit: { baseTypeOid: 1562, arrayTypeOid: 1563 },
  numeric: { baseTypeOid: 1700, arrayTypeOid: 1231 },
  refcursor: { baseTypeOid: 1790, arrayTypeOid: 2201 },
  regprocedure: { baseTypeOid: 2202, arrayTypeOid: 2207 },
  regoper: { baseTypeOid: 2203, arrayTypeOid: 2208 },
  regoperator: { baseTypeOid: 2204, arrayTypeOid: 2209 },
  regclass: { baseTypeOid: 2205, arrayTypeOid: 2210 },
  regtype: { baseTypeOid: 2206, arrayTypeOid: 2211 },
  uuid: { baseTypeOid: 2950, arrayTypeOid: 2951 },
  txid_snapshot: { baseTypeOid: 2970, arrayTypeOid: 2949 },
  pg_lsn: { baseTypeOid: 3220, arrayTypeOid: 3221 },
  tsquery: { baseTypeOid: 3615, arrayTypeOid: 3645 },
  regconfig: { baseTypeOid: 3734, arrayTypeOid: 3735 },
  regdictionary: { baseTypeOid: 3769, arrayTypeOid: 3770 },
  jsonb: { baseTypeOid: 3802, arrayTypeOid: 3807 },
  jsonpath: { baseTypeOid: 4072, arrayTypeOid: 4073 },
  regnamespace: { baseTypeOid: 4089, arrayTypeOid: 4090 },
  regrole: { baseTypeOid: 4096, arrayTypeOid: 4097 },
  regcollation: { baseTypeOid: 4191, arrayTypeOid: 4192 },
  pg_snapshot: { baseTypeOid: 5038, arrayTypeOid: 5039 },
  xid8: { baseTypeOid: 5069, arrayTypeOid: 271 },
};

export type PgTypeName = keyof typeof PgTypes;

export type MakeValueTypeParserOptions = {
  parseNumerics?: boolean;
  parseDates?: boolean;
  parseInts?: boolean;
  parseBigInts?: boolean;
  parseBooleans?: boolean;
  parseFloats?: boolean;
  parseJson?: boolean;
  parseArrays?: boolean;
};

export type NestedArray<A> = (A | NestedArray<A>)[];

export type IfFlag<O, F extends string, T> =
  Required<O> extends {
    [K in F]: boolean;
  }
    ? O extends { [K in F]: false }
      ? never // flag is false
      : T // flag is true or boolean or an optional boolean
    : never; // no flag in options type

export type BaseValueType<O> =
  | IfFlag<O, "parseBooleans", boolean>
  | IfFlag<O, "parseBigInts", bigint>
  | IfFlag<O, "parseDates", Date>
  | IfFlag<O, "parseFloats", number>
  | IfFlag<O, "parseInts", number>
  | IfFlag<O, "parseNumerics", Decimal>
  | IfFlag<O, "parseJson", object>
  | string
  | null;

export type ValueType<O = MakeValueTypeParserOptions> =
  | BaseValueType<O>
  | IfFlag<O, "parseArrays", NestedArray<BaseValueType<O>>>;

export const ALL_ENABLED_PARSER_OPTIONS: MakeValueTypeParserOptions = {
  parseBooleans: true,
  parseFloats: true,
  parseInts: true,
  parseJson: true,
  parseArrays: true,
  parseNumerics: true,
  parseDates: true,
  parseBigInts: true,
};

export const DEFAULT_PARSER_OPTIONS: MakeValueTypeParserOptions = {
  ...ALL_ENABLED_PARSER_OPTIONS,
  parseNumerics: false,
  parseDates: false,
  parseBigInts: false,
};

export const NONE_ENABLED_PARSER_OPTIONS = {};

export const makeValueTypeParser = <O extends MakeValueTypeParserOptions>(
  oid: number,
  options?: O,
): P.Parser<string, ValueType<O>> => {
  const opt = options ?? (DEFAULT_PARSER_OPTIONS as O);

  const elementTypeOid = ArrayTypeMap.get(oid);
  if (elementTypeOid && opt.parseArrays) {
    return makeArrayValueTypeParser(
      makeBaseValueTypeParser(elementTypeOid, opt),
    ) as P.Parser<string, ValueType<O>>;
  }
  return pipe(
    makeBaseValueTypeParser(oid, opt),
    P.chainFirst(() => P.eof()),
  ) as P.Parser<string, ValueType<O>>;
};

export const getTypeName = (
  oid: number,
): `${PgTypeName}` | `${PgTypeName}[]` | "unknown" => {
  const typeName = BaseTypeMap.get(oid);
  if (typeName) {
    return typeName;
  }
  const elementTypeOid = ArrayTypeMap.get(oid);
  if (elementTypeOid) {
    const elementTypeName = BaseTypeMap.get(elementTypeOid);
    if (elementTypeName) {
      return `${elementTypeName}[]`;
    }
  }
  return "unknown";
};

const BaseTypeMap = new Map<number, PgTypeName>(
  Array.from(Object.entries(PgTypes)).map(
    ([name, { baseTypeOid }]) => [baseTypeOid, name as PgTypeName] as const,
  ),
);

const ArrayTypeMap = new Map<number, number>(
  Array.from(Object.entries(PgTypes)).map(
    ([, { baseTypeOid, arrayTypeOid }]) => [arrayTypeOid, baseTypeOid],
  ),
);

const makeBaseValueTypeParser: <O extends MakeValueTypeParserOptions>(
  oid: number,
  options: O,
) => P.Parser<string, BaseValueType<O>> = (oid, options) => {
  const baseType = BaseTypeMap.get(oid);

  switch (baseType) {
    case "int2":
    case "int4": {
      if (options.parseInts) {
        return S.int;
      }
      return anyParser;
    }
    case "int8": {
      if (options.parseBigInts) {
        return bigIntParser;
      }
      return anyParser;
    }
    case "numeric": {
      if (options.parseNumerics) {
        return decimalParser;
      }
      return anyParser;
    }
    case "bool": {
      if (options.parseBooleans) {
        return boolParser;
      }
      return anyParser;
    }
    case "float4":
    case "float8": {
      if (options.parseFloats) {
        return S.float;
      }
      return anyParser;
    }
    case "json":
    case "jsonb": {
      if (options.parseJson) {
        return jsonParser;
      }
      return anyParser;
    }
    case "date":
    case "timestamp":
    case "timestamptz": {
      if (options.parseDates) {
        return dateParser;
      }
      return anyParser;
    }
    default: {
      return anyParser;
    }
  }
};

const anyParser = S.many(P.item<string>());

const dateParser = pipe(
  anyParser,
  P.chain((s) => {
    const dt = new Date(s);
    return isNaN(dt.getTime()) ? P.fail() : P.succeed(dt);
  }),
);

const decimalParser = P.expected(
  pipe(
    S.fold([
      S.maybe(C.oneOf("+-")),
      C.many1(C.digit),
      S.maybe(S.fold([C.char("."), C.many1(C.digit)])),
    ]),
    P.map((s) => new Decimal(s)),
  ),
  "a decimal",
);

const bigIntParser = P.expected(
  pipe(
    S.fold([S.maybe(C.char("-")), C.many1(C.digit)]),
    P.map((s) => BigInt(s)),
  ),
  "an integer",
);

const jsonParser = pipe(
  S.many(P.item<string>()),
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  P.chain<string, string, any>((s) =>
    pipe(
      E.tryCatch(
        () => JSON.parse(s),
        (error) =>
          error instanceof Error ? `JSON, but got: ${error.message}` : "JSON",
      ),
      E.fold(
        (expected) => P.expected(P.fail(), expected),
        (json) => P.succeed(json),
      ),
    ),
  ),
);

const boolParser = P.expected(
  pipe(
    P.sat<string>((c) => c === "t" || c === "f"),
    P.map((c) => c === "t"),
  ),
  "true (t) or false (f)",
);

const makeArrayValueTypeParser = <A>(
  elementParser: P.Parser<string, A>,
): P.Parser<string, NestedArray<A>> => {
  const innerParser = P.either(
    pipe(
      S.doubleQuotedString,
      P.chain<string, string, A>((quoted) =>
        pipe(
          S.run(quoted.replaceAll('\\"', '"'))(
            pipe(
              elementParser,
              P.chainFirst(() => P.eof()),
            ),
          ),
          E.fold(
            () => P.fail(),
            ({ value }) => P.succeed(value),
          ),
        ),
      ),
    ),
    () =>
      pipe(
        P.lookAhead(C.notChar("{")), // don't want the catch-all parser matching a subarray
        P.chain(() => elementParser),
      ),
  );

  return P.between(
    C.char("{"),
    C.char("}"),
  )(
    P.sepBy(
      C.char(","),
      P.either<string, A | NestedArray<A>>(innerParser, () =>
        makeArrayValueTypeParser(innerParser),
      ),
    ),
  );
};
