import * as ParseResult from "@effect/schema/ParseResult";
import { Decimal } from "decimal.js";
import { AST, Schema } from "@effect/schema";

export const walLsnFromString = Schema.transformOrFail(
  Schema.string,
  Schema.bigintFromSelf,
  (s, options, ast) => {
    const match = s.match(/^([A-F0-9]+)\/([A-F0-9]+)$/);
    if (match) {
      const top = BigInt(Number.parseInt(match[1], 16));
      const bottom = BigInt(Number.parseInt(match[2], 16));
      return ParseResult.succeed((top << 32n) + bottom);
    }
    return ParseResult.fail(
      new ParseResult.Transformation(
        ast,
        s,
        "Transformation",
        new ParseResult.Type(ast, s, "unexpected format"),
      ),
    );
  },
  (i) =>
    ParseResult.succeed(
      `${Number(i >> 32n)
        .toString(16)
        .toUpperCase()}/${Number(i & 0xffffffffn)
        .toString(16)
        .toUpperCase()}`,
    ),
);

export const DecimalFromSelf: Schema.Schema<Decimal> = Schema.declare(
  (u): u is Decimal => Decimal.isDecimal(u),
  {
    [AST.IdentifierAnnotationId]: "Decimal",
    [AST.MessageAnnotationId]: "a decimal",
    pretty: () => (d) => `new Decimal(${JSON.stringify(d)})`,
  },
);

export const DecimalFromString = Schema.transformOrFail(
  Schema.string,
  DecimalFromSelf,
  (s, options, ast) =>
    ParseResult.try({
      try: () => new Decimal(s),
      catch: (e) =>
        new ParseResult.Transformation(
          ast,
          s,
          "Transformation",
          new ParseResult.Type(ast, s, (e as Error).message),
        ),
    }),
  (d) => ParseResult.succeed(d.toString()),
).pipe(Schema.identifier("DecimalFromString"));
