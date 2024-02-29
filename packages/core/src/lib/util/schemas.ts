import * as ParseResult from '@effect/schema/ParseResult';
import { Decimal } from 'decimal.js';
import { AST, Schema } from '@effect/schema';

export const walLsnFromString = Schema.transformOrFail(
  Schema.string,
  Schema.bigintFromSelf,
  (s) => {
    const match = s.match(/^([A-F0-9]+)\/([A-F0-9]+)$/);
    if (match) {
      const top = BigInt(Number.parseInt(match[1], 16));
      const bottom = BigInt(Number.parseInt(match[2], 16));
      return ParseResult.succeed((top << 32n) + bottom);
    }
    return ParseResult.fail(ParseResult.unexpected(s));
  },
  (i) =>
    ParseResult.succeed(
      `${Number(i >> 32n)
        .toString(16)
        .toUpperCase()}/${Number(i & 0xffffffffn)
        .toString(16)
        .toUpperCase()}`
    )
);

export const DecimalFromSelf: Schema.Schema<Decimal> = Schema.declare(
  [],
  Schema.struct({}),
  () => (u, _, ast) =>
    Decimal.isDecimal(u)
      ? ParseResult.succeed(u)
      : ParseResult.fail(ParseResult.type(ast, u)),
  {
    [AST.IdentifierAnnotationId]: 'Decimal',
    [AST.MessageAnnotationId]: 'a decimal',
  }
);

export const DecimalFromString = Schema.transformOrFail(
  Schema.string,
  DecimalFromSelf,
  (s) => {
    try {
      return ParseResult.succeed(new Decimal(s));
    } catch (e) {
      return ParseResult.fail(ParseResult.unexpected(s));
    }
  },
  (d) => ParseResult.succeed(d.toString())
);
