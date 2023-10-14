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
      return ParseResult.success((top << 32n) + bottom);
    }
    return ParseResult.failure(ParseResult.unexpected(s));
  },
  (i) =>
    ParseResult.success(
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
      ? ParseResult.success(u)
      : ParseResult.failure(ParseResult.type(ast, u)),
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
      return ParseResult.success(new Decimal(s));
    } catch (e) {
      return ParseResult.failure(ParseResult.unexpected(s));
    }
  },
  (d) => ParseResult.success(d.toString())
);
