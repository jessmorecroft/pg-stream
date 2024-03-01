import * as ParseResult from '@effect/schema/ParseResult';
import { Decimal } from 'decimal.js';
import { AST, Arbitrary, Pretty, Schema } from '@effect/schema';

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
      ParseResult.transform(ast, s, 'From', ParseResult.type(ast, s))
    );
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
  Decimal.isDecimal,
  {
    identifier: 'DecimalFromSelf',
    description: 'a decimal',
    pretty: (): Pretty.Pretty<Decimal> => (decimal) =>
      `new Decimal(${JSON.stringify(decimal)})`,
  }
);

export const DecimalFromString: Schema.Schema<Decimal, string> =
  Schema.transform(
    Schema.string,
    DecimalFromSelf,
    (s) => new Decimal(s),
    (n) => n.toString()
  ).pipe(Schema.identifier('DecimalFromString'));
