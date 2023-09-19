import * as Schema from '@effect/schema/Schema';
import * as ParseResult from '@effect/schema/ParseResult';

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
