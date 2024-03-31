import * as P from "parser-ts/Parser";
import * as PR from "parser-ts/ParseResult";
import * as S from "parser-ts/Stream";
import * as Str from "parser-ts/string";
import { pipe } from "fp-ts/lib/function";
import * as O from "fp-ts/Option";
import * as E from "fp-ts/Either";
import * as P2 from "./parser";
import * as t from "io-ts";
import { failure } from "io-ts/PathReporter";

export type Byte = number;

export const stream: (buf: Buffer, cursor?: number) => S.Stream<Byte> = (
  buf,
  cursor,
) => S.stream(buf as unknown as Array<number>, cursor);

export function buffer(i: Buffer): P.Parser<Byte, Buffer>;
export function buffer(i: string, enc?: BufferEncoding): P.Parser<Byte, Buffer>;
export function buffer(
  i: string | Buffer,
  enc?: BufferEncoding,
): P.Parser<Byte, Buffer> {
  let buf: Buffer;
  if (typeof i === "string") {
    buf = Buffer.from(i, enc);
  } else {
    buf = i;
  }
  return P.expected(
    P.ChainRec.chainRec<Byte, Buffer, Buffer>(buf, (acc) =>
      pipe(
        O.fromNullable(acc.at(0)),
        O.fold(
          () => P.of(E.right(buf)),
          (c) =>
            pipe(
              P.sat((b: Byte) => b === c),
              P.chain(() => P.of(E.left(acc.slice(1)))),
            ),
        ),
      ),
    ),
    JSON.stringify(buf),
  );
}

export const string: (
  encoding?: BufferEncoding,
) => (n: number) => P.Parser<number, string> = (encoding) => (n) =>
  pipe(
    P2.items<number>(n),
    P.map((items) => Buffer.from(items).toString(encoding)),
  );

export const nullString: (
  encoding?: BufferEncoding,
) => P.Parser<number, string> = (encoding) =>
  pipe(
    P.manyTill(
      P.item<number>(),
      P.sat((c) => c === 0),
    ),
    P.map((buf) => Buffer.from(buf).toString(encoding)),
  );

export const keyOf: <K extends Record<string, null>>(
  keys: K,
  encoding?: BufferEncoding,
) => (length: number) => P.Parser<number, keyof K> =
  (keys, encoding) => (length) =>
    pipe(
      P.withStart(P2.items<number>(length)),
      P.chain(([buf, i]) =>
        pipe(
          t.keyof(keys).decode(Buffer.from(buf).toString(encoding)),
          E.fold(
            (error) => P.expected(P.failAt(i), failure(error).join(", ")),
            (val) => P.of(val),
          ),
        ),
      ),
    );

export const boolean = pipe(
  P.expected(
    P.sat<number>((n) => n === 1 || n === 0),
    "Expected 1 (true) or 0 (false).",
  ),
  P.map((n) => n === 1),
);

export const parseString: <T>(
  innerParser: P.Parser<string, T>,
) => (parser: P.Parser<number, string>) => P.Parser<number, T> =
  (innerParser) => (parser) =>
    pipe(
      P.withStart(parser),
      P.chain(
        ([value, s]) =>
          (i) =>
            pipe(
              Str.run(value)(innerParser),
              E.fold(
                (error) =>
                  PR.error(
                    S.stream(s.buffer, s.cursor + error.input.cursor),
                    error.expected,
                    error.fatal,
                  ),
                (result) => PR.success(result.value, i, s),
              ),
            ),
      ),
    );

const numberParser: <N extends bigint | number>(
  byteCount: number,
  convert: (buf: Buffer) => N,
) => (expected?: N) => P.Parser<Byte, N> = (byteCount, convert) => (expected) =>
  pipe(
    P.withStart(P2.items<Byte>(byteCount)),
    P.map(([buf, i]) => [convert(buf as unknown as Buffer), i] as const),
    P.chain(([n, i]) =>
      expected === undefined || expected === n
        ? P.of(n)
        : P.expected(P.failAt(i), `Expected "${expected}", not "${n}"`),
    ),
  );

export const int16BE = numberParser(2, (buf) => buf.readInt16BE());
export const int32BE = numberParser(4, (buf) => buf.readInt32BE());
export const int64BE = numberParser(8, (buf) => buf.readBigInt64BE());
export const lenInt32BE = numberParser(4, (buf) => buf.readInt32BE() - 4);
