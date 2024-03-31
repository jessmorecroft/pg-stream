import { pipe } from "fp-ts/lib/function";
import * as O from "fp-ts/Option";
import * as P from "parser-ts/Parser";
import { HKT, Kind, URIS } from "fp-ts/lib/HKT";
import { Foldable, Foldable1 } from "fp-ts/lib/Foldable";
import * as S from "parser-ts/Stream";
import * as PR from "parser-ts/ParseResult";
import * as A from "fp-ts/Array";
import * as E from "fp-ts/Either";
import * as NEA from "fp-ts/NonEmptyArray";

export type ValueOf<T> = T extends P.Parser<any, infer A> ? A : never;
export type InputOf<T> = T extends P.Parser<infer I, any> ? I : never;

export function oneOf<F extends URIS>(
  F: Foldable1<F>,
): <I, A>(pp: Kind<F, P.Parser<I, A>>) => P.Parser<I, A>;
export function oneOf<F>(
  F: Foldable<F>,
): <I, A>(pp: HKT<F, P.Parser<I, A>>) => P.Parser<I, A>;
export function oneOf<F>(
  F: Foldable<F>,
): <I, A>(pp: HKT<F, P.Parser<I, A>>) => P.Parser<I, A> {
  return (pp) =>
    F.reduce(pp, P.fail(), (prev, p) =>
      pipe(
        prev,
        P.alt(() => p),
      ),
    );
}

export const oneOfArray = oneOf(A.Foldable);

export const skip: <A>(
  i: S.Stream<A>,
  count: number,
) => O.Option<S.Stream<A>> = (i, count) => {
  const endIndex = count + i.cursor;
  if (endIndex <= i.buffer.length) {
    return O.some(S.stream(i.buffer, endIndex));
  }
  return O.none;
};

export const getManyAndNext: <A>(
  i: S.Stream<A>,
  count: number,
) => O.Option<{
  value: A[];
  next: S.Stream<A>;
}> = (i, count) => {
  const endIndex = count + i.cursor;
  if (endIndex <= i.buffer.length) {
    return O.some({
      value: i.buffer.slice(i.cursor, endIndex),
      next: S.stream(i.buffer, endIndex),
    });
  }
  return O.none;
};

export const items: <A>(count: number) => P.Parser<A, Array<A>> =
  (count: number) => (i) =>
    pipe(
      getManyAndNext(i, count),
      O.fold(
        () => PR.error(i, [`Failed to consume ${count} item(s)`]),
        ({ value, next }) => PR.success(value, next, i),
      ),
    );

export const minLeft: <I>(count: number) => P.Parser<I, number> = (
  count: number,
) =>
  P.lookAhead((i) =>
    pipe(
      skip(i, count),
      O.fold(
        () => PR.error(i, [`Expected >= ${count} item(s)`]),
        (next) => PR.success(count, next, i),
      ),
    ),
  );

export const fixedLength: (
  length: number,
) => <I, A>(parser: P.Parser<I, A>) => P.Parser<I, A> = (length) => (parser) =>
  pipe(
    minLeft<InputOf<typeof parser>>(length),
    P.chain(() => P.withStart(parser)),
    P.chain(([a, before]) => (after) => {
      const consumed = after.cursor - before.cursor;
      return length === consumed
        ? PR.success(a, after, before)
        : PR.error(before, [
            `Consumed ${consumed} item(s), expected ${length}`,
          ]);
    }),
  );

export const mapLeft: <I>(
  f: (e: PR.ParseError<I>) => PR.ParseError<I>,
) => <A>(parser: P.Parser<I, A>) => P.Parser<I, A> = (e) => (parser) => (i) =>
  pipe(parser(i), E.mapLeft(e));

export const repeat: (
  count: number,
) => <I, A>(parser: P.Parser<I, A>) => P.Parser<I, A[]> =
  (count) => (parser) =>
    count === 0
      ? P.of([])
      : pipe(
          P.withStart(
            pipe(
              parser,
              P.chain((head) =>
                P.ChainRec.chainRec(NEA.of(head), (acc) =>
                  acc.length === count
                    ? P.of(E.right(acc))
                    : pipe(
                        parser,
                        P.map((a) => E.left(NEA.snoc(acc, a))),
                      ),
                ),
              ),
            ),
          ),
          P.chain(([results, i]) =>
            results.length === count ? P.of(results) : P.failAt(i),
          ),
        );
