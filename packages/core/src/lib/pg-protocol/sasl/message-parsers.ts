import * as P from 'parser-ts/Parser';
import * as S from 'parser-ts/string';
import * as C from 'parser-ts/char';
import { pipe } from 'fp-ts/lib/function';
import { ValueOf } from '../../parser/parser';

export const serverFirstMessageParser = pipe(
  S.string('r='),
  P.chain(() => S.many1(C.notOneOf(','))),
  P.bindTo('nonce'),
  P.chainFirst(() => S.string(',')),
  P.bind('salt', () =>
    pipe(
      S.string('s='),
      P.chain(() => S.many1(C.notOneOf(','))),
      P.map((salt) => Buffer.from(salt, 'base64'))
    )
  ),
  P.chainFirst(() => S.string(',')),
  P.bind('iterationCount', () =>
    pipe(
      S.string('i='),
      P.chain(() => S.int)
    )
  )
);

export const serverFinalMessageParser = pipe(
  S.string('v='),
  P.chain(() => S.many1(C.notOneOf(','))),
  P.map((salt) => Buffer.from(salt, 'base64')),
  P.bindTo('serverSignature')
);

export type ServerFirstMessage = ValueOf<
  ReturnType<typeof serverFirstMessageParser>
>;

export type ServerFinalMessage = ValueOf<typeof serverFinalMessageParser>;
