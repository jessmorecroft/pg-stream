import { pipe } from 'fp-ts/lib/function';
import * as P from 'parser-ts/Parser';
import * as B from '../parser/buffer';
import {
  ValueOf,
  minLeft,
  repeat,
  oneOfArray,
  items,
  fixedLength,
  mapLeft,
} from '../parser/parser';
import { pgOutputMessageParser } from './pgoutput/message-parsers';
import * as S from 'parser-ts/string';
import { serverFinalMessageParser, serverFirstMessageParser } from './sasl';
import * as E from 'fp-ts/Either';

const PROTOCOL_VERSION = 196608;

const SSL_REQUEST_CODE = 80877103;

export const startupMessage = pipe(
  B.lenInt32BE(),
  P.bindTo('length'),
  P.bind('contents', ({ length }) =>
    fixedLength(length)(
      pipe(
        B.int32BE(PROTOCOL_VERSION),
        P.bindTo('protocolVersion'),
        P.bind('parameters', () =>
          P.manyTill(
            pipe(
              B.nullString(),
              P.bindTo('name'),
              P.bind('value', () => B.nullString())
            ),
            P.sat((c) => c === 0)
          )
        )
      )
    )
  ),
  P.map(({ contents: { parameters, protocolVersion } }) => ({
    type: 'StartupMessage' as const,
    protocolVersion: protocolVersion as typeof PROTOCOL_VERSION,
    parameters,
  }))
);

export const sslRequest = pipe(
  B.int32BE(8),
  P.chain(() => B.int32BE(SSL_REQUEST_CODE)),
  P.bindTo('requestCode'),
  P.map(({ requestCode }) => ({
    type: 'SSLRequest' as const,
    requestCode: requestCode as typeof SSL_REQUEST_CODE,
  }))
);

export const authenticationOk = pipe(
  B.buffer('R'),
  P.chain(() => B.int32BE(8)),
  P.chain(() => B.int32BE(0)),
  P.map(() => ({ type: 'AuthenticationOk' as const }))
);

export const authenticationCleartextPassword = pipe(
  B.buffer('R'),
  P.chain(() => B.int32BE(8)),
  P.chain(() => B.int32BE(3)),
  P.map(() => ({ type: 'AuthenticationCleartextPassword' as const }))
);

export const authenticationMD5Password = pipe(
  B.buffer('R'),
  P.chain(() => B.int32BE(12)),
  P.chain(() => B.int32BE(5)),
  P.chain(() => items(4)),
  P.bindTo('salt'),
  P.map(({ salt }) => ({ type: 'AuthenticationMD5Password' as const, salt }))
);

export const authenticationSASL = pipe(
  B.buffer('R'),
  P.chain(() => B.lenInt32BE()),
  P.bindTo('length'),
  P.bind('contents', ({ length }) =>
    fixedLength(length)(
      pipe(
        B.int32BE(10),
        P.chain(() =>
          P.many1Till(
            B.nullString(),
            P.sat((c) => c === 0)
          )
        ),
        P.bindTo('mechanisms')
      )
    )
  ),
  P.map(({ contents: { mechanisms } }) => ({
    type: 'AuthenticationSASL' as const,
    mechanisms,
  }))
);

export const authenticationSASLContinue = pipe(
  B.buffer('R'),
  P.chain(() => B.lenInt32BE()),
  P.bindTo('length'),
  P.bind('contents', ({ length }) =>
    fixedLength(length)(
      pipe(
        B.int32BE(11),
        P.chain(() => B.string()(length - 4)),
        P.bindTo('serverFirstMessage'),
        P.bind('serverFirstMessageParsed', ({ serverFirstMessage }) =>
          pipe(
            S.run(serverFirstMessage)(serverFirstMessageParser),
            E.fold(
              () =>
                P.fail<
                  number,
                  { salt: Buffer; nonce: string; iterationCount: number }
                >(),
              ({ value }) => P.of(value)
            )
          )
        )
      )
    )
  ),
  P.map(
    ({
      contents: {
        serverFirstMessage,
        serverFirstMessageParsed: { iterationCount, nonce, salt },
      },
    }) => ({
      type: 'AuthenticationSASLContinue' as const,
      serverFirstMessage,
      iterationCount,
      nonce,
      salt,
    })
  )
);

export const authenticationSASLFinal = pipe(
  B.buffer('R'),
  P.chain(() => B.lenInt32BE()),
  P.bindTo('length'),
  P.bind('contents', ({ length }) =>
    fixedLength(length)(
      pipe(
        B.int32BE(12),
        P.chain(() => B.string()(length - 4)),
        P.chain((s) =>
          pipe(
            S.run(s)(serverFinalMessageParser),
            E.fold(
              () => P.fail<number, { serverSignature: Buffer }>(),
              ({ value }) => P.of(value)
            )
          )
        ),
        P.bindTo('serverFinalMessage')
      )
    )
  ),
  P.map(({ contents: { serverFinalMessage } }) => ({
    type: 'AuthenticationSASLFinal' as const,
    serverFinalMessage,
  }))
);

export const saslInitialResponse = pipe(
  B.buffer('p'),
  P.chain(() => B.lenInt32BE()),
  P.bindTo('length'),
  P.bind('contents', ({ length }) =>
    fixedLength(length)(
      pipe(
        B.nullString(),
        P.bindTo('mechanism'),
        P.bind('length2', () => B.int32BE()),
        P.bind('clientFirstMessage', ({ length2 }) => B.string()(length2))
      )
    )
  ),
  P.map(({ contents: { mechanism, clientFirstMessage } }) => ({
    type: 'SASLInitialResponse' as const,
    mechanism,
    clientFirstMessage,
  }))
);

export const saslResponse = pipe(
  B.buffer('p'),
  P.chain(() => B.lenInt32BE()),
  P.bindTo('length'),
  P.bind('clientFinalMessage', ({ length }) => B.string()(length)),
  P.map(({ clientFinalMessage }) => ({
    type: 'SASLResponse' as const,
    clientFinalMessage,
  }))
);

export const passwordMessage = pipe(
  B.buffer('p'),
  P.chain(() => B.lenInt32BE()),
  P.chain((length) => fixedLength(length)(B.nullString())),
  P.map((password) => ({ type: 'PasswordMessage' as const, password }))
);

export const backendKeyData = pipe(
  B.buffer('K'),
  P.chain(() => B.int32BE(12)),
  P.chain(() => B.int32BE()),
  P.bindTo('pid'),
  P.bind('secretKey', () => B.int32BE()),
  P.map(({ pid, secretKey }) => ({
    type: 'BackendKeyData' as const,
    pid,
    secretKey,
  }))
);

export const parameterStatus = pipe(
  B.buffer('S'),
  P.chain(() => B.lenInt32BE()),
  P.bindTo('length'),
  P.bind('contents', ({ length }) =>
    fixedLength(length)(
      pipe(
        B.nullString(),
        P.bindTo('name'),
        P.bind('value', () => B.nullString())
      )
    )
  ),
  P.map(({ contents: { name, value } }) => ({
    type: 'ParameterStatus' as const,
    name,
    value,
  }))
);

export const readyForQuery = pipe(
  B.buffer('Z'),
  P.chain(() => B.int32BE(5)),
  P.chain(() =>
    B.keyOf({
      I: null,
      T: null,
      E: null,
    })(1)
  ),
  P.map((transactionStatus) => ({
    type: 'ReadyForQuery' as const,
    transactionStatus,
  }))
);

export const errorResponse = pipe(
  B.buffer('E'),
  P.chain(() => B.lenInt32BE()),
  P.bindTo('length'),
  P.bind('errors', ({ length }) =>
    fixedLength(length)(
      pipe(
        P.many1Till(
          pipe(
            B.string()(1),
            P.bindTo('type'),
            P.bind('value', () => B.nullString())
          ),
          P.sat((c) => c === 0)
        )
      )
    )
  ),
  P.map(({ errors }) => ({
    type: 'ErrorResponse' as const,
    errors,
  }))
);

export const noticeResponse = pipe(
  B.buffer('N'),
  P.chain(() => B.lenInt32BE()),
  P.bindTo('length'),
  P.bind('notices', ({ length }) =>
    fixedLength(length)(
      pipe(
        P.many1Till(
          pipe(
            B.string()(1),
            P.bindTo('type'),
            P.bind('value', () => B.nullString())
          ),
          P.sat((c) => c === 0)
        )
      )
    )
  ),
  P.map(({ notices }) => ({
    type: 'NoticeResponse' as const,
    notices,
  }))
);

export const commandComplete = pipe(
  B.buffer('C'),
  P.chain(() => B.lenInt32BE()),
  P.bindTo('length'),
  P.bind('commandTag', ({ length }) => fixedLength(length)(B.nullString())),
  P.map(({ commandTag }) => ({
    type: 'CommandComplete' as const,
    commandTag,
  }))
);

export const query = pipe(
  B.buffer('Q'),
  P.chain(() => B.lenInt32BE()),
  P.bindTo('length'),
  P.bind('sql', ({ length }) => fixedLength(length)(B.nullString())),
  P.map(({ sql }) => ({
    type: 'Query' as const,
    sql,
  }))
);

export const rowDescription = pipe(
  B.buffer('T'),
  P.chain(() => B.lenInt32BE()),
  P.bindTo('length'),
  P.bind('contents', ({ length }) =>
    fixedLength(length)(
      pipe(
        B.int16BE(),
        P.bindTo('count'),
        P.bind('fields', ({ count }) =>
          repeat(count)(
            pipe(
              B.nullString(),
              P.bindTo('name'),
              P.bind('tableId', () => B.int32BE()),
              P.bind('columnId', () => B.int16BE()),
              P.bind('dataTypeId', () => B.int32BE()),
              P.bind('dataTypeSize', () => B.int16BE()),
              P.bind('dataTypeModifier', () => B.int32BE()),
              P.bind('format', () => B.int16BE())
            )
          )
        )
      )
    )
  ),
  P.map(({ contents: { fields } }) => ({
    type: 'RowDescription' as const,
    fields,
  }))
);

export const dataRow = pipe(
  B.buffer('D'),
  P.chain(() => B.lenInt32BE()),
  P.bind('contents', (length) =>
    fixedLength(length)(
      pipe(
        B.int16BE(),
        P.bindTo('count'),
        P.bind('values', ({ count }) =>
          repeat(count)(
            pipe(
              B.int32BE(),
              P.chain((valueLength) =>
                valueLength < 0 ? P.of(null) : B.string()(valueLength)
              )
            )
          )
        )
      )
    )
  ),
  P.map(({ contents: { values } }) => ({
    type: 'DataRow' as const,
    values,
  }))
);

export const copyBothResponse = pipe(
  B.buffer('W'),
  P.chain(() => B.lenInt32BE()),
  P.bind('contents', (length) =>
    fixedLength(length)(
      pipe(
        B.boolean,
        P.bindTo('binary'),
        P.bind('count', () => B.int16BE()),
        P.bind('columnBinary', ({ count }) => repeat(count)(B.boolean))
      )
    )
  ),
  P.map(({ contents: { columnBinary, binary } }) => ({
    type: 'CopyBothResponse' as const,
    binary,
    columnBinary,
  }))
);

export const copyDone = pipe(
  B.buffer('c'),
  P.chain(() => B.int32BE(4)),
  P.map(() => ({
    type: 'CopyDone' as const,
  }))
);

export const copyFail = pipe(
  B.buffer('f'),
  P.chain(() => B.lenInt32BE()),
  P.bind('error', (length) => fixedLength(length)(B.nullString())),
  P.map(({ error }) => ({
    type: 'CopyFail' as const,
    error,
  }))
);

const xLogData = (length: number) =>
  pipe(
    B.buffer('w'),
    P.chain(() => B.int64BE()),
    P.bindTo('walStart'),
    P.bind('walEnd', () => B.int64BE()),
    P.bind('timeStamp', () => B.int64BE()),
    P.bind('payload', () => fixedLength(length - 25)(pgOutputMessageParser)),
    P.map(({ walStart, walEnd, timeStamp, payload }) => ({
      type: 'XLogData' as const,
      walStart,
      walEnd,
      timeStamp,
      payload,
    }))
  );

const xKeepAlive = pipe(
  B.buffer('k'),
  P.chain(() => B.int64BE()),
  P.bindTo('walEnd'),
  P.bind('timeStamp', () => B.int64BE()),
  P.bind('replyNow', () => B.boolean),
  P.map(({ walEnd, timeStamp, replyNow }) => ({
    type: 'XKeepAlive' as const,
    walEnd,
    timeStamp,
    replyNow,
  }))
);

const xStatusUpdate = pipe(
  B.buffer('r'),
  P.chain(() => B.int64BE()),
  P.bindTo('lastWalWrite'),
  P.bind('lastWalFlush', () => B.int64BE()),
  P.bind('lastWalApply', () => B.int64BE()),
  P.bind('timeStamp', () => B.int64BE()),
  P.bind('replyNow', () => B.boolean),
  P.map(
    ({ lastWalWrite, lastWalFlush, lastWalApply, timeStamp, replyNow }) => ({
      type: 'XStatusUpdate' as const,
      lastWalWrite,
      lastWalFlush,
      lastWalApply,
      timeStamp,
      replyNow,
    })
  )
);

export type XLogData = ValueOf<ReturnType<typeof xLogData>>;
export type XKeepAlive = ValueOf<typeof xKeepAlive>;
export type XStatusUpdate = ValueOf<typeof xStatusUpdate>;

export type CopyDataTypes = XLogData | XKeepAlive | XStatusUpdate;

export type CopyDataParser = P.Parser<number, CopyDataTypes>;

const copyDataParsers: (length: number) => CopyDataParser[] = (length) => [
  xLogData(length),
  xKeepAlive,
  xStatusUpdate,
];

export const copyData = pipe(
  B.buffer('d'),
  P.chain(() => B.lenInt32BE()),
  P.bind('payload', (length) =>
    fixedLength(length)(oneOfArray(copyDataParsers(length)))
  ),
  P.map(({ payload }) => ({
    type: 'CopyData' as const,
    payload,
  }))
);

export const anyMessage = pipe(
  B.string()(1),
  P.bindTo('id'),
  P.bind('contentLength', () => B.lenInt32BE()),
  P.chainFirst(({ contentLength }) => minLeft(contentLength))
);

export const pgSSLRequestResponse = pipe(
  B.buffer('S'),
  P.alt(() => B.buffer('N')),
  P.map((code) => ({
    type: 'SSLRequestResponse' as const,
    useSSL: code.toString() === 'S',
  }))
);

export type AuthenticationOk = ValueOf<typeof authenticationOk>;
export type AuthenticationCleartextPassword = ValueOf<
  typeof authenticationCleartextPassword
>;
export type AuthenticationMD5Password = ValueOf<
  typeof authenticationMD5Password
>;
export type AuthenticationSASL = ValueOf<typeof authenticationSASL>;
export type AuthenticationSASLContinue = ValueOf<
  typeof authenticationSASLContinue
>;
export type AuthenticationSASLFinal = ValueOf<typeof authenticationSASLFinal>;
export type BackendKeyData = ValueOf<typeof backendKeyData>;
export type ParameterStatus = ValueOf<typeof parameterStatus>;
export type ReadyForQuery = ValueOf<typeof readyForQuery>;
export type ErrorResponse = ValueOf<typeof errorResponse>;
export type CommandComplete = ValueOf<typeof commandComplete>;
export type RowDescription = ValueOf<typeof rowDescription>;
export type DataRow = ValueOf<typeof dataRow>;
export type CopyBothResponse = ValueOf<typeof copyBothResponse>;
export type CopyDone = ValueOf<typeof copyDone>;
export type CopyFail = ValueOf<typeof copyFail>;
export type CopyData = ValueOf<typeof copyData>;
export type NoticeResponse = ValueOf<typeof noticeResponse>;
export type SSLRequestResponse = ValueOf<typeof pgSSLRequestResponse>;

export type PgServerMessageTypes =
  | AuthenticationOk
  | AuthenticationCleartextPassword
  | AuthenticationMD5Password
  | AuthenticationSASL
  | AuthenticationSASLContinue
  | AuthenticationSASLFinal
  | BackendKeyData
  | ParameterStatus
  | ReadyForQuery
  | ErrorResponse
  | CommandComplete
  | RowDescription
  | DataRow
  | CopyBothResponse
  | CopyDone
  | CopyFail
  | CopyData
  | NoticeResponse
  | SSLRequestResponse;

export type SSLRequest = ValueOf<typeof sslRequest>;
export type StartupMessage = ValueOf<typeof startupMessage>;
export type Query = ValueOf<typeof query>;
export type PasswordMessage = ValueOf<typeof passwordMessage>;
export type SASLInitialResponse = ValueOf<typeof saslInitialResponse>;
export type SASLResponse = ValueOf<typeof saslResponse>;

export type PgClientMessageTypes =
  | SSLRequest
  | StartupMessage
  | Query
  | PasswordMessage
  | CopyData
  | SASLInitialResponse
  | SASLResponse;

export type PgServerMessageParser = P.Parser<number, PgServerMessageTypes>;
export type PgClientMessageParser = P.Parser<number, PgClientMessageTypes>;

const pgServerMessageParsers: PgServerMessageParser[] = [
  authenticationOk,
  authenticationCleartextPassword,
  authenticationMD5Password,
  authenticationSASL,
  authenticationSASLContinue,
  authenticationSASLFinal,
  backendKeyData,
  parameterStatus,
  readyForQuery,
  errorResponse,
  commandComplete,
  rowDescription,
  dataRow,
  copyBothResponse,
  copyDone,
  copyFail,
  copyData,
  noticeResponse,
];

const pgClientMessageParsers: PgClientMessageParser[] = [
  saslInitialResponse,
  passwordMessage,
  saslResponse,
  query,
  copyData,
];

export const pgServerMessageParser = pipe(
  P.lookAhead(anyMessage),
  P.chain(() =>
    pipe(
      oneOfArray(pgServerMessageParsers),
      mapLeft((e) => ({
        ...e,
        fatal: true,
      }))
    )
  )
);

export const pgClientMessageParser = pipe(
  P.lookAhead(anyMessage),
  P.chain(() =>
    pipe(
      oneOfArray(pgClientMessageParsers),
      mapLeft((e) => ({
        ...e,
        fatal: true,
      }))
    )
  ),
  P.alt((): P.Parser<number, PgClientMessageTypes> => startupMessage),
  P.alt((): P.Parser<number, PgClientMessageTypes> => sslRequest)
);
