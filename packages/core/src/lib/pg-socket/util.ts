import { ErrorResponse, NoticeResponse } from '../pg-protocol';
import { BinaryLike, createHash, createHmac } from 'crypto';
import { Effect } from 'effect';
import * as _ from 'lodash';

export const sha256 = (text: BinaryLike) => {
  return createHash('sha256').update(text).digest();
};

export const hmacSha256 = (key: BinaryLike, msg: BinaryLike) => {
  return createHmac('sha256', key).update(msg).digest();
};

export const xorBuffers = (a: Buffer, b: Buffer) =>
  Buffer.from(a.map((_, i) => a[i] ^ b[i]));

export const Hi = (password: string, saltBytes: Buffer, iterations: number) => {
  let ui1 = hmacSha256(
    password,
    Buffer.concat([saltBytes, Buffer.from([0, 0, 0, 1])])
  );

  let ui = ui1;
  _.times(iterations - 1, () => {
    ui1 = hmacSha256(password, ui1);
    ui = xorBuffers(ui, ui1);
  });

  return ui;
};

export const logBackendMessage = (message: NoticeResponse | ErrorResponse) => {
  const pairs =
    message.type === 'NoticeResponse' ? message.notices : message.errors;

  const { log, fields, msg } = pairs.reduce(
    ({ log, fields, msg }, { type, value }) => {
      switch (type) {
        case 'V': {
          const log2 =
            {
              ERROR: Effect.logError,
              FATAL: Effect.logFatal,
              PANIC: Effect.logFatal,
              WARNING: Effect.logWarning,
              NOTICE: Effect.logInfo,
              DEBUG: Effect.logDebug,
              INFO: Effect.logInfo,
              LOG: Effect.log,
            }[value] ?? log;
          return { log: log2, fields, msg };
        }
        case 'C': {
          return { log, fields: { ...fields, code: value }, msg };
        }
        case 'M': {
          return { log, fields, msg: `POSTGRES: ${value}` };
        }
        case 'D': {
          return { log, fields: { ...fields, detail: value }, msg };
        }
        case 'H': {
          return { log, fields: { ...fields, advice: value }, msg };
        }
        case 'P': {
          return { log, fields: { ...fields, queryPosition: value }, msg };
        }
        case 'p': {
          return {
            log,
            fields: { ...fields, internalQueryPosition: value },
            msg,
          };
        }
        case 'q': {
          return { log, fields: { ...fields, internalQuery: value }, msg };
        }
        case 'W': {
          return { log, fields: { ...fields, context: value }, msg };
        }
        case 's': {
          return { log, fields: { ...fields, schema: value }, msg };
        }
        case 't': {
          return { log, fields: { ...fields, table: value }, msg };
        }
        case 'c': {
          return { log, fields: { ...fields, column: value }, msg };
        }
        case 'd': {
          return { log, fields: { ...fields, dataType: value }, msg };
        }
        case 'n': {
          return { log, fields: { ...fields, constraint: value }, msg };
        }
        case 'F': {
          return { log, fields: { ...fields, file: value }, msg };
        }
        case 'L': {
          return { log, fields: { ...fields, line: value }, msg };
        }
        case 'R': {
          return { log, fields: { ...fields, routine: value }, msg };
        }
      }
      return { log, fields, msg };
    },
    {
      log: Effect.log,
      fields: {},
      msg: 'Message received from backend.',
    }
  );

  return log(JSON.stringify({ msg, fields }));
};
