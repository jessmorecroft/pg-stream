import assert from 'assert';
import {
  Query,
  RowDescription,
  DataRow,
  ReadyForQuery,
  BackendKeyData,
  ParameterStatus,
  ErrorResponse,
  NoticeResponse,
  CopyData,
  PgClientMessageTypes,
  StartupMessage,
  PasswordMessage,
  CopyBothResponse,
  SASLInitialResponse,
  SASLResponse,
  SSLRequest,
  PgServerMessageTypes,
  CommandComplete,
  CopyFail,
  AuthenticationSASL,
  AuthenticationSASLContinue,
  AuthenticationSASLFinal,
  AuthenticationMD5Password,
  SSLRequestResponse,
} from './message-parsers';
import { makePgOutputMessage } from './pgoutput';

type NoTag<T> = Omit<T, 'type'>;

export const makePgQuery = ({ sql }: NoTag<Query>) => {
  const buf = Buffer.allocUnsafe(1 + 4 + sql.length + 1);

  let offset = buf.write('Q');
  offset = buf.writeInt32BE(buf.length - 1, offset);
  offset += buf.write(sql, offset);
  offset = buf.writeInt8(0, offset);

  assert(offset === buf.length, `offset ${offset} != length ${buf.length}`);

  return buf;
};

export const makePgCommandComplete = ({
  commandTag,
}: NoTag<CommandComplete>) => {
  const buf = Buffer.allocUnsafe(1 + 4 + commandTag.length + 1);

  let offset = buf.write('C');
  offset = buf.writeInt32BE(buf.length - 1, offset);
  offset += buf.write(commandTag, offset);
  offset = buf.writeInt8(0, offset);

  assert(offset === buf.length, `offset ${offset} != length ${buf.length}`);

  return buf;
};

export const makePgCopyDone = () => {
  const buf = Buffer.allocUnsafe(1 + 4);

  let offset = buf.write('c');
  offset = buf.writeInt32BE(4, offset);

  assert(offset === buf.length, `offset ${offset} != length ${buf.length}`);

  return buf;
};

export const makePgCopyFail = ({ error }: NoTag<CopyFail>) => {
  const buf = Buffer.allocUnsafe(1 + 4 + error.length + 1);

  let offset = buf.write('f');
  offset = buf.writeInt32BE(buf.length - 1, offset);
  offset += buf.write(error, offset);
  offset = buf.writeInt8(0, offset);

  assert(offset === buf.length, `offset ${offset} != length ${buf.length}`);

  return buf;
};

export const makePgRowDescription = ({ fields }: NoTag<RowDescription>) => {
  const buf = Buffer.allocUnsafe(
    1 +
      4 +
      2 +
      fields.reduce(
        (acc, field) => acc + field.name.length + 1 + 4 + 2 + 4 + 2 + 4 + 2,
        0
      )
  );

  let offset = buf.write('T');
  offset = buf.writeInt32BE(buf.length - 1, offset);
  offset = buf.writeInt16BE(fields.length, offset);

  fields.forEach((field) => {
    offset += buf.write(field.name, offset);
    offset = buf.writeInt8(0, offset);
    offset = buf.writeInt32BE(field.tableId, offset);
    offset = buf.writeInt16BE(field.columnId, offset);
    offset = buf.writeInt32BE(field.dataTypeId, offset);
    offset = buf.writeInt16BE(field.dataTypeSize, offset);
    offset = buf.writeInt32BE(field.dataTypeModifier, offset);
    offset = buf.writeInt16BE(field.format, offset);
  });

  assert(offset === buf.length, `offset ${offset} != length ${buf.length}`);

  return buf;
};

export const makePgAuthenticateOk = () => {
  const buf = Buffer.allocUnsafe(1 + 4 + 4);

  let offset = buf.write('R');
  offset = buf.writeInt32BE(buf.length - 1, offset);
  offset = buf.writeInt32BE(0, offset);

  assert(offset === buf.length, `offset ${offset} != length ${buf.length}`);

  return buf;
};

export const makePgAuthenticateCleartextPassword = () => {
  const buf = Buffer.allocUnsafe(1 + 4 + 4);

  let offset = buf.write('R');
  offset = buf.writeInt32BE(buf.length - 1, offset);
  offset = buf.writeInt32BE(3, offset);

  assert(offset === buf.length, `offset ${offset} != length ${buf.length}`);

  return buf;
};

export const makePgBackendKeyData = ({
  pid,
  secretKey,
}: NoTag<BackendKeyData>) => {
  const buf = Buffer.allocUnsafe(1 + 4 + 4 + 4);

  let offset = buf.write('K');
  offset = buf.writeInt32BE(buf.length - 1, offset);
  offset = buf.writeInt32BE(pid, offset);
  offset = buf.writeInt32BE(secretKey, offset);

  assert(offset === buf.length, `offset ${offset} != length ${buf.length}`);

  return buf;
};

export const makePgParameterStatus = ({
  name,
  value,
}: NoTag<ParameterStatus>) => {
  const buf = Buffer.allocUnsafe(1 + 4 + name.length + 1 + value.length + 1);

  let offset = buf.write('S');
  offset = buf.writeInt32BE(buf.length - 1, offset);
  offset += buf.write(name, offset);
  offset = buf.writeInt8(0, offset);
  offset += buf.write(value, offset);
  offset = buf.writeInt8(0, offset);

  assert(offset === buf.length, `offset ${offset} != length ${buf.length}`);

  return buf;
};

export const makePgErrorResponse = ({ errors }: NoTag<ErrorResponse>) => {
  const buf = Buffer.allocUnsafe(
    1 +
      4 +
      errors.reduce((acc, error) => acc + 1 + error.value.length + 1, 0) +
      1
  );

  let offset = buf.write('E');
  offset = buf.writeInt32BE(buf.length - 1, offset);

  errors.forEach(({ type, value }) => {
    offset += buf.write(type.length === 1 ? type : '?', offset);
    offset += buf.write(value, offset);
    offset = buf.writeInt8(0, offset);
  });
  offset = buf.writeInt8(0, offset);

  assert(offset === buf.length, `offset ${offset} != length ${buf.length}`);

  return buf;
};

export const makePgNoticeResponse = ({ notices }: NoTag<NoticeResponse>) => {
  const buf = Buffer.allocUnsafe(
    1 +
      4 +
      notices.reduce((acc, notice) => acc + 1 + notice.value.length + 1, 0) +
      1
  );

  let offset = buf.write('N');
  offset = buf.writeInt32BE(buf.length - 1, offset);

  notices.forEach(({ type, value }) => {
    offset += buf.write(type.length === 1 ? type : '?', offset);
    offset += buf.write(value, offset);
    offset = buf.writeInt8(0, offset);
  });
  offset = buf.writeInt8(0, offset);

  assert(offset === buf.length, `offset ${offset} != length ${buf.length}`);

  return buf;
};

export const makePgDataRow = ({ values }: NoTag<DataRow>) => {
  const buf = Buffer.allocUnsafe(
    1 + 4 + 2 + values.reduce((acc, value) => acc + 4 + (value?.length ?? 0), 0)
  );

  let offset = buf.write('D');
  offset = buf.writeInt32BE(buf.length - 1, offset);
  offset = buf.writeInt16BE(values.length, offset);

  values.forEach((value) => {
    offset = buf.writeInt32BE(value?.length ?? -1, offset);
    offset += buf.write(value ?? '', offset);
  });

  assert(offset === buf.length, `offset ${offset} != length ${buf.length}`);

  return buf;
};

export const makePgReadyForQuery = ({
  transactionStatus,
}: NoTag<ReadyForQuery>) => {
  const buf = Buffer.allocUnsafe(1 + 4 + 1);

  let offset = buf.write('Z');
  offset = buf.writeInt32BE(buf.length - 1, offset);
  offset += buf.write(transactionStatus, offset);

  assert(offset === buf.length, `offset ${offset} != length ${buf.length}`);

  return buf;
};

export const makePgCopyBothResponse = ({
  binary,
  columnBinary,
}: NoTag<CopyBothResponse>) => {
  const buf = Buffer.allocUnsafe(1 + 4 + 1 + 2 + columnBinary.length);

  let offset = buf.write('W');
  offset = buf.writeInt32BE(buf.length - 1, offset);
  offset = buf.writeInt8(binary ? 1 : 0, offset);
  offset = buf.writeInt16BE(columnBinary.length, offset);

  columnBinary.forEach((value) => {
    offset = buf.writeInt8(value ? 1 : 0, offset);
  });

  assert(offset === buf.length, `offset ${offset} != length ${buf.length}`);

  return buf;
};

export const makePgCopyData = (copyData: NoTag<CopyData>) => {
  const { payload } = copyData;
  let buf2: Buffer;
  switch (payload.type) {
    case 'XLogData': {
      const { walStart, walEnd, timeStamp, payload: innerPayload } = payload;

      const headerBuf = Buffer.allocUnsafe(1 + 8 + 8 + 8);
      let offset = headerBuf.write('w');
      offset = headerBuf.writeBigInt64BE(walStart, offset);
      offset = headerBuf.writeBigInt64BE(walEnd, offset);
      offset = headerBuf.writeBigInt64BE(timeStamp, offset);
      assert(
        offset === headerBuf.length,
        `offset ${offset} != length ${headerBuf.length}`
      );
      const payloadBuf = makePgOutputMessage(innerPayload);
      buf2 = Buffer.concat([headerBuf, payloadBuf]);
      break;
    }
    case 'XKeepAlive': {
      const { walEnd, timeStamp, replyNow } = payload;
      buf2 = Buffer.allocUnsafe(1 + 8 + 8 + 1);
      let offset = buf2.write('k');
      offset = buf2.writeBigInt64BE(walEnd, offset);
      offset = buf2.writeBigInt64BE(timeStamp, offset);
      offset = buf2.writeInt8(replyNow ? 1 : 0, offset);
      assert(
        offset === buf2.length,
        `offset ${offset} != length ${buf2.length}`
      );
      break;
    }
    case 'XStatusUpdate': {
      const { lastWalWrite, lastWalFlush, lastWalApply, timeStamp, replyNow } =
        payload;
      buf2 = Buffer.allocUnsafe(1 + 8 + 8 + 8 + 8 + 1);
      let offset = buf2.write('r');
      offset = buf2.writeBigInt64BE(lastWalWrite, offset);
      offset = buf2.writeBigInt64BE(lastWalFlush, offset);
      offset = buf2.writeBigInt64BE(lastWalApply, offset);
      offset = buf2.writeBigInt64BE(timeStamp, offset);
      offset = buf2.writeInt8(replyNow ? 1 : 0, offset);
      assert(
        offset === buf2.length,
        `offset ${offset} != length ${buf2.length}`
      );
      break;
    }
  }

  const buf1 = Buffer.allocUnsafe(1 + 4);

  const offset = buf1.write('d');
  buf1.writeInt32BE(buf1.length + buf2.length - 1, offset);

  return Buffer.concat([buf1, buf2]);
};

export const makePgStartupMessage = ({
  protocolVersion,
  parameters,
}: NoTag<StartupMessage>) => {
  const buf = Buffer.allocUnsafe(
    4 +
      4 +
      parameters.reduce(
        (acc, { name, value }) => acc + name.length + 1 + value.length + 1,
        0
      ) +
      1
  );

  let offset = buf.writeInt32BE(buf.length);
  offset = buf.writeInt32BE(protocolVersion, offset);

  parameters.forEach(({ name, value }) => {
    offset += buf.write(name, offset);
    offset = buf.writeInt8(0, offset);
    offset += buf.write(value, offset);
    offset = buf.writeInt8(0, offset);
  });

  offset = buf.writeInt8(0, offset);

  assert(offset === buf.length, `offset ${offset} != length ${buf.length}`);

  return buf;
};

export const makePgPasswordMessage = ({ password }: NoTag<PasswordMessage>) => {
  const buf = Buffer.allocUnsafe(1 + 4 + password.length + 1);

  let offset = buf.write('p');
  offset = buf.writeInt32BE(buf.length - 1, offset);
  offset += buf.write(password, offset);
  offset = buf.writeInt8(0, offset);

  assert(offset === buf.length, `offset ${offset} != length ${buf.length}`);

  return buf;
};

export const makePgSaslInitialResponse = ({
  clientFirstMessage,
  mechanism,
}: NoTag<SASLInitialResponse>) => {
  const buf = Buffer.allocUnsafe(
    1 + 4 + mechanism.length + 1 + 4 + clientFirstMessage.length
  );

  let offset = buf.write('p');
  offset = buf.writeInt32BE(buf.length - 1, offset);
  offset += buf.write(mechanism, offset);
  offset = buf.writeInt8(0, offset);
  offset = buf.writeInt32BE(clientFirstMessage.length, offset);
  offset += buf.write(clientFirstMessage, offset);

  assert(offset === buf.length, `offset ${offset} != length ${buf.length}`);

  return buf;
};

export const makePgSaslResponse = ({
  clientFinalMessage,
}: NoTag<SASLResponse>) => {
  const buf = Buffer.allocUnsafe(1 + 4 + clientFinalMessage.length);

  let offset = buf.write('p');
  offset = buf.writeInt32BE(buf.length - 1, offset);
  offset += buf.write(clientFinalMessage, offset);

  assert(offset === buf.length, `offset ${offset} != length ${buf.length}`);

  return buf;
};

export const makePgSSLRequest = ({ requestCode }: NoTag<SSLRequest>) => {
  const buf = Buffer.allocUnsafe(4 + 4);

  let offset = buf.writeInt32BE(8);
  offset = buf.writeInt32BE(requestCode, offset);

  assert(offset === buf.length, `offset ${offset} != length ${buf.length}`);

  return buf;
};

export const makePgSSLRequestResponse = ({
  useSSL,
}: NoTag<SSLRequestResponse>) => {
  const buf = Buffer.allocUnsafe(1);

  if (useSSL) {
    buf.write('S');
  } else {
    buf.write('N');
  }

  return buf;
};

export const makePgClientMessage = (message: PgClientMessageTypes): Buffer => {
  switch (message.type) {
    case 'SSLRequest':
      return makePgSSLRequest(message);
    case 'StartupMessage':
      return makePgStartupMessage(message);
    case 'Query':
      return makePgQuery(message);
    case 'PasswordMessage':
      return makePgPasswordMessage(message);
    case 'CopyData':
      return makePgCopyData(message);
    case 'SASLInitialResponse':
      return makePgSaslInitialResponse(message);
    case 'SASLResponse':
      return makePgSaslResponse(message);
  }
};

export type PgTestServerMessageTypes = Exclude<
  PgServerMessageTypes,
  | AuthenticationSASL
  | AuthenticationSASLContinue
  | AuthenticationSASLFinal
  | AuthenticationMD5Password
>;

export const makePgServerMessage = (
  message: PgTestServerMessageTypes
): Buffer => {
  switch (message.type) {
    case 'ReadyForQuery':
      return makePgReadyForQuery(message);
    case 'AuthenticationCleartextPassword':
      return makePgAuthenticateCleartextPassword();
    case 'AuthenticationOk':
      return makePgAuthenticateOk();
    case 'BackendKeyData':
      return makePgBackendKeyData(message);
    case 'CopyBothResponse':
      return makePgCopyBothResponse(message);
    case 'CopyData':
      return makePgCopyData(message);
    case 'DataRow':
      return makePgDataRow(message);
    case 'ErrorResponse':
      return makePgErrorResponse(message);
    case 'NoticeResponse':
      return makePgNoticeResponse(message);
    case 'RowDescription':
      return makePgRowDescription(message);
    case 'ParameterStatus':
      return makePgParameterStatus(message);
    case 'CommandComplete':
      return makePgCommandComplete(message);
    case 'CopyDone':
      return makePgCopyDone();
    case 'CopyFail':
      return makePgCopyFail(message);
    case 'SSLRequestResponse':
      return makePgSSLRequestResponse(message);
  }
};
