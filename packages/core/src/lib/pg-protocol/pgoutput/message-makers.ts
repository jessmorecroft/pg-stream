import * as assert from 'assert';
import {
  Begin,
  Commit,
  Delete,
  Insert,
  Message,
  Origin,
  PgOutputMessageTypes,
  Relation,
  Truncate,
  Type,
  Update,
} from './message-parsers';
import * as O from 'fp-ts/Option';
import { pipe } from 'fp-ts/lib/function';

type NoTag<T> = Omit<T, 'type'>;

export const makePgOutputBegin = ({
  finalLsn,
  timeStamp,
  tranId,
}: NoTag<Begin>) => {
  const buf = Buffer.allocUnsafe(1 + 8 + 8 + 4);

  let offset = buf.write('B');
  offset = buf.writeBigInt64BE(finalLsn, offset);
  offset = buf.writeBigInt64BE(timeStamp, offset);
  offset = buf.writeInt32BE(tranId, offset);

  assert(offset === buf.length, `offset ${offset} != length ${buf.length}`);

  return buf;
};

export const makePgOutputDecodingMessage = ({
  isTransactional,
  lsn,
  prefix,
  content,
}: NoTag<Message>) => {
  const buf = Buffer.allocUnsafe(
    1 + 1 + 8 + prefix.length + 1 + 4 + content.length
  );

  let offset = buf.write('M');
  offset = buf.writeInt8(isTransactional ? 1 : 0, offset);
  offset = buf.writeBigInt64BE(lsn, offset);
  offset += buf.write(prefix, offset);
  offset = buf.writeInt8(0, offset);
  offset = buf.writeInt32BE(content.length, offset);
  offset += buf.write(content, offset);

  assert(offset === buf.length, `offset ${offset} != length ${buf.length}`);

  return buf;
};

export const makePgOutputCommit = ({
  lsn,
  endLsn,
  timeStamp,
}: NoTag<Commit>) => {
  const buf = Buffer.allocUnsafe(1 + 1 + 8 + 8 + 8);

  let offset = buf.write('C');
  offset = buf.writeInt8(0, offset);
  offset = buf.writeBigInt64BE(lsn, offset);
  offset = buf.writeBigInt64BE(endLsn, offset);
  offset = buf.writeBigInt64BE(timeStamp, offset);

  assert(offset === buf.length, `offset ${offset} != length ${buf.length}`);

  return buf;
};

export const makePgOutputOrigin = ({ lsn, name }: NoTag<Origin>) => {
  const buf = Buffer.allocUnsafe(1 + 8 + name.length + 1);

  let offset = buf.write('O');
  offset = buf.writeBigInt64BE(lsn, offset);
  offset += buf.write(name, offset);
  offset = buf.writeInt8(0, offset);

  assert(offset === buf.length, `offset ${offset} != length ${buf.length}`);

  return buf;
};

export const makePgOutputRelation = ({
  id,
  namespace,
  name,
  replIdent,
  columns,
}: NoTag<Relation>) => {
  const buf = Buffer.allocUnsafe(
    1 +
      4 +
      namespace.length +
      1 +
      name.length +
      1 +
      1 +
      2 +
      columns.reduce((acc, value) => acc + 1 + value.name.length + 1 + 4 + 4, 0)
  );

  let offset = buf.write('R');
  offset = buf.writeInt32BE(id, offset);
  offset += buf.write(namespace, offset);
  offset = buf.writeInt8(0, offset);
  offset += buf.write(name, offset);
  offset = buf.writeInt8(0, offset);
  offset += buf.write(replIdent, offset);
  offset = buf.writeInt16BE(columns.length, offset);

  columns.forEach(({ isKey, name, dataTypeId, typeMod }) => {
    offset = buf.writeInt8(isKey ? 1 : 0, offset);
    offset += buf.write(name, offset);
    offset = buf.writeInt8(0, offset);
    offset = buf.writeInt32BE(dataTypeId, offset);
    offset = buf.writeInt32BE(typeMod, offset);
  });

  assert(offset === buf.length, `offset ${offset} != length ${buf.length}`);

  return buf;
};

export const makePgOutputType = ({ id, namespace, name }: NoTag<Type>) => {
  const buf = Buffer.allocUnsafe(
    1 + 4 + namespace.length + 1 + name.length + 1
  );

  let offset = buf.write('Y');
  offset = buf.writeInt32BE(id, offset);
  offset += buf.write(namespace, offset);
  offset = buf.writeInt8(0, offset);
  offset += buf.write(name, offset);
  offset = buf.writeInt8(0, offset);

  assert(offset === buf.length, `offset ${offset} != length ${buf.length}`);

  return buf;
};

const makePgOutputTupleData = (values: Insert['newRecord']) => {
  const buf = Buffer.allocUnsafe(
    2 +
      values.reduce(
        (acc, value) =>
          acc +
          1 +
          pipe(
            value,
            O.fold(
              () => 0,
              (v) => (v === null ? 0 : 4 + v.length)
            )
          ),
        0
      )
  );

  let offset = buf.writeInt16BE(values.length);

  values.forEach((value) => {
    pipe(
      value,
      O.fold(
        () => {
          offset += buf.write('u', offset);
        },
        (item) => {
          if (item === null) {
            offset += buf.write('n', offset);
          } else if (typeof item === 'string') {
            offset += buf.write('t', offset);
            offset = buf.writeInt32BE(item.length, offset);
            offset += buf.write(item, offset);
          } else {
            offset += buf.write('b', offset);
            offset = buf.writeInt32BE(item.length, offset);
            offset += item.copy(buf, offset);
          }
        }
      )
    );
  });

  assert(offset === buf.length, `offset ${offset} != length ${buf.length}`);

  return buf;
};

export const makePgOutputInsert = ({
  relationId,
  newRecord,
}: NoTag<Insert>) => {
  const buf1 = Buffer.allocUnsafe(1 + 4 + 1);

  let offset = buf1.write('I');
  offset = buf1.writeInt32BE(relationId, offset);
  offset += buf1.write('N', offset);

  assert(offset === buf1.length, `offset ${offset} != length ${buf1.length}`);

  const buf2 = makePgOutputTupleData(newRecord);

  return Buffer.concat([buf1, buf2]);
};

export const makePgOutputUpdate = ({
  relationId,
  oldKey,
  oldRecord,
  newRecord,
}: NoTag<Update>) => {
  const buf1 = Buffer.allocUnsafe(1 + 4);
  const offset = buf1.write('U');
  buf1.writeInt32BE(relationId, offset);

  const buf2 = pipe(
    oldKey,
    O.fold(
      () => Buffer.from([]),
      (data) => {
        const header = Buffer.allocUnsafe(1);
        header.write('K');
        const tupleBuf = makePgOutputTupleData(data);
        return Buffer.concat([header, tupleBuf]);
      }
    )
  );

  const buf3 = pipe(
    oldRecord,
    O.fold(
      () => Buffer.from([]),
      (data) => {
        const header = Buffer.allocUnsafe(1);
        header.write('O');
        const tupleBuf = makePgOutputTupleData(data);
        return Buffer.concat([header, tupleBuf]);
      }
    )
  );

  const buf4 = Buffer.allocUnsafe(1);
  buf4.write('N');
  const buf5 = makePgOutputTupleData(newRecord);

  return Buffer.concat([buf1, buf2, buf3, buf4, buf5]);
};

export const makePgOutputDelete = ({
  relationId,
  oldKey,
  oldRecord,
}: NoTag<Delete>) => {
  const buf1 = Buffer.allocUnsafe(1 + 4);
  const offset = buf1.write('D');
  buf1.writeInt32BE(relationId, offset);

  const buf2 = pipe(
    oldKey,
    O.fold(
      () => Buffer.from([]),
      (data) => {
        const header = Buffer.allocUnsafe(1);
        header.write('K');
        const tupleBuf = makePgOutputTupleData(data);
        return Buffer.concat([header, tupleBuf]);
      }
    )
  );

  const buf3 = pipe(
    oldRecord,
    O.fold(
      () => Buffer.from([]),
      (data) => {
        const header = Buffer.allocUnsafe(1);
        header.write('O');
        const tupleBuf = makePgOutputTupleData(data);
        return Buffer.concat([header, tupleBuf]);
      }
    )
  );

  return Buffer.concat([buf1, buf2, buf3]);
};

export const makePgOutputTruncate = ({
  options,
  relationIds,
}: NoTag<Truncate>) => {
  const buf = Buffer.allocUnsafe(1 + 4 + 1 + relationIds.length * 4);

  let offset = buf.write('T');
  offset = buf.writeInt32BE(relationIds.length, offset);
  offset = buf.writeInt8(options, offset);

  relationIds.forEach((relationId) => {
    offset = buf.writeInt32BE(relationId, offset);
  });

  assert(offset === buf.length, `offset ${offset} != length ${buf.length}`);

  return buf;
};

export const makePgOutputMessage = (message: PgOutputMessageTypes): Buffer => {
  switch (message.type) {
    case 'Begin':
      return makePgOutputBegin(message);
    case 'Message':
      return makePgOutputDecodingMessage(message);
    case 'Commit':
      return makePgOutputCommit(message);
    case 'Origin':
      return makePgOutputOrigin(message);
    case 'Relation':
      return makePgOutputRelation(message);
    case 'Type':
      return makePgOutputType(message);
    case 'Insert':
      return makePgOutputInsert(message);
    case 'Update':
      return makePgOutputUpdate(message);
    case 'Delete':
      return makePgOutputDelete(message);
    case 'Truncate':
      return makePgOutputTruncate(message);
  }
};
