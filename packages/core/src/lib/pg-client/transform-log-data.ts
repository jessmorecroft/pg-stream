import {
  Delete,
  Insert,
  Relation,
  Truncate,
  TupleData,
  Update,
  PgOutputMessageTypes,
  getTypeName,
  makeValueTypeParser,
  ValueType,
  XLogData,
  Begin,
  Commit,
} from '../pg-protocol';
import * as P from 'parser-ts/Parser';
import * as S from 'parser-ts/string';
import { pipe } from 'fp-ts/function';
import * as O from 'fp-ts/Option';
import * as E from 'fp-ts/Either';
import { Data, Effect } from 'effect';

export type DecoratedRelation = Omit<Relation, 'columns'> & {
  columns: (Relation['columns'][number] & {
    dataTypeName: ReturnType<typeof getTypeName>;
  })[];
};

export type DecoratedBegin = Omit<Begin, 'timeStamp'> & {
  timeStamp: Date;
};

export type DecoratedCommit = Commit & {
  begin: DecoratedBegin;
};

export type DecoratedInsert = Omit<Insert, 'relationId' | 'newRecord'> & {
  namespace: string;
  name: string;
  begin: DecoratedBegin;
  newRecord: Record<string, ValueType | null>;
};

export type DecoratedUpdate = Omit<
  Update,
  'relationId' | 'newRecord' | 'oldRecord' | 'oldKey'
> & {
  namespace: string;
  name: string;
  begin: DecoratedBegin;
  oldKey?: Record<string, ValueType | null>;
  oldRecord?: Record<string, ValueType | null>;
  newRecord: Record<string, ValueType | null>;
};

export type DecoratedDelete = Omit<
  Delete,
  'relationId' | 'oldRecord' | 'oldKey'
> & {
  namespace: string;
  name: string;
  begin: DecoratedBegin;
  oldKey?: Record<string, ValueType | null>;
  oldRecord?: Record<string, ValueType | null>;
};

export type DecoratedTruncate = Omit<Truncate, 'relationIds'> & {
  relations: {
    namespace: string;
    name: string;
  }[];
};

export type PgOutputDecoratedMessageTypes =
  | Exclude<
      PgOutputMessageTypes,
      Begin | Commit | Relation | Insert | Update | Delete | Truncate
    >
  | DecoratedBegin
  | DecoratedCommit
  | DecoratedRelation
  | DecoratedInsert
  | DecoratedUpdate
  | DecoratedDelete
  | DecoratedTruncate;

type NamedParser = { name: string; parser: P.Parser<string, ValueType> };

export type TableInfoMap = Map<
  number,
  {
    namespace: string;
    name: string;
    colParsers: NamedParser[];
  }
>;

export class TableInfoNotFoundError extends Data.TaggedClass(
  'TableInfoNotFoundError'
)<{
  key: unknown;
}> {}

export class NoTransactionContextError extends Data.TaggedClass(
  'NoTransactionContexError'
)<{
  logData: XLogData;
}> {}

const convertTupleData = (tupleData: TupleData, parsers: NamedParser[]) =>
  tupleData.reduce((acc, item, index) => {
    const { name, parser } = parsers[index];

    const value = pipe(
      item,
      O.fold(
        () => undefined,
        (val) =>
          typeof val === 'string'
            ? pipe(
                S.run(val)(parser),
                E.fold(
                  () => val,
                  ({ value }) => value
                )
              )
            : val
      )
    );

    if (value !== undefined) {
      return { ...acc, [name]: value };
    }
    return acc;
  }, {} as Record<string, ValueType | null>);

export const transformLogData = (
  tableInfo: TableInfoMap,
  logData: XLogData,
  begin?: DecoratedBegin
): Effect.Effect<
  never,
  TableInfoNotFoundError | NoTransactionContextError,
  PgOutputDecoratedMessageTypes
> => {
  const xlog = logData.payload;
  switch (xlog.type) {
    case 'Begin': {
      const begin: DecoratedBegin = {
        ...xlog,
        timeStamp: new Date(
          Number(xlog.timeStamp / 1000n) + Date.UTC(2000, 0, 1)
        ),
      };
      return Effect.succeed(begin);
    }
    case 'Commit': {
      if (!begin) {
        return Effect.fail(new NoTransactionContextError({ logData }));
      }
      const commit: DecoratedCommit = {
        ...xlog,
        begin,
      };
      return Effect.succeed(commit);
    }
    case 'Relation': {
      const columns = xlog.columns.map((col) => {
        const dataTypeName = getTypeName(col.dataTypeId);
        return {
          ...col,
          dataTypeName,
        };
      });
      const colParsers = xlog.columns.map((col) => ({
        name: col.name,
        parser: makeValueTypeParser(col.dataTypeId),
      }));
      const { namespace, name } = xlog;
      tableInfo.set(xlog.id, {
        namespace,
        name,
        colParsers,
      });
      const relation: DecoratedRelation = { ...xlog, columns };
      return Effect.succeed(relation);
    }
    case 'Insert': {
      if (!begin) {
        return Effect.fail(new NoTransactionContextError({ logData }));
      }
      const { type, relationId } = xlog;
      const relation = tableInfo.get(relationId);
      if (!relation) {
        return Effect.fail(new TableInfoNotFoundError({ key: relationId }));
      }
      const { namespace, name, colParsers } = relation;
      const newRecord = convertTupleData(xlog.newRecord, colParsers);
      const insert: DecoratedInsert = {
        type,
        namespace,
        begin,
        name,
        newRecord,
      };

      return Effect.succeed(insert);
    }
    case 'Update': {
      if (!begin) {
        return Effect.fail(new NoTransactionContextError({ logData }));
      }
      const { type, relationId } = xlog;
      const relation = tableInfo.get(relationId);
      if (!relation) {
        return Effect.fail(new TableInfoNotFoundError({ key: relationId }));
      }
      const { namespace, name, colParsers } = relation;
      const newRecord = convertTupleData(xlog.newRecord, colParsers);
      const oldKey = pipe(
        xlog.oldKey,
        O.fold(
          () => undefined,
          (tuple) => convertTupleData(tuple, colParsers)
        )
      );
      const oldRecord = pipe(
        xlog.oldRecord,
        O.fold(
          () => undefined,
          (tuple) => convertTupleData(tuple, colParsers)
        )
      );
      const update: DecoratedUpdate = {
        type,
        namespace,
        name,
        begin,
        oldKey,
        oldRecord,
        newRecord,
      };
      return Effect.succeed(update);
    }
    case 'Delete': {
      if (!begin) {
        return Effect.fail(new NoTransactionContextError({ logData }));
      }
      const { type, relationId } = xlog;
      const relation = tableInfo.get(relationId);
      if (!relation) {
        return Effect.fail(new TableInfoNotFoundError({ key: relationId }));
      }
      const { namespace, name, colParsers } = relation;
      const oldKey = pipe(
        xlog.oldKey,
        O.fold(
          () => undefined,
          (tuple) => convertTupleData(tuple, colParsers)
        )
      );
      const oldRecord = pipe(
        xlog.oldRecord,
        O.fold(
          () => undefined,
          (tuple) => convertTupleData(tuple, colParsers)
        )
      );
      const destroy: DecoratedDelete = {
        type,
        namespace,
        name,
        begin,
        oldKey,
        oldRecord,
      };
      return Effect.succeed(destroy);
    }
    case 'Truncate': {
      const { type, options } = xlog;
      const notFound: number[] = [];
      const relations = xlog.relationIds.flatMap((relationId) => {
        const info = tableInfo.get(relationId);
        if (info) {
          const { namespace, name } = info;
          return [
            {
              namespace,
              name,
            },
          ];
        }
        notFound.push(relationId);
        return [];
      });
      if (notFound.length > 0) {
        return Effect.fail(
          new TableInfoNotFoundError({
            key: notFound,
          })
        );
      }
      const truncate: DecoratedTruncate = {
        type,
        relations,
        options,
      };
      return Effect.succeed(truncate);
    }
    default: {
      return Effect.succeed(xlog);
    }
  }
};
