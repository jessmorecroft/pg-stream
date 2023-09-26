import { make as makePgClient } from './pg-client';
import { make as makePgPool } from './pg-pool';

export type {
  DecoratedBegin,
  DecoratedCommit,
  DecoratedDelete,
  DecoratedInsert,
  DecoratedRelation,
  DecoratedTruncate,
  DecoratedUpdate,
  PgOutputDecoratedMessageTypes,
} from './transform-log-data';

export { makePgClient, makePgPool };

export {
  PgClient,
  XLogProcessor,
  PgFailedAuth,
  PgParseError,
  PgServerError,
} from './pg-client';

export { PgPool } from './pg-pool';
