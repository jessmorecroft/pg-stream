/* eslint-disable @typescript-eslint/no-explicit-any */
import {
  Chunk,
  Deferred,
  Effect,
  Either,
  GroupBy,
  Option,
  Sink,
  Stream,
  pipe,
} from "effect";
import * as P from "parser-ts/Parser";
import {
  ALL_ENABLED_PARSER_OPTIONS,
  CopyData,
  CopyDone,
  MakeValueTypeParserOptions,
  XKeepAlive,
  XLogData,
  makePgClientMessage,
} from "../pg-protocol";
import {
  NoMoreMessagesError,
  ParseMessageError,
  ParseMessageGroupError,
  ReadableError,
  WritableError,
  hasTypeOf,
} from "../stream";
import { Duplex } from "stream";
import {
  XLogProcessor,
  XLogProcessorError,
  isCommandComplete,
  isReadyForQuery,
  item,
  read,
  readOrFail,
  readUntilReady,
  write,
  PgServerError,
} from "./util";
import * as Schema from "@effect/schema/Schema";
import {
  DecoratedBegin,
  NoTransactionContextError,
  PgOutputDecoratedMessageTypes,
  TableInfoMap,
  TableInfoNotFoundError,
  transformLogData,
} from "./transform-log-data";
import * as stream from "../stream";
import { walLsnFromString } from "../util/schemas";

const isCopyData = hasTypeOf("CopyData");

export const recvlogical =
  (socket: Duplex) =>
  <E, T extends PgOutputDecoratedMessageTypes>({
    slotName,
    publicationNames,
    processor,
    parserOptions,
    signal,
  }: {
    slotName: string;
    publicationNames: string[];
    processor: XLogProcessor<E, T>;
    parserOptions?: MakeValueTypeParserOptions;
    signal?: Deferred.Deferred<void>;
  }): Effect.Effect<
    void,
    | ReadableError
    | WritableError
    | ParseMessageError
    | ParseMessageGroupError
    | NoMoreMessagesError
    | PgServerError
    | stream.UnexpectedMessageError
    | TableInfoNotFoundError
    | NoTransactionContextError
    | XLogProcessorError<E>
  > =>
    Effect.gen(function* (_) {
      yield* _(
        write(socket)({
          type: "Query",
          sql: `START_REPLICATION SLOT ${slotName} LOGICAL ${Schema.encodeSync(
            walLsnFromString,
          )(0n)} (proto_version '1', publication_names '${publicationNames.join(
            ",",
          )}')`,
        }),
      );

      // wait for this before streaming
      yield* _(readOrFail(socket)("CopyBothResponse"));

      const processedLsns: [bigint, boolean][] = [];

      const [logData, keepalives] = yield* _(
        stream
          .readStream(read(socket), (e) => e._tag === "NoMoreMessagesError")
          .pipe(
            Stream.takeUntil((_) => _.type === "CopyDone"),
            Stream.filter(isCopyData),
            Stream.tap((msg) => {
              if (
                msg.payload.type === "XLogData" &&
                msg.payload.walEnd !== 0n
              ) {
                processedLsns.push([msg.payload.walEnd, false]);
              } else if (msg.payload.type === "XKeepAlive") {
                processedLsns.push([msg.payload.walEnd, true]);
              }
              return Effect.unit;
            }),
            Stream.partitionEither(({ payload }) => {
              return hasTypeOf("XLogData")(payload)
                ? Effect.succeed(Either.left(payload))
                : Effect.succeed(Either.right(payload));
            }),
          ),
      );

      const [filtered, skipped] = yield* _(
        logData.pipe(
          Stream.mapAccumEffect(
            [new Map(), undefined],
            ([map, begin]: [TableInfoMap, DecoratedBegin | undefined], log) =>
              transformLogData(
                map,
                log,
                begin,
                parserOptions ?? ALL_ENABLED_PARSER_OPTIONS,
              ).pipe(
                Effect.map(
                  (
                    msg,
                  ): [
                    [TableInfoMap, DecoratedBegin | undefined],
                    [PgOutputDecoratedMessageTypes, XLogData],
                  ] => [
                    [
                      map,
                      msg.type === "Begin"
                        ? msg
                        : msg.type !== "Commit"
                          ? begin
                          : undefined,
                    ],
                    [msg, log],
                  ],
                ),
              ),
          ),
          Stream.partitionEither(
            (
              msgAndLog,
            ): Effect.Effect<Either.Either<XLogData, [T, XLogData]>> =>
              processor.filter(msgAndLog[0])
                ? Effect.succeed(Either.left([msgAndLog[0], msgAndLog[1]]))
                : Effect.succeed(Either.right(msgAndLog[1])),
          ),
        ),
      );

      const dataStream = filtered.pipe(
        Stream.bufferChunks({ capacity: 16 }),
        Stream.groupByKey(([msg]) => {
          if (processor.key) {
            if (processor.key === "serial") {
              return "";
            }
            if (processor.key === "table") {
              return "namespace" in msg ? `${msg.namespace}.${msg.name}` : "";
            }
            return processor.key(msg);
          }
          return "";
        }),
        GroupBy.evaluate((key, stream) =>
          stream.pipe(
            Stream.mapChunksEffect((chunk) =>
              Effect.map(
                Effect.mapError(
                  processor.process(
                    key,
                    Chunk.map(chunk, ([msg]) => msg),
                  ),
                  (cause) => new XLogProcessorError<E>({ cause }),
                ),
                () => Chunk.map(chunk, ([, log]) => log.walEnd),
              ),
            ),
          ),
        ),
        Stream.merge(Stream.map(skipped, (log) => log.walEnd)),
        Stream.mapChunks((chunk) => {
          Chunk.forEach(chunk, (wal) => {
            const item = processedLsns.find((_) => _[0] === wal && !_[1]);
            if (item) {
              item[1] = true;
            }
          });
          return Chunk.of<void>(undefined);
        }),
      );

      const ticks = keepalives.pipe(
        Stream.filter(
          (msg): msg is XKeepAlive => msg.type === "XKeepAlive" && msg.replyNow,
        ),
        Stream.merge(Stream.tick("10 seconds")),
      );

      const source = Stream.merge(dataStream, ticks, {
        haltStrategy: "left",
      }).pipe(
        Stream.flatMap((keepalive) => {
          let next: bigint | undefined;
          while (processedLsns.length > 0) {
            if (processedLsns[0][1]) {
              next = processedLsns.shift()?.[0];
            } else {
              break;
            }
          }
          if (next) {
            return Stream.succeed(next);
          }
          if (keepalive) {
            return Stream.succeed(0n);
          }
          return Stream.empty;
        }),
        Stream.scan(0n, (s, a) => (a > s ? a : s)),
        Stream.map((lsn): CopyData => {
          const timeStamp =
            BigInt(new Date().getTime() - Date.UTC(2000, 0, 1)) * 1000n;

          return {
            type: "CopyData",
            payload: {
              type: "XStatusUpdate",
              lastWalWrite: lsn,
              lastWalFlush: lsn,
              lastWalApply: 0n,
              replyNow: false,
              timeStamp,
            },
          };
        }),
      );

      const bufferPush = stream.toSinkable(stream.push(socket));

      const push = (input: Option.Option<Chunk.Chunk<CopyData | CopyDone>>) => {
        if (Option.isNone(input)) {
          return bufferPush(Option.none());
        }

        const chunk = input.value;

        const [head, tail] = Chunk.splitWhere(
          chunk,
          (_) => _.type === "CopyDone",
        );

        if (Chunk.isNonEmpty(tail)) {
          return bufferPush(
            Option.some(
              Chunk.map(
                Chunk.append(head, Chunk.headNonEmpty(tail)),
                makePgClientMessage,
              ),
            ),
          ).pipe(
            Effect.flatMap(() =>
              Effect.fail([
                Either.right<void>(undefined),
                Chunk.empty<never>(),
              ] as const),
            ),
          );
        }

        return bufferPush(Option.some(Chunk.map(chunk, makePgClientMessage)));
      };

      const writeSink = Sink.fromPush(Effect.succeed(push));

      yield* _(
        Stream.run(
          Stream.merge(
            source,
            Stream.fromEffect(
              (signal ? Deferred.await(signal) : Effect.never).pipe(
                Effect.map((): CopyDone => ({ type: "CopyDone" })),
              ),
            ),
          ),
          Sink.flatMap(writeSink, () => Sink.drain),
        ),
      );

      const { commandTags } = yield* _(
        readUntilReady(socket)(
          pipe(
            item,
            P.filter(isCopyData),
            P.many,
            P.chain(() =>
              pipe(
                item,
                P.filter(isCommandComplete),
                P.map(({ commandTag }) => commandTag),
                P.many,
                P.bindTo("commandTags"),
                P.chainFirst(() =>
                  pipe(
                    item,
                    P.filter(isReadyForQuery),
                    P.chain(() => P.eof()),
                  ),
                ),
              ),
            ),
          ),
        ),
      );

      yield* _(Effect.forEach(commandTags, (_) => Effect.log(_)));
    }).pipe(Effect.scoped);
