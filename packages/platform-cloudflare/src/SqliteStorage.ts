import {
  OutboxRecord,
  SourceStorage,
  StorageError,
  StoredEvent,
  StoredHead,
  StoredReceipt,
  type ReceiptIdentity,
  type SourceTransaction,
  type StorageCommit,
} from "@yielded/sync/server";
import { Effect, Layer, Schema } from "effect";
import { DurableObjectSqlite, DurableObjectState } from "effect-cf";
import { SqlClient } from "effect/unstable/sql";

export interface Options {
  readonly namespace: string;
  readonly replayWindow: number;
  readonly maxPendingOutbox: number;
}

const failure = (cause: unknown) =>
  StorageError.make({ message: "Authoritative SQLite operation failed", cause });

const Row = Schema.Struct({ value: Schema.String });

const fromRow = <S extends Schema.Codec<unknown, unknown>>(schema: S, row: unknown) =>
  Schema.decodeUnknownEffect(Row)(row).pipe(
    Effect.flatMap(({ value }) => Schema.decodeEffect(Schema.fromJsonString(schema))(value)),
    Effect.mapError(failure),
  );

const toJson = <S extends Schema.Codec<unknown, unknown>>(schema: S, value: S["Type"]) =>
  Schema.encodeEffect(Schema.fromJsonString(schema))(value).pipe(Effect.mapError(failure));

export const layer = (options: Options) =>
  Layer.effect(
    SourceStorage,
    Effect.gen(function* () {
      if (
        options.namespace.length === 0 ||
        !Number.isSafeInteger(options.replayWindow) ||
        options.replayWindow <= 0 ||
        !Number.isSafeInteger(options.maxPendingOutbox) ||
        options.maxPendingOutbox < 0
      )
        return yield* Effect.die("Invalid SQLite storage limits or namespace");
      const sql = (yield* SqlClient.SqlClient).withoutTransforms();
      const host = yield* DurableObjectState.DurableObjectState;
      const native = host.raw.storage;

      yield* sql
        .withTransaction(
          Effect.gen(function* () {
            yield* sql`CREATE TABLE IF NOT EXISTS sync_metadata (singleton INTEGER PRIMARY KEY CHECK(singleton = 1), version INTEGER NOT NULL, namespace TEXT NOT NULL)`;

            const rows = yield* sql<{
              version: number;
              namespace: string;
            }>`SELECT version, namespace FROM sync_metadata WHERE singleton = 1`;

            if (
              rows.length > 0 &&
              (rows[0].version !== 1 || rows[0].namespace !== options.namespace)
            )
              return yield* StorageError.make({
                message:
                  "Storage namespace or migration version differs; refusing to reset authoritative data",
              });
            yield* sql`CREATE TABLE IF NOT EXISTS sync_head (singleton INTEGER PRIMARY KEY CHECK(singleton = 1), generation TEXT NOT NULL, cursor INTEGER NOT NULL, value TEXT NOT NULL)`;
            yield* sql`CREATE TABLE IF NOT EXISTS sync_events (generation TEXT NOT NULL, cursor INTEGER NOT NULL, value TEXT NOT NULL, PRIMARY KEY(generation, cursor))`;
            yield* sql`CREATE TABLE IF NOT EXISTS sync_receipts (generation TEXT NOT NULL, actor TEXT NOT NULL, command TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY(generation, actor, command))`;
            yield* sql`CREATE UNIQUE INDEX IF NOT EXISTS sync_unbound_receipts ON sync_receipts(actor, command) WHERE json_extract(value, '$.command.admittedGeneration') IS NULL`;
            yield* sql`CREATE TABLE IF NOT EXISTS sync_outbox (id TEXT PRIMARY KEY, available INTEGER NOT NULL, attempts INTEGER NOT NULL, status TEXT NOT NULL, value TEXT NOT NULL, failure TEXT)`;
            yield* sql`CREATE INDEX IF NOT EXISTS sync_outbox_pending ON sync_outbox(status, available)`;
            yield* sql`INSERT OR IGNORE INTO sync_metadata(singleton, version, namespace) VALUES(1, 1, ${options.namespace})`;
          }),
        )
        .pipe(Effect.mapError(failure));

      const arm = (atMillis: number) =>
        Effect.tryPromise({
          try: async () => {
            const current = await native.getAlarm();

            if (current === null || current > atMillis) await native.setAlarm(atMillis);
          },
          catch: failure,
        });

      const readHead = Effect.gen(function* () {
        const rows = yield* sql`SELECT value FROM sync_head WHERE singleton = 1`;

        return rows[0] === undefined ? undefined : yield* fromRow(StoredHead, rows[0]);
      }).pipe(Effect.mapError(failure));

      const initialize = Effect.fn("SqliteStorage.initialize")(function* (head: StoredHead) {
        const value = yield* toJson(StoredHead, head);

        yield* sql`INSERT INTO sync_head(singleton, generation, cursor, value) VALUES(1, ${head.position.sourceAuthorityGeneration}, ${head.position.cursor}, ${value})`;
      }, Effect.mapError(failure));

      const readReceipt = Effect.fn("SqliteStorage.readReceipt")(function* (
        identity: ReceiptIdentity,
      ) {
        const rows =
          identity.sourceAuthorityGeneration === null
            ? yield* sql`SELECT value FROM sync_receipts WHERE actor = ${identity.actorId} AND command = ${identity.commandId} AND json_extract(value, '$.command.admittedGeneration') IS NULL`
            : yield* sql`SELECT value FROM sync_receipts WHERE generation = ${identity.sourceAuthorityGeneration} AND actor = ${identity.actorId} AND command = ${identity.commandId}`;

        return rows[0] === undefined ? undefined : yield* fromRow(StoredReceipt, rows[0]);
      }, Effect.mapError(failure));

      const readEvents: SourceTransaction["readEvents"] = Effect.fn("SqliteStorage.readEvents")(
        function* (after, limit) {
          const rows =
            yield* sql`SELECT value FROM sync_events WHERE generation = ${after.sourceAuthorityGeneration} AND cursor > ${after.cursor} ORDER BY cursor LIMIT ${limit}`;

          return yield* Effect.forEach(rows, (row) => fromRow(StoredEvent, row));
        },
        Effect.mapError(failure),
      );

      const commit = Effect.fn("SqliteStorage.commit")(function* (plan: StorageCommit) {
        const value = yield* toJson(StoredHead, plan.head);

        const updated =
          yield* sql`UPDATE sync_head SET generation = ${plan.head.position.sourceAuthorityGeneration}, cursor = ${plan.head.position.cursor}, value = ${value} WHERE singleton = 1 AND generation = ${plan.expected.sourceAuthorityGeneration} AND cursor = ${plan.expected.cursor} RETURNING singleton`;

        if (updated.length !== 1)
          return yield* StorageError.make({ message: "Stale source commit" });
        for (const event of plan.events) {
          const encoded = yield* toJson(StoredEvent, event);

          yield* sql`INSERT INTO sync_events(generation, cursor, value) VALUES(${event.position.sourceAuthorityGeneration}, ${event.position.cursor}, ${encoded})`;
        }
        const receipt = yield* toJson(StoredReceipt, plan.receipt);

        yield* sql`INSERT INTO sync_receipts(generation, actor, command, value) VALUES(${plan.receipt.sourceAuthorityGeneration}, ${plan.receipt.actorId}, ${plan.receipt.commandId}, ${receipt})`;
        if (plan.outbox.length > 0) {
          const count = yield* sql<{
            count: number;
          }>`SELECT COUNT(*) AS count FROM sync_outbox WHERE status = 'pending'`;

          if (count[0].count + plan.outbox.length > options.maxPendingOutbox)
            return yield* StorageError.make({ message: "Outbox is at capacity" });
          for (const record of plan.outbox) {
            const encoded = yield* toJson(OutboxRecord, record);

            yield* sql`INSERT INTO sync_outbox(id, available, attempts, status, value) VALUES(${record.id}, ${record.availableAtMillis}, 0, 'pending', ${encoded})`;
          }
          // The alarm is armed in the same native transaction before work is accepted.
          yield* arm(Math.min(...plan.outbox.map((record) => record.availableAtMillis)));
        }
        // Receipts and failed outbox evidence have independent retention.
        yield* sql`DELETE FROM sync_events WHERE generation = ${plan.head.position.sourceAuthorityGeneration} AND cursor <= ${plan.head.position.cursor - options.replayWindow}`;
      }, Effect.mapError(failure));

      const transaction: SourceStorage["Service"]["transaction"] = (body) =>
        sql
          .withTransaction(body({ readHead, initialize, readReceipt, readEvents, commit }))
          .pipe(Effect.catchTag("SqlError", (cause) => Effect.fail(failure(cause))));

      const claimOutbox: SourceStorage["Service"]["claimOutbox"] = Effect.fn(
        "SqliteStorage.claimOutbox",
      )(function* (now, leaseMillis, limit) {
        return yield* sql.withTransaction(
          Effect.gen(function* () {
            const rows =
              yield* sql`SELECT value FROM sync_outbox WHERE status = 'pending' AND available <= ${now} ORDER BY available, id LIMIT ${limit}`;

            const claimed: Array<OutboxRecord> = [];

            for (const row of rows) {
              const previous = yield* fromRow(OutboxRecord, row);

              const record = {
                ...previous,
                attempts: previous.attempts + 1,
                availableAtMillis: now + leaseMillis,
              };

              const value = yield* toJson(OutboxRecord, record);

              yield* sql`UPDATE sync_outbox SET attempts = ${record.attempts}, available = ${record.availableAtMillis}, value = ${value} WHERE id = ${record.id} AND status = 'pending'`;
              claimed.push(record);
            }
            if (claimed.length > 0) yield* arm(now + leaseMillis);

            return claimed;
          }),
        );
      }, Effect.mapError(failure));

      const settleOutbox: SourceStorage["Service"]["settleOutbox"] = Effect.fn(
        "SqliteStorage.settleOutbox",
      )(function* (record, disposition) {
        yield* sql.withTransaction(
          Effect.gen(function* () {
            if (disposition._tag === "Delivered") {
              yield* sql`DELETE FROM sync_outbox WHERE id = ${record.id} AND attempts = ${record.attempts} AND status = 'pending'`;
            } else if (disposition._tag === "PermanentFailure") {
              yield* sql`UPDATE sync_outbox SET status = 'failed', failure = ${disposition.reason} WHERE id = ${record.id} AND attempts = ${record.attempts} AND status = 'pending'`;
            } else {
              const value = yield* toJson(OutboxRecord, {
                ...record,
                availableAtMillis: disposition.atMillis,
              });

              yield* sql`UPDATE sync_outbox SET available = ${disposition.atMillis}, value = ${value} WHERE id = ${record.id} AND attempts = ${record.attempts} AND status = 'pending'`;
              yield* arm(disposition.atMillis);
            }
          }),
        );
      }, Effect.mapError(failure));

      const nextOutboxTime = sql<{
        at: number | null;
      }>`SELECT MIN(available) AS at FROM sync_outbox WHERE status = 'pending'`.pipe(
        Effect.map((rows) => rows[0].at ?? undefined),
        Effect.mapError(failure),
      );

      return SourceStorage.of({ transaction, claimOutbox, settleOutbox, nextOutboxTime });
    }),
  ).pipe(Layer.provide(DurableObjectSqlite.layer()));
