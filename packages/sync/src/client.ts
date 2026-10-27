/**
 * Scoped client replicas, transport, and persistence contracts.
 *
 * @since 0.1.0-beta.0
 */

export * as Client from "./client/Client.ts";
export { ClientError } from "./client/Model.ts";

export {
  ReplicaPersistence,
  PersistenceError,
  JournalRow,
  type Handle as PersistenceHandle,
  type IntentJournal,
  type JournalChanges,
  type SnapshotCache,
} from "./client/ReplicaPersistence.ts";
