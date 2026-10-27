/**
 * Portable source hosting with ordered turns, live subscriptions, and durable delivery.
 * Applications supply authenticated connections, transport, and wakeup scheduling.
 *
 * @since 0.1.0-beta.0
 */

import type * as Effect from "effect/Effect";
import type * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import type * as Stream from "effect/Stream";

import * as internal from "../internal/host.ts";
import type { ProtocolError, SourceAddress, SourcePosition } from "../Model.ts";
import type * as Source from "../Source.ts";
import type { Definition, Runtime, Session } from "./Server.ts";
import type {
  DeliveryDisposition,
  OutboxRecord,
  ServerCrypto,
  SourceStorage,
  StorageError,
} from "./SourceStorage.ts";

/**
 * Trusted application context, never populated from client-supplied RPC headers.
 *
 * @category models
 * @since 0.1.0-beta.0
 */
export interface Connection {
  readonly address: SourceAddress;
  readonly session: Session;
}

/**
 * An authenticated connection owned by its acquisition scope.
 *
 * @category models
 * @since 0.1.0-beta.0
 */
export interface LiveConnection extends Connection {
  /** Failure requests transport closure; normal scope disposal completes successfully. */
  readonly closed: Effect.Effect<void, ProtocolError>;
}

/**
 * Bounds and optional durable delivery for one source coordinator.
 * Counts and durations must be positive safe integers.
 *
 * @category models
 * @since 0.1.0-beta.0
 */
export interface HostOptions<R = never> {
  /** Defaults to 512 events and 600000 encoded bytes per replay batch. */
  readonly replay?: { readonly maxEvents: number; readonly maxBatchBytes: number };
  /** Defaults to 100 connections and 256 buffered events per subscriber. */
  readonly connections?: { readonly maxConnections: number; readonly bufferSize: number };
  /** Maximum waiting turns. Defaults to 128. */
  readonly maxQueuedTurns?: number;
  /** Maximum time for one source turn. Defaults to 5000 milliseconds. */
  readonly turnTimeoutMillis?: number;
  readonly outbox?: {
    /** Maximum records per drain and per command. Defaults to 10. */
    readonly batchSize?: number;
    /**
     * Claim lease and automatic retry delay in milliseconds; defaults to 60,000.
     * Delivery attempts time out after half this interval.
     */
    readonly retryMillis?: number;
    readonly deliver: (
      record: OutboxRecord,
    ) => Effect.Effect<DeliveryDisposition, ProtocolError, R>;
    /** Persist the next wakeup, or cancel it when undefined. */
    readonly schedule: (atMillis: number | undefined) => Effect.Effect<void, StorageError, R>;
  };
}

type Frame<S extends Source.Spec> = Source.Definition<S>["envelope"]["outbound"]["Type"];

/**
 * One scoped coordinator for a source storage authority and its authenticated connections.
 *
 * @category models
 * @since 0.1.0-beta.0
 */
export interface Host<S extends Source.Spec = Source.Spec> {
  readonly contract: Source.Definition<S>;
  /** Keep this scope alive for the native connection, independently of subscriptions. */
  readonly connect: (
    connection: Connection,
  ) => Effect.Effect<LiveConnection, ProtocolError, Scope.Scope>;
  readonly checkConnection: (connection: Connection) => Effect.Effect<void, ProtocolError>;
  readonly snapshot: (
    connection: Connection,
    address: SourceAddress,
  ) => ReturnType<Runtime<S>["snapshot"]>;
  readonly lookup: (
    connection: Connection,
    address: SourceAddress,
    command: Schema.Json,
  ) => ReturnType<Runtime<S>["lookup"]>;
  readonly subscribe: (
    connection: Connection,
    address: SourceAddress,
    after?: SourcePosition,
  ) => Stream.Stream<Frame<S>, ProtocolError>;
  readonly dispatch: (
    connection: Connection,
    address: SourceAddress,
    command: Schema.Json,
  ) => Effect.Effect<Schema.Json, ProtocolError>;
  readonly publishMessage: (
    connection: Connection,
    address: SourceAddress,
    message: Schema.Json,
  ) => Effect.Effect<void, ProtocolError>;
  /** Invoke on a durable wakeup; the host does not create process-local timers. */
  readonly drainOutbox: Effect.Effect<void, ProtocolError | StorageError>;
}

/**
 * Creates one coordinator per storage authority. The caller's scope owns all
 * connections and captures the definition's services. Each turn acquires its
 * own operation scope. Outbox delivery and storage must be configured together.
 *
 * Invalid nonpositive bounds die; an outbox capability mismatch fails with
 * `StorageError`. Overloaded turns fail with `ProtocolError`.
 *
 * @category constructors
 * @since 0.1.0-beta.0
 */
export const host: <S extends Source.Spec, R, DeliveryR = never>(
  definition: Definition<S, R>,
  options?: HostOptions<DeliveryR>,
) => Effect.Effect<
  Host<S>,
  StorageError,
  R | DeliveryR | SourceStorage | ServerCrypto | Scope.Scope
> = internal.host;
