# How Sync works

A source is one ordered authority for a domain entity. The shared contract names
its public snapshot, durable events, ephemeral messages, and typed actions. A
plugin adds a reusable capability with its own snapshot and actions while sharing
the source's cursor and lifecycle.

## Authority and receipts

The server keeps private state behind the public contract. Each action turn
authorizes against stored state, checks for an existing receipt, or runs the
application handler. A successful turn atomically stores the new state, events,
exact result, receipt, and any outbox obligations. Declared domain rejections are
also durable outcomes. Operational failures do not become domain rejections.

A command identity belongs to its authenticated actor and source authority.
Retrying the same command with the same encoded payload returns its original
outcome, even after later commands change the source. Reusing the identity for a
different action or payload is rejected.

## Replay and recovery

The server assigns one cursor to durable events. A client applies contiguous
events once and catches up from a retained position after reconnecting. A gap
requires a new authoritative snapshot; an ordinary snapshot cannot rewind a
live replica. Ephemeral messages have no cursor and do not change durable state.

The client exposes both authoritative position and optimistic value. A declared
rejection removes its optimistic overlay. If a response is lost after admission,
the pending journal retains the original command for result lookup or exact
resend. A change in authority leaves old unresolved work visible for application
reconciliation.

## Ownership boundaries

| Sync owns                                     | Your application owns                       |
| --------------------------------------------- | ------------------------------------------- |
| Source/action/plugin contracts and codecs     | Domain schemas and business rules           |
| Atomic execution, receipts, replay, and retry | Authentication and authorization policy     |
| Scoped client sessions and recovery           | Projection destinations and delivery policy |
| Persistence ports and host adapters           | UI, bootstrap data, and migration decisions |

Server storage and client persistence are separate contracts. Disposable
snapshot caches never replace the pending-intent journal. Choose an explicit
namespace and reconcile unresolved identities before resetting or migrating
durable data. The [consumer example](../api/consumer.md) puts these pieces
together; the [client guide](../client.md) describes the replica lifecycle.
