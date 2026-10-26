# Integrating a slide editor

A slide editor owns its slide schemas, editing rules, authorization, catalog,
projection destinations, and UI. Use Sync's public entry points for coordination
without moving those application decisions into the library. The
[React board](../examples/list-board/README.md) provides runnable session and
transport setup.

| Application concern                 | Sync integration                                                       |
| ----------------------------------- | ---------------------------------------------------------------------- |
| Slide state and events              | `Source.make` with the application's schemas                           |
| Add, edit, remove, reorder, replace | Typed `Action.make` registrations and domain rejections                |
| Validation and authoritative edits  | `Server.make` handlers committing state and events together            |
| Event application and optimism      | Pure `Client.definition` reducers                                      |
| Web and mobile sessions             | `Client.make`, `SourceAtom.make`, and the platform persistence adapter |
| Authentication                      | Application provider at the Worker boundary                            |
| Catalog and checkpoint projections  | Outbox obligations with idempotent destination delivery                |

## Bootstrap and existing data

Keep slide count limits, deterministic ids, reorder validation, and shell identity
checks in the application. Initial slide and shell creation belong in one
application-owned bootstrap workflow.

`Server.make` initialization receives no command payload. Supply an initialized
deck through a service, or model bootstrap as a typed action with an explicit
empty-state schema. Choose that contract before switching a live deck.

Start with a disposable deck and namespace. Copying a deck into a new Durable
Object or local namespace does not migrate its authority or retry evidence.
Reconcile receipts, unresolved commands, and pending projections before switching.

## Editing and recovery

Add optimistic reducers for edits that need immediate feedback; omit them when
the edit should wait for authority. Domain rejections remove optimism; uncertain
transport outcomes retain the original command for exact retry. Web and mobile
sessions close on identity change and reopen against the correct actor journal.

Presentation code reads atoms and dispatches Effects. It needs no second command
queue or socket cursor. Catalog and checkpoint destinations must deduplicate
outbox delivery using the stable obligation id. Check restart recovery on the
application's storage platform and projection delivery at its destination.
