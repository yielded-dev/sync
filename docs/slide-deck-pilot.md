# Slide-deck consumer pilot

The slide-deck application owns slide schemas, editing rules, authorization,
catalog records, projection destinations, and UI. A pilot should use the public
sync entry points without moving those product decisions into the library.
The [list and board consumer](../examples/list-board/README.md) provides a small
runnable example of the transport and runtime assembly.

Start in the product's `packages/slide-deck-source`: `SlideDeckRealtime.ts` owns
the state, event, and command schemas; `slides/SlideReducer.ts` owns event
application; `server/SlidesCapability.ts` owns validation and bootstrap;
`client/definition.ts` owns optimistic behavior. Keep the current one-to-100
slide bound, deterministic slide IDs, exact reorder validation, and shell
identity checks when adapting those files.

## Contract and runtime mapping

| Current responsibility                         | Pilot ownership                                                                                                                                 |
| ---------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| Slide state and events                         | Product `Source.make` contract using its existing `Slide` schema and schema version                                                             |
| Add, edit title/body, remove, reorder, replace | Separate typed `Action.make` registrations with product rejection schemas; runtime owns command IDs, authority binding, and exact result lookup |
| Validation and event application               | Product `Server.make` handlers and pure `Client.definition` reducers; actions commit state and events together                                  |
| Web and mobile replica                         | One actor-scoped `Client.make` session, `SourceAtom.make` bindings, and the IndexedDB or Expo SQLite persistence adapter                        |
| Session authorization                          | Product auth provider at the Cloudflare Worker boundary; root authorization also covers every registered capability                             |
| Catalog, shell, checkpoint and projections     | Product database and delivery services; commit required outbox intent with the source mutation and deliver idempotently                         |

The first pilot should use a disposable deck and namespace. Keep its initial
slide and shell creation in one product-owned bootstrap workflow. The existing
deck cannot be silently copied into a new Durable Object or local namespace:
unresolved command identities and receipts need explicit reconciliation. Public
`Server.make` initialization has no command payload, so the application must
either supply a fully initialized deck through a service before opening the
source or represent bootstrap as a typed action with an explicit empty state.
The latter requires a distinct empty-state schema because today's `SlideDeckState`
requires at least one slide. Choose that contract before switching a live deck.

The client can reuse the product's pure slide event reducer. Each action needs
an optimistic reducer, including an identity reducer when an edit should wait
for authority. Domain rejections remove the overlay; uncertain transport
failures retain the original command. Web and mobile sessions close on identity
change and reopen against the same actor journal. Presentation code reads Atom
state and dispatches Effects; it owns no second command queue or socket cursor.

For the pilot, verify one new deck through public exports: two signed-in clients
converge; a rejected edit rolls back; a response lost after commit returns its
original result; a disconnected client catches up; and browser reload and native
app restart retain unresolved evidence. Verify checkpoint or catalog projection
delivery against the product destination with a stable idempotency key. The
existing native iOS adapter proof covers unchanged SQLite operations; run the
application's own restart check before replacing its source. Android execution
still needs separate proof.
