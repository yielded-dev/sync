# Examples

Runnable library consumers belong in this workspace directory. They must depend on
public `@yielded/sync` entry points and selected adapters.

The standalone consumer in KOM-208 will exercise convergence, rejection rollback,
lost-response retry, reconnect, and reload/restart recovery before the first beta.
