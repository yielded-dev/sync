import { Effect } from "effect";

import { Persistence } from "../../persistence/src/index.ts";

const store = (): Persistence.AtomicStore => {
  const values = new Map<string, string>();

  return {
    read: (key) => Effect.sync(() => values.get(key)),
    modify: (key, f) =>
      Effect.try({
        try: () => {
          const [value, next] = f(values.get(key));

          if (next === undefined) values.delete(key);
          else values.set(key, next);

          return value;
        },
        catch: Persistence.storageError,
      }),
  };
};

export const memory = (options: Omit<Persistence.Options, "namespace"> = {}) => {
  const drivers = { journal: Effect.succeed(store()), cache: Effect.succeed(store()) };

  return { open: Persistence.make({ namespace: "client-test", ...options }, drivers) };
};
