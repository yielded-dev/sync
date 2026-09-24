import { ExpoSqlite } from "@yielded/sync-local-expo";
import { Clock, Effect } from "effect";
import { registerRootComponent } from "expo";
import { createElement, useEffect, useState } from "react";
import { ScrollView, Text } from "react-native";

import * as scenarios from "../../packages/sync/test/persistence-scenarios.ts";

const namespace = "native-restart-proof-v2";

const probe = Effect.gen(function* () {
  const existing = yield* Effect.scoped(
    Effect.gen(function* () {
      const storage = yield* ExpoSqlite.open({ namespace, actorId: "alice" });

      return yield* storage.intentJournal.transaction((tx) =>
        tx.get(scenarios.evidence.address, scenarios.evidence.commandId),
      );
    }),
  );

  const run = String(yield* Clock.currentTimeMillis);
  const transactions = yield* scenarios.exercise(ExpoSqlite.open, `native-operations-${run}`);
  const conflicts = yield* scenarios.concurrent(ExpoSqlite.open, `native-conflicts-${run}`);

  if (existing === undefined) {
    yield* scenarios.seed(ExpoSqlite.open, namespace);
    const client = yield* scenarios.runtimeSeed(ExpoSqlite.open, `${namespace}-client`);

    return { phase: "seeded", transactions, conflicts, client };
  }
  yield* scenarios.restore(ExpoSqlite.open, namespace);
  const client = yield* scenarios.runtimeRestore(ExpoSqlite.open, `${namespace}-client`);

  return { phase: "restored", transactions, conflicts, client };
});

const App = () => {
  const [report, setReport] = useState("Running native persistence proof…");

  useEffect(() => {
    void Effect.runPromise(probe).then(
      (result) => {
        setReport(JSON.stringify(result));
      },
      (error: unknown) => {
        setReport(JSON.stringify({ error: String(error) }));
      },
    );
  }, []);

  return createElement(
    ScrollView,
    { contentContainerStyle: { padding: 32 } },
    createElement(Text, null, report),
  );
};

registerRootComponent(App);
