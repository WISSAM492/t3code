import { assert, describe, it } from "@effect/vitest";
import {
  EnvironmentId,
  ProviderInstanceId,
  ThreadId,
  type FleetMetadata,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as TestClock from "effect/testing/TestClock";
import { Coordinator, layer, LEASE_MS } from "../../../fleet/Coordinator.ts";
import { SqlitePersistenceMemory } from "../../../persistence/Layers/Sqlite.ts";
import { McpInvocationContext } from "../../McpInvocationContext.ts";
import { make } from "./handlers.ts";
const fixture = layer.pipe(Layer.provide(SqlitePersistenceMemory));
const invocation = {
  environmentId: EnvironmentId.make("home"),
  threadId: ThreadId.make("authorized"),
  providerInstanceId: ProviderInstanceId.make("codex"),
  providerSessionId: "session",
  capabilities: new Set<never>(),
  issuedAt: 0,
};
const metadata: FleetMetadata = {
  environmentId: "environment-mac",
  os: "darwin",
  arch: "arm64",
  t3Version: "0.0.44",
  policyHash: "1".repeat(64),
  tasks: {},
  applications: {},
  readRoots: ["home"],
  writeRoots: ["home"],
  execution: "allow",
  paths: { read: { home: "/Users/me" }, write: { home: "/Users/me" } },
};
const command = { executable: "/usr/bin/sw_vers", args: [], cwd: "/Users/me", timeoutSeconds: 30 };
const run = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(Effect.provide(fixture), Effect.provideService(McpInvocationContext, invocation));
describe("Fleet device capabilities in an ordinary agent thread", () => {
  it.effect("uses the MCP thread identity and discovers native device paths without tasks", () =>
    run(
      Effect.gen(function* () {
        const coordinator = yield* Coordinator;
        yield* coordinator.enroll("macbook", "device-token");
        yield* coordinator.grant("authorized", "macbook", ["run", "read"], 100_000);
        yield* coordinator.poll("device-token", metadata);
        yield* TestClock.adjust(LEASE_MS + 1);
        const handlers = yield* make;
        const devices = yield* handlers.fleet_devices();
        assert.equal(devices[0]!.os, "darwin");
        assert.deepEqual(devices[0]!.paths?.read, { home: "/Users/me" });
        const result = yield* handlers.fleet_run({ requestId: "test", device: "macbook", command });
        assert.equal(result.status, "queued");
        const job = yield* coordinator.job(result.operationId);
        assert.equal(job.threadId, "authorized");
        assert.deepEqual(job.action, { kind: "exec", command });
        const unauthorized = { ...invocation, threadId: ThreadId.make("other-thread") };
        assert.equal(
          (yield* handlers
            .fleet_run({ device: "macbook", command })
            .pipe(Effect.provideService(McpInvocationContext, unauthorized), Effect.result))._tag,
          "Failure",
        );
        assert.deepEqual(
          yield* handlers
            .fleet_devices()
            .pipe(Effect.provideService(McpInvocationContext, unauthorized)),
          [],
        );
        assert.equal(
          (yield* handlers
            .fleet_result({ operationId: result.operationId })
            .pipe(Effect.provideService(McpInvocationContext, unauthorized), Effect.result))._tag,
          "Failure",
        );
        const search = yield* handlers.fleet_search_files({
          device: "macbook",
          path: "/Users/me",
          query: "report.pdf",
        });
        assert.deepEqual((yield* coordinator.job(search.operationId)).action, {
          kind: "search",
          path: "/Users/me",
          query: "report.pdf",
          recursive: true,
        });
      }),
    ),
  );
  it.effect(
    "requires both device grants and deduplicates a move without exposing internal jobs",
    () =>
      run(
        Effect.gen(function* () {
          const coordinator = yield* Coordinator;
          yield* coordinator.enroll("linux-main", "linux-token");
          yield* coordinator.enroll("macbook", "mac-token");
          yield* coordinator.grant("authorized", "linux-main", ["transfer"], 100_000);
          const handlers = yield* make;
          const input = {
            requestId: "move",
            from: "linux-main",
            fromPath: "/home/me/report.pdf",
            to: "macbook",
            toPath: "/Users/me/Downloads/report.pdf",
            move: true,
          };
          assert.equal((yield* handlers.fleet_transfer(input).pipe(Effect.result))._tag, "Failure");
          assert.deepEqual(yield* coordinator.jobs(), []);
          yield* coordinator.grant("authorized", "macbook", ["transfer"], 100_000);
          const result = yield* handlers.fleet_transfer(input);
          assert.equal((yield* coordinator.job(result.operationId)).action.kind, "remove-source");
          assert.equal((yield* handlers.fleet_transfer(input)).operationId, result.operationId);
          assert.equal((yield* coordinator.jobs()).length, 3);
        }),
      ),
  );
});
