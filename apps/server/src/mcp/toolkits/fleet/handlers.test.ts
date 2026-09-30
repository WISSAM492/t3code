import { assert, describe, it } from "@effect/vitest";
import { EnvironmentId, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { Coordinator, layer } from "../../../fleet/Coordinator.ts";
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
describe("Fleet agent tools", () => {
  it.effect("derives the thread from the MCP credential rather than agent-supplied data", () =>
    Effect.gen(function* () {
      const coordinator = yield* Coordinator;
      yield* coordinator.enroll("macbook", "device-token");
      yield* coordinator.grant("authorized", "macbook", ["run"], 100_000);
      const handlers = yield* make;
      const job = yield* handlers.fleet_run({
        requestId: "test",
        device: "macbook",
        task: "tests",
      });
      assert.equal(job.threadId, "authorized");
      const unauthorized = { ...invocation, threadId: ThreadId.make("other-thread") };
      assert.equal(
        (yield* handlers
          .fleet_run({ requestId: "test", device: "macbook", task: "tests" })
          .pipe(Effect.provideService(McpInvocationContext, unauthorized), Effect.result))._tag,
        "Failure",
      );
      assert.deepEqual(
        yield* handlers
          .fleet_devices()
          .pipe(Effect.provideService(McpInvocationContext, unauthorized)),
        [],
      );
      assert.equal((yield* handlers.fleet_status({})).length, 1);
      assert.equal(
        (yield* handlers
          .fleet_status({ jobId: job.id })
          .pipe(Effect.provideService(McpInvocationContext, unauthorized), Effect.result))._tag,
        "Failure",
      );
    }).pipe(Effect.provide(fixture), Effect.provideService(McpInvocationContext, invocation)),
  );
  it.effect("requires both device grants before persisting either side of a transfer", () =>
    Effect.gen(function* () {
      const coordinator = yield* Coordinator;
      yield* coordinator.enroll("linux-main", "linux-token");
      yield* coordinator.enroll("macbook", "mac-token");
      yield* coordinator.grant("authorized", "linux-main", ["transfer"], 100_000);
      const handlers = yield* make;
      const input = {
        requestId: "move",
        from: "linux-main",
        fromRoot: "project",
        fromPath: "report.pdf",
        to: "macbook",
        toRoot: "downloads",
        toPath: "report.pdf",
      };
      assert.equal((yield* handlers.fleet_transfer(input).pipe(Effect.result))._tag, "Failure");
      assert.deepEqual(yield* coordinator.jobs(), []);
      yield* coordinator.grant("authorized", "macbook", ["transfer"], 100_000);
      const jobs = yield* handlers.fleet_transfer(input);
      assert.equal(jobs.destination.action.kind, "receive");
      assert.equal((yield* handlers.fleet_transfer(input)).source.id, jobs.source.id);
    }).pipe(Effect.provide(fixture), Effect.provideService(McpInvocationContext, invocation)),
  );
});
