import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import { EnvironmentId, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import { McpInvocationContext } from "../mcp/McpInvocationContext.ts";
import { make as makeTools } from "../mcp/toolkits/fleet/handlers.ts";
import { assert, describe, it } from "@effect/vitest";
import { type OrchestrationCommand, type OrchestrationThreadActivity } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { derivePendingRequests } from "../../../../packages/client-runtime/src/pendingRequests.ts";
import { OrchestrationEngineService } from "../orchestration/Services/OrchestrationEngine.ts";
import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import { Coordinator, layer } from "./Coordinator.ts";
import { requestDeviceApproval, respondToDeviceApproval } from "./Approvals.ts";

const fixture = layer.pipe(Layer.provide(SqlitePersistenceMemory));
describe("Connect device approval in the existing thread UI", () => {
  it.effect(
    "keeps ordinary commands automatic, approves elevation once, and rejects another thread or persistent approval",
    () =>
      Effect.gen(function* () {
        const activities: Array<OrchestrationThreadActivity> = [];
        const commands = new Set<string>();
        const engine = Layer.mock(OrchestrationEngineService, {
          dispatch: (command: OrchestrationCommand) =>
            Effect.sync(() => {
              if (command.type === "thread.activity.append" && !commands.has(command.commandId)) {
                commands.add(command.commandId);
                activities.push(command.activity);
              }
              return { sequence: activities.length };
            }),
        });
        yield* Effect.gen(function* () {
          const coordinator = yield* Coordinator;
          yield* coordinator.connectDevices(["env-windows"], "thread-one");
          yield* coordinator.poll("connect:env-windows", {
            environmentId: "env-windows",
            os: "windows",
            arch: "x64",
            t3Version: "0.0.44",
            policyHash: "1".repeat(64),
            execution: "allow",
            readRoots: ["home"],
            writeRoots: ["home"],
            tasks: {},
            applications: {},
          });
          const command = {
            executable: "powershell.exe",
            args: ["-Command", "Write-Output ok"],
            cwd: "C:\\Users\\me",
            timeoutSeconds: 30,
          };
          const ordinary = yield* coordinator.enqueue("thread-one", {
            requestId: "ordinary",
            device: "env-windows",
            action: { kind: "exec", command },
          });
          assert.equal(ordinary.status, "queued");
          yield* requestDeviceApproval(ordinary);
          assert.deepEqual(activities, []);
          const elevated = yield* coordinator.enqueue("thread-one", {
            requestId: "elevated",
            device: "env-windows",
            action: { kind: "exec", command: { ...command, requiresElevation: true } },
          });
          yield* requestDeviceApproval(elevated);
          yield* requestDeviceApproval(elevated);
          const pending = derivePendingRequests(activities).approvals;
          assert.equal(pending.length, 1);
          const id = pending[0]!.requestId;
          assert.deepEqual(
            pending[0]!.options?.map((option) => option.label),
            ["Allow once", "Deny"],
          );
          assert.equal(
            (yield* respondToDeviceApproval("other-thread", id, "accept").pipe(Effect.result))._tag,
            "Failure",
          );
          assert.equal(
            (yield* respondToDeviceApproval("thread-one", id, "acceptAlways").pipe(Effect.result))
              ._tag,
            "Failure",
          );
          const approved = yield* respondToDeviceApproval("thread-one", id, "accept");
          assert.equal(approved.status, "queued");
          assert.equal(approved.approvedPolicyHash, "1".repeat(64));
          assert.deepEqual(derivePendingRequests(activities).approvals, []);
          const another = yield* coordinator.enqueue("thread-one", {
            requestId: "another-elevation",
            device: "env-windows",
            action: elevated.action,
          });
          assert.equal(another.status, "awaiting-approval");
          yield* requestDeviceApproval(another);
          const deniedId = derivePendingRequests(activities).approvals[0]!.requestId;
          assert.equal(
            (yield* respondToDeviceApproval("thread-one", deniedId, "decline")).status,
            "cancelled",
          );
          assert.deepEqual(derivePendingRequests(activities).approvals, []);
        }).pipe(Effect.provide(engine));
      }).pipe(Effect.provide(fixture)),
  );
});

it.effect(
  "continues the same agent tool call after native T3 approval and the target receipt",
  () =>
    Effect.gen(function* () {
      const opened = yield* Deferred.make<string>();
      const engine = Layer.mock(OrchestrationEngineService, {
        dispatch: (command: OrchestrationCommand) =>
          Effect.gen(function* () {
            if (
              command.type === "thread.activity.append" &&
              command.activity.kind === "approval.requested"
            ) {
              const payload = command.activity.payload as { requestId: string };
              yield* Deferred.succeed(opened, payload.requestId);
            }
            return { sequence: 1 };
          }),
      });
      yield* Effect.gen(function* () {
        const coordinator = yield* Coordinator;
        yield* coordinator.connectDevices(["env-windows"], "thread-one");
        const metadata = {
          environmentId: "env-windows",
          os: "windows" as const,
          arch: "x64" as const,
          t3Version: "0.0.44",
          policyHash: "1".repeat(64),
          execution: "allow" as const,
          readRoots: ["home"],
          writeRoots: ["home"],
          tasks: {},
          applications: {},
        };
        yield* coordinator.poll("connect:env-windows", metadata);
        const handlers = yield* makeTools;
        const fiber = yield* handlers
          .fleet_run({
            requestId: "install",
            device: "env-windows",
            command: {
              executable: "msiexec.exe",
              args: ["/i", "C:\\builds\\app.msi"],
              cwd: "C:\\builds",
              timeoutSeconds: 60,
              requiresElevation: true,
            },
          })
          .pipe(
            Effect.provideService(McpInvocationContext, {
              environmentId: EnvironmentId.make("env-linux"),
              threadId: ThreadId.make("thread-one"),
              providerInstanceId: ProviderInstanceId.make("codex"),
              providerSessionId: "session",
              capabilities: new Set<never>(),
              issuedAt: 0,
            }),
            Effect.forkChild({ startImmediately: true }),
          );
        const id = yield* Deferred.await(opened);
        yield* respondToDeviceApproval("thread-one", id, "accept");
        const claim = yield* coordinator.poll("connect:env-windows", metadata);
        assert(claim);
        yield* coordinator.receipt("connect:env-windows", {
          jobId: claim.job.id,
          lease: claim.job.lease!,
          result: {
            status: "succeeded",
            stdout: "Installed exact build",
            stderr: "",
            exitCode: 0,
            truncated: false,
          },
        });
        const result = yield* Fiber.join(fiber);
        assert.equal(result.status, "succeeded");
        assert.equal(result.result?.stdout, "Installed exact build");
      }).pipe(Effect.provide(engine));
    }).pipe(Effect.provide(fixture)),
);
