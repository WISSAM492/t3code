import { assert, describe, it } from "@effect/vitest";
import { type FleetAction, type FleetMetadata, type FleetResult } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as TestClock from "effect/testing/TestClock";
import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import { Coordinator, layer, LEASE_MS } from "./Coordinator.ts";

const metadata: FleetMetadata = {
  environmentId: "environment-linux",
  os: "linux",
  arch: "x64",
  t3Version: "0.0.44",
  policyHash: "1".repeat(64),
  tasks: { tests: "always", install: "once" },
  applications: { demo: "once" },
  readRoots: ["project"],
  writeRoots: ["project"],
};
const ok: FleetResult = {
  status: "succeeded",
  stdout: "done",
  stderr: "",
  exitCode: 0,
  truncated: false,
};
const fixture = layer.pipe(Layer.provide(SqlitePersistenceMemory));
const setup = Effect.gen(function* () {
  const coordinator = yield* Coordinator;
  yield* coordinator.enroll("linux-main", "credential-linux");
  yield* coordinator.grant(
    "thread-one",
    "linux-main",
    ["run", "read", "transfer", "deploy"],
    10_000_000,
  );
  yield* coordinator.poll("credential-linux", metadata);
  return coordinator;
});
const enqueue = (action: FleetAction, requestId = "request-1") =>
  Effect.flatMap(Coordinator, (coordinator) =>
    coordinator.enqueue("thread-one", { device: "linux-main", requestId, action }),
  );

describe("Fleet durable coordinator", () => {
  it.effect(
    "keeps dependent commands behind verified results and cancels them if their prerequisite fails",
    () =>
      Effect.gen(function* () {
        const coordinator = yield* setup;
        const enabled = { ...metadata, execution: "allow" as const };
        yield* coordinator.poll("credential-linux", enabled);
        yield* coordinator.enroll("macbook", "credential-mac");
        yield* coordinator.grant("thread-one", "macbook", ["run"], 10_000_000);
        const mac = { ...enabled, environmentId: "environment-mac", os: "darwin" as const };
        yield* coordinator.poll("credential-mac", mac);
        const command = {
          executable: "/bin/sh",
          args: ["-c", "echo installed"],
          cwd: "/Users/me",
          timeoutSeconds: 30,
        };
        for (const failed of [false, true]) {
          const prerequisite = yield* enqueue(
            { kind: "read", path: "/home/me/build-id" },
            `prepare-${failed}`,
          );
          const dependent = yield* coordinator.enqueue("thread-one", {
            device: "macbook",
            requestId: `install-${failed}`,
            action: { kind: "exec", command, afterOperationId: prerequisite.id },
          });
          assert.equal(dependent.status, "queued");
          assert.equal(yield* coordinator.poll("credential-mac", mac), null);
          const source = yield* coordinator.poll("credential-linux", enabled);
          assert(source);
          yield* coordinator.receipt("credential-linux", {
            jobId: source.job.id,
            lease: source.job.lease!,
            result: failed ? { ...ok, status: "failed" } : ok,
          });
          if (failed) {
            yield* TestClock.adjust(LEASE_MS + 1);
            assert.equal((yield* coordinator.job(dependent.id)).status, "cancelled");
            assert.equal(yield* coordinator.poll("credential-mac", mac), null);
          } else {
            const target = yield* coordinator.poll("credential-mac", mac);
            assert.equal(target?.job.id, dependent.id);
            yield* coordinator.receipt("credential-mac", {
              jobId: target!.job.id,
              lease: target!.job.lease!,
              result: ok,
            });
          }
        }
        yield* coordinator.grant("other-thread", "linux-main", ["read"], 10_000_000);
        const other = yield* coordinator.enqueue("other-thread", {
          device: "linux-main",
          requestId: "other",
          action: { kind: "read", path: "/home/me/file" },
        });
        assert.equal(
          (yield* coordinator
            .enqueue("thread-one", {
              device: "macbook",
              requestId: "cross-thread",
              action: { kind: "exec", command, afterOperationId: other.id },
            })
            .pipe(Effect.result))._tag,
          "Failure",
        );
      }).pipe(Effect.provide(fixture)),
  );

  it.effect(
    "requires explicit command permission and binds once approval to the exact command and policy",
    () =>
      Effect.gen(function* () {
        const coordinator = yield* setup;
        const command = {
          executable: "powershell.exe",
          args: ["-NoProfile", "-Command", "Write-Output 42"],
          cwd: "C:\\Users\\me",
          timeoutSeconds: 30,
        };
        const action = { kind: "exec" as const, command };
        assert.equal((yield* enqueue(action).pipe(Effect.result))._tag, "Failure");
        const asking = { ...metadata, execution: "ask" as const };
        yield* coordinator.poll("credential-linux", asking);
        const job = yield* enqueue(action);
        assert.equal(job.status, "awaiting-approval");
        assert.equal(
          (yield* enqueue({ ...action, command: { ...command, args: ["changed"] } }).pipe(
            Effect.result,
          ))._tag,
          "Failure",
        );
        yield* coordinator.approve(job.id, true);
        assert.equal(
          yield* coordinator.poll("credential-linux", { ...asking, policyHash: "2".repeat(64) }),
          null,
        );
        assert.equal((yield* coordinator.job(job.id)).status, "awaiting-approval");
        yield* coordinator.approve(job.id, true);
        const claim = yield* coordinator.poll("credential-linux", {
          ...asking,
          policyHash: "2".repeat(64),
        });
        assert.equal(claim?.job.id, job.id);
        yield* coordinator.receipt("credential-linux", {
          jobId: job.id,
          lease: claim!.job.lease!,
          result: ok,
        });
        const allowed = { ...metadata, execution: "allow" as const };
        yield* coordinator.poll("credential-linux", allowed);
        assert.equal((yield* enqueue(action, "allowed")).status, "queued");
        assert.equal(
          (yield* enqueue(
            { kind: "exec", command: { ...command, requiresElevation: true } },
            "elevated",
          )).status,
          "awaiting-approval",
        );
      }).pipe(Effect.provide(fixture)),
  );
  it.effect(
    "cancels dependent transfers even while their targets are offline and refuses same-file moves",
    () =>
      Effect.gen(function* () {
        const coordinator = yield* setup;
        yield* coordinator.enroll("macbook", "mac-token");
        yield* coordinator.grant("thread-one", "macbook", ["transfer"], 10_000_000);
        const jobs = yield* coordinator.transfer("thread-one", {
          requestId: "move",
          from: "linux-main",
          fromPath: "/home/me/file",
          to: "macbook",
          toPath: "/Users/me/file",
          move: true,
        });
        const claim = yield* coordinator.poll("credential-linux", metadata);
        assert(claim);
        yield* coordinator.receipt("credential-linux", {
          jobId: claim.job.id,
          lease: claim.job.lease!,
          result: { ...ok, status: "failed", stderr: "file missing" },
        });
        assert.equal((yield* coordinator.job(jobs.destination.id)).status, "cancelled");
        assert.equal((yield* coordinator.job(jobs.cleanup!.id)).status, "cancelled");
        assert.equal(
          (yield* coordinator
            .transfer("thread-one", {
              requestId: "same-file",
              from: "linux-main",
              fromPath: "/home/me/file",
              to: "linux-main",
              toPath: "/home/me/file",
              move: true,
              overwrite: true,
            })
            .pipe(Effect.result))._tag,
          "Failure",
        );
      }).pipe(Effect.provide(fixture)),
  );

  it.effect("keeps devices invisible and actions denied without an explicit thread grant", () =>
    Effect.gen(function* () {
      const coordinator = yield* setup;
      assert.deepEqual(yield* coordinator.devices("other-thread"), []);
      assert.equal(
        (yield* coordinator
          .enqueue("other-thread", {
            device: "linux-main",
            requestId: "x",
            action: { kind: "run", task: "tests" },
          })
          .pipe(Effect.result))._tag,
        "Failure",
      );
      assert.equal((yield* coordinator.devices("thread-one")).length, 1);
    }).pipe(Effect.provide(fixture)),
  );
  it.effect(
    "queues offline work, deduplicates the exact request, and rejects conflicting reuse",
    () =>
      Effect.gen(function* () {
        const coordinator = yield* setup;
        yield* TestClock.adjust(LEASE_MS + 1);
        assert.equal((yield* coordinator.devices("thread-one"))[0]!.online, false);
        const job = yield* enqueue({ kind: "run", task: "tests" });
        assert.equal(job.status, "queued");
        assert.equal((yield* enqueue({ kind: "run", task: "tests" })).id, job.id);
        assert.equal(
          (yield* enqueue({ kind: "run", task: "install" }).pipe(Effect.result))._tag,
          "Failure",
        );
        const claim = yield* coordinator.poll("credential-linux", metadata);
        assert.equal(claim?.job.id, job.id);
        assert.equal(yield* coordinator.poll("credential-linux", metadata), null);
      }).pipe(Effect.provide(fixture)),
  );
  it.effect("requires once approval and invalidates it when the device policy changes", () =>
    Effect.gen(function* () {
      const coordinator = yield* setup;
      const job = yield* enqueue({ kind: "run", task: "install" });
      assert.equal(job.status, "awaiting-approval");
      assert.equal(yield* coordinator.poll("credential-linux", metadata), null);
      yield* coordinator.approve(job.id, true);
      assert.equal(
        yield* coordinator.poll("credential-linux", { ...metadata, policyHash: "2".repeat(64) }),
        null,
      );
      assert.equal((yield* coordinator.job(job.id)).status, "awaiting-approval");
      yield* coordinator.approve(job.id, true);
      assert.equal(
        (yield* coordinator.poll("credential-linux", { ...metadata, policyHash: "2".repeat(64) }))
          ?.job.id,
        job.id,
      );
    }).pipe(Effect.provide(fixture)),
  );
  it.effect("cancels denied approvals and revoked queued actions", () =>
    Effect.gen(function* () {
      const coordinator = yield* setup;
      const denied = yield* enqueue({ kind: "run", task: "install" });
      yield* coordinator.approve(denied.id, false);
      const job = yield* enqueue({ kind: "run", task: "tests" }, "second");
      yield* coordinator.revoke("thread-one", "linux-main");
      assert.equal((yield* coordinator.job(denied.id)).status, "cancelled");
      assert.equal((yield* coordinator.job(job.id)).status, "cancelled");
      assert.equal(yield* coordinator.poll("credential-linux", metadata), null);
    }).pipe(Effect.provide(fixture)),
  );
  it.effect("rechecks grant expiry before dispatching offline jobs", () =>
    Effect.gen(function* () {
      const coordinator = yield* setup;
      const job = yield* enqueue({ kind: "run", task: "tests" });
      yield* coordinator.grant("thread-one", "linux-main", ["run"], 10);
      yield* TestClock.adjust(11);
      assert.equal(yield* coordinator.poll("credential-linux", metadata), null);
      assert.equal((yield* coordinator.job(job.id)).status, "cancelled");
    }).pipe(Effect.provide(fixture)),
  );
  it.effect(
    "marks a lost execution uncertain and accepts its late immutable receipt without rerunning",
    () =>
      Effect.gen(function* () {
        const coordinator = yield* setup;
        const job = yield* enqueue({ kind: "run", task: "tests" });
        const claim = yield* coordinator.poll("credential-linux", metadata);
        assert(claim?.job.lease);
        yield* TestClock.adjust(LEASE_MS + 1);
        assert.equal((yield* coordinator.job(job.id)).status, "uncertain");
        assert.equal(yield* coordinator.poll("credential-linux", metadata), null);
        assert.equal((yield* coordinator.job(job.id)).status, "uncertain");
        const receipt = { jobId: job.id, lease: claim.job.lease, result: ok };
        assert.equal((yield* coordinator.receipt("credential-linux", receipt)).status, "succeeded");
        assert.equal((yield* coordinator.receipt("credential-linux", receipt)).status, "succeeded");
        assert.equal(
          (yield* coordinator
            .receipt("credential-linux", { ...receipt, result: { ...ok, stdout: "different" } })
            .pipe(Effect.result))._tag,
          "Failure",
        );
        assert.equal(yield* coordinator.poll("credential-linux", metadata), null);
      }).pipe(Effect.provide(fixture)),
  );
  it.effect("revokes running leases and never reports a restart as a success", () =>
    Effect.gen(function* () {
      const coordinator = yield* setup;
      yield* enqueue({ kind: "run", task: "tests" });
      const claim = yield* coordinator.poll("credential-linux", metadata);
      assert(claim?.job.lease);
      yield* coordinator.begin(claim.job);
      yield* coordinator.recover;
      assert.equal((yield* coordinator.pendingReceipts)[0]!.result.status, "uncertain");
      assert.equal((yield* coordinator.begin(claim.job).pipe(Effect.result))._tag, "Failure");
      yield* coordinator.revoke("thread-one", "linux-main");
      assert.equal(
        (yield* coordinator
          .renew("credential-linux", claim.job.id, claim.job.lease)
          .pipe(Effect.result))._tag,
        "Failure",
      );
    }).pipe(Effect.provide(fixture)),
  );
  it.effect(
    "binds credentials to one stable environment identity and keeps receipts device scoped",
    () =>
      Effect.gen(function* () {
        const coordinator = yield* setup;
        assert.equal(
          (yield* coordinator
            .poll("credential-linux", { ...metadata, environmentId: "different" })
            .pipe(Effect.result))._tag,
          "Failure",
        );
        yield* coordinator.enroll("macbook", "credential-mac");
        yield* enqueue({ kind: "run", task: "tests" });
        const claim = yield* coordinator.poll("credential-linux", metadata);
        assert(claim?.job.lease);
        assert.equal(
          (yield* coordinator
            .receipt("credential-mac", { jobId: claim.job.id, lease: claim.job.lease, result: ok })
            .pipe(Effect.result))._tag,
          "Failure",
        );
      }).pipe(Effect.provide(fixture)),
  );
  it.effect("pins platform builds and requires the actual installed version in the receipt", () =>
    Effect.gen(function* () {
      const coordinator = yield* setup;
      const build = {
        sha256: "a".repeat(64),
        size: 12,
        name: "demo",
        version: "82e9c51",
        os: "linux" as const,
        arch: "x64" as const,
      };
      yield* coordinator.registerArtifact(build);
      assert.equal(
        (yield* coordinator
          .registerArtifact({ ...build, sha256: "b".repeat(64) })
          .pipe(Effect.result))._tag,
        "Failure",
      );
      assert.equal(
        (yield* coordinator.registerArtifact({ ...build, version: "new" }).pipe(Effect.result))
          ._tag,
        "Failure",
      );
      const job = yield* enqueue({ kind: "deploy", application: "demo", sha256: build.sha256 });
      yield* coordinator.approve(job.id, true);
      const claim = yield* coordinator.poll("credential-linux", metadata);
      assert(claim?.job.lease);
      assert.equal(
        (yield* coordinator
          .receipt("credential-linux", {
            jobId: job.id,
            lease: claim.job.lease,
            result: { ...ok, installedVersion: "wrong", artifact: build.sha256 },
          })
          .pipe(Effect.result))._tag,
        "Failure",
      );
      yield* coordinator.receipt("credential-linux", {
        jobId: job.id,
        lease: claim.job.lease,
        result: { ...ok, installedVersion: build.version, artifact: build.sha256, healthy: true },
      });
      assert.deepEqual((yield* coordinator.devices())[0]!.installed.demo, {
        version: build.version,
        sha256: build.sha256,
        healthy: true,
      });
    }).pipe(Effect.provide(fixture)),
  );
  it.effect("rejects a Windows artifact on Linux and never crosses another thread's transfer", () =>
    Effect.gen(function* () {
      const coordinator = yield* setup;
      const build = {
        sha256: "a".repeat(64),
        size: 12,
        name: "demo",
        version: "abc123",
        os: "windows" as const,
        arch: "x64" as const,
      };
      yield* coordinator.registerArtifact(build);
      assert.equal(
        (yield* enqueue({ kind: "deploy", application: "demo", sha256: build.sha256 }).pipe(
          Effect.result,
        ))._tag,
        "Failure",
      );
      const source = yield* enqueue({ kind: "capture", root: "project", path: "file" });
      yield* coordinator.grant("other-thread", "linux-main", ["transfer"], 10_000_000);
      assert.equal(
        (yield* coordinator
          .enqueue("other-thread", {
            requestId: "steal",
            device: "linux-main",
            action: { kind: "receive", root: "project", path: "file", sourceJobId: source.id },
          })
          .pipe(Effect.result))._tag,
        "Failure",
      );
    }).pipe(Effect.provide(fixture)),
  );
  it.effect("queues the destination until the source receipt exists and checks both grants", () =>
    Effect.gen(function* () {
      const coordinator = yield* setup;
      yield* coordinator.enroll("macbook", "credential-mac");
      yield* coordinator.grant("thread-one", "macbook", ["transfer"], 10_000_000);
      const transfer = yield* coordinator.transfer("thread-one", {
        requestId: "send",
        from: "linux-main",
        fromRoot: "project",
        fromPath: "file",
        to: "macbook",
        toRoot: "project",
        toPath: "file",
      });
      assert.equal(
        yield* coordinator.poll("credential-mac", {
          ...metadata,
          environmentId: "environment-mac",
        }),
        null,
      );
      const claim = yield* coordinator.poll("credential-linux", metadata);
      assert(claim?.job.lease);
      const artifact = {
        sha256: "b".repeat(64),
        size: 3,
        name: "file",
        version: "b".repeat(64),
        os: "unknown" as const,
        arch: "other" as const,
      };
      yield* coordinator.registerArtifact(artifact);
      yield* coordinator.captured(
        "credential-linux",
        transfer.source.id,
        claim.job.lease,
        artifact,
      );
      yield* coordinator.receipt("credential-linux", {
        jobId: transfer.source.id,
        lease: claim.job.lease,
        result: { ...ok, artifact: artifact.sha256 },
      });
      yield* coordinator.revoke("thread-one", "linux-main");
      assert.equal(
        yield* coordinator.poll("credential-mac", {
          ...metadata,
          environmentId: "environment-mac",
        }),
        null,
      );
      assert.equal((yield* coordinator.job(transfer.destination.id)).status, "cancelled");
    }).pipe(Effect.provide(fixture)),
  );
});
