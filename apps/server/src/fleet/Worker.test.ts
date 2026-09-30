import * as NodeHttp from "node:http";
import * as NodeCrypto from "node:crypto";
import * as NodeHttpServer from "@effect/platform-node/NodeHttpServer";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import {
  AuthFleetDeviceScope,
  AuthStandardClientScopes,
  FleetJob,
  type FleetWorkerConfig,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as TestClock from "effect/testing/TestClock";
import { HostProcessPlatform, HostProcessArchitecture } from "@t3tools/shared/hostProcess";
import { HttpRouter, HttpServer } from "effect/unstable/http";
import * as EnvironmentAuth from "../auth/EnvironmentAuth.ts";
import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as ServerConfig from "../config.ts";
import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import { layer as processLayer } from "../processRunner.ts";
import { blobPath, publishArtifact } from "./Artifacts.ts";
import { Coordinator, layer as coordinatorLayer, MAX_OUTPUT_BYTES } from "./Coordinator.ts";
import { routes } from "./http.ts";
import {
  execute,
  makeTransport,
  resolveRootPath,
  runClaim,
  runCycle,
  workerMetadata as metadataFor,
} from "./Worker.ts";

const authLayer = EnvironmentAuth.layer.pipe(
  Layer.provideMerge(SqlitePersistenceMemory),
  Layer.provideMerge(ServerSecretStore.layer),
  Layer.provide(ServerEnvironment.identityLayer),
);
const fixture = HttpRouter.serve(routes, { disableListenLog: true, disableLogger: true }).pipe(
  Layer.provideMerge(coordinatorLayer),
  Layer.provideMerge(authLayer),
  Layer.provideMerge(processLayer),
  Layer.provideMerge(NodeHttpServer.layer(NodeHttp.createServer, { host: "127.0.0.1", port: 0 })),
  Layer.provideMerge(ServerConfig.layerTest(process.cwd(), { prefix: "t3-fleet-worker-test-" })),
  Layer.provideMerge(NodeServices.layer),
);
const setup = Effect.gen(function* () {
  const platform = yield* HostProcessPlatform;
  const architecture = yield* HostProcessArchitecture;
  const workerMetadata = (config: FleetWorkerConfig, id: string) =>
    metadataFor(config, id, platform, architecture);
  const fs = yield* FileSystem.FileSystem;
  const server = yield* HttpServer.HttpServer;
  assert("port" in server.address);
  const url = `http://127.0.0.1:${server.address.port}`;
  const config = yield* ServerConfig.ServerConfig;
  const auth = yield* EnvironmentAuth.EnvironmentAuth;
  const coordinator = yield* Coordinator;
  const issued = yield* auth.issueSession({ scopes: [AuthFleetDeviceScope] });
  yield* coordinator.enroll("linux-main", issued.sessionId);
  yield* coordinator.grant(
    "thread-one",
    "linux-main",
    ["run", "read", "transfer", "deploy"],
    Date.now() + 86400_000,
  );
  const root = yield* fs.makeTempDirectoryScoped({ prefix: "fleet-root-" });
  const worker: FleetWorkerConfig = {
    version: 1,
    coordinator: url,
    tokenFile: `${root}/token`,
    readRoots: { project: root },
    writeRoots: { project: root },
    tasks: {},
    applications: {},
  };
  yield* fs.writeFileString(worker.tokenFile, issued.token);
  const saveConfig = (value: FleetWorkerConfig) =>
    fs.writeFileString(`${config.stateDir}/fleet-worker.json`, JSON.stringify(value));
  yield* saveConfig(worker);
  const transport = makeTransport(worker, issued.token);
  yield* coordinator.poll(issued.sessionId, workerMetadata(worker, "environment-linux"));
  return {
    fs,
    config,
    root,
    url,
    auth,
    coordinator,
    issued,
    worker,
    transport,
    saveConfig,
    workerMetadata,
  };
});
const run = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.scoped(effect).pipe(Effect.provide(fixture), TestClock.withLive);

describe("Fleet outbound worker and HTTP boundary", () => {
  it.effect(
    "reports escaped and invalid UTF-8 file output without stranding its durable receipt",
    () =>
      run(
        Effect.gen(function* () {
          const { fs, root, coordinator } = yield* setup;
          for (const byte of [0, 255]) {
            yield* fs.writeFile(`${root}/data-${byte}`, Buffer.alloc(MAX_OUTPUT_BYTES, byte));
            const job = yield* coordinator.enqueue("thread-one", {
              device: "linux-main",
              requestId: `read-${byte}`,
              action: { kind: "read", root: "project", path: `data-${byte}` },
            });
            yield* runCycle(
              "environment-linux",
              yield* HostProcessPlatform,
              yield* HostProcessArchitecture,
            );
            const finished = yield* coordinator.job(job.id);
            assert.equal(finished.status, "succeeded");
            assert(Buffer.byteLength(finished.result!.stdout) <= MAX_OUTPUT_BYTES);
            assert.equal(finished.result!.truncated, byte === 255);
            assert.deepEqual(yield* coordinator.pendingReceipts, []);
          }
        }),
      ),
  );
  it.effect("rejects missing/browser/ordinary credentials and unenrolled device tokens", () =>
    run(
      Effect.gen(function* () {
        const { url, auth, worker, transport, workerMetadata } = yield* setup;
        const ordinary = yield* auth.issueSession({ scopes: AuthStandardClientScopes });
        const unbound = yield* auth.issueSession({ scopes: [AuthFleetDeviceScope] });
        for (const [headers, expected] of [
          [{}, 401],
          [{ authorization: `Bearer ${ordinary.token}` }, 403],
          [{ authorization: `Bearer ${unbound.token}` }, 400],
        ] as const) {
          const response = yield* Effect.promise(() =>
            fetch(`${url}/api/fleet/poll`, {
              method: "POST",
              headers: { ...headers, "content-type": "application/json" },
              body: JSON.stringify(workerMetadata(worker, "environment-linux")),
            }),
          );
          assert.equal(response.status, expected);
        }
        assert.equal(
          (yield* transport
            .request("poll", {
              method: "POST",
              headers: { origin: "https://app.t3.codes" },
              body: "{}",
            })
            .pipe(Effect.result))._tag,
          "Failure",
        );
        const chunked = yield* Effect.promise(() =>
          fetch(`${url}/api/fleet/poll`, {
            method: "POST",
            body: "{}",
            headers: { authorization: `Bearer ${unbound.token}`, "content-length": "2" },
          }),
        );
        assert.equal(chunked.status, 400);
      }),
    ),
  );
  it.effect("transfers a binary file through real authenticated HTTP and refuses overwrites", () =>
    run(
      Effect.gen(function* () {
        const { fs, root, worker, issued, coordinator, transport, workerMetadata } = yield* setup;
        const bytes = Buffer.alloc(10 * 1024 * 1024, 0xab);
        yield* fs.writeFile(`${root}/source.bin`, bytes);
        const jobs = yield* coordinator.transfer("thread-one", {
          requestId: "transfer",
          from: "linux-main",
          fromRoot: "project",
          fromPath: "source.bin",
          to: "linux-main",
          toRoot: "project",
          toPath: "received.bin",
        });
        const source = yield* coordinator.poll(
          issued.sessionId,
          workerMetadata(worker, "environment-linux"),
        );
        assert(source);
        assert.equal(
          (yield* transport
            .request("artifact", {
              method: "PUT",
              headers: {
                "x-fleet-job": source.job.id,
                "x-fleet-lease": source.job.lease!,
                "x-fleet-offset": "100",
                "x-fleet-final": "0",
              },
              body: Buffer.from("bad offset"),
            })
            .pipe(Effect.result))._tag,
          "Failure",
        );
        const captured = yield* Effect.scoped(execute(worker, source, transport));
        assert.equal(
          captured.artifact,
          NodeCrypto.createHash("sha256").update(bytes).digest("hex"),
        );
        yield* transport.json(
          "receipt",
          { jobId: jobs.source.id, lease: source.job.lease, result: captured },
          FleetJob,
        );
        const destination = yield* coordinator.poll(
          issued.sessionId,
          workerMetadata(worker, "environment-linux"),
        );
        assert(destination);
        const received = yield* Effect.scoped(execute(worker, destination, transport));
        assert.equal(received.status, "succeeded");
        assert.deepEqual(Buffer.from(yield* fs.readFile(`${root}/received.bin`)), bytes);
        assert.equal(
          (yield* Effect.scoped(execute(worker, destination, transport)).pipe(Effect.result))._tag,
          "Failure",
        );
        assert.deepEqual(Buffer.from(yield* fs.readFile(`${root}/received.bin`)), bytes);
      }),
    ),
  );
  it.effect(
    "drives the outbound worker through its saved configuration and publishes the receipt",
    () =>
      run(
        Effect.gen(function* () {
          const { root, worker, coordinator, issued, saveConfig, workerMetadata } = yield* setup;
          const configured: FleetWorkerConfig = {
            ...worker,
            tasks: {
              tests: {
                approval: "always",
                command: {
                  executable: process.execPath,
                  args: ["-e", "console.log('tests passed')"],
                  cwd: root,
                  timeoutSeconds: 10,
                },
              },
            },
          };
          yield* saveConfig(configured);
          const metadata = workerMetadata(configured, "environment-linux");
          yield* coordinator.poll(issued.sessionId, metadata);
          const job = yield* coordinator.enqueue("thread-one", {
            device: "linux-main",
            requestId: "cycle",
            action: { kind: "run", task: "tests" },
          });
          yield* runCycle(
            "environment-linux",
            yield* HostProcessPlatform,
            yield* HostProcessArchitecture,
          );
          const finished = yield* coordinator.job(job.id);
          assert.equal(finished.status, "succeeded");
          assert.match(finished.result!.stdout, /tests passed/);
          assert.deepEqual(yield* coordinator.pendingReceipts, []);
        }),
      ),
  );
  it.effect("rechecks revocation before spawning a claimed task", () =>
    run(
      Effect.gen(function* () {
        const { root, fs, worker, coordinator, issued, saveConfig, transport, workerMetadata } =
          yield* setup;
        const configured: FleetWorkerConfig = {
          ...worker,
          tasks: {
            test: {
              approval: "always",
              command: {
                executable: process.execPath,
                args: [
                  "-e",
                  "require('node:fs').writeFileSync(process.argv[1],'ran')",
                  `${root}/ran`,
                ],
                cwd: root,
                timeoutSeconds: 10,
              },
            },
          },
        };
        yield* saveConfig(configured);
        const metadata = workerMetadata(configured, "environment-linux");
        yield* coordinator.poll(issued.sessionId, metadata);
        yield* coordinator.enqueue("thread-one", {
          device: "linux-main",
          requestId: "revoke",
          action: { kind: "run", task: "test" },
        });
        const claim = yield* coordinator.poll(issued.sessionId, metadata);
        assert(claim);
        yield* coordinator.revoke("thread-one", "linux-main");
        const result = yield* runClaim(configured, claim, transport);
        assert.equal(result.status, "uncertain");
        assert.equal(yield* fs.exists(`${root}/ran`), false);
      }),
    ),
  );
  it.effect("blocks absolute paths, traversal, and symlink escapes for reads and writes", () =>
    run(
      Effect.gen(function* () {
        const { fs, root } = yield* setup;
        const outside = yield* fs.makeTempDirectoryScoped();
        yield* fs.writeFileString(`${outside}/secret`, "private");
        yield* fs.symlink(outside, `${root}/escape`);
        for (const [relative, writing] of [
          [`${outside}/secret`, false],
          ["../secret", false],
          ["escape/secret", false],
          ["escape/new", true],
          ["", false],
        ] as const) {
          assert.equal(
            (yield* resolveRootPath({ project: root }, "project", relative, writing).pipe(
              Effect.result,
            ))._tag,
            "Failure",
          );
        }
        yield* fs.writeFileString(`${root}/allowed`, "allowed");
        assert.equal(
          yield* resolveRootPath({ project: root }, "project", "allowed"),
          `${root}/allowed`,
        );
      }),
    ),
  );
  it.effect(
    "installs the hashed artifact, probes the actual version, launches, and verifies startup",
    () =>
      run(
        Effect.gen(function* () {
          const { fs, root, worker, issued, coordinator, transport, saveConfig, workerMetadata } =
            yield* setup;
          const version = "82e9c51";
          yield* fs.writeFileString(`${root}/build`, version);
          const node = (script: string, args: ReadonlyArray<string> = []) => ({
            executable: process.execPath,
            args: ["-e", script, ...args],
            cwd: root,
            timeoutSeconds: 10,
          });
          const configured: FleetWorkerConfig = {
            ...worker,
            applications: {
              demo: {
                approval: "always",
                install: node(
                  "require('node:fs').copyFileSync(process.argv[1],process.argv[2]); console.log('installed')",
                  ["{artifact}", `${root}/installed-version`],
                ),
                verify: node(
                  "process.stdout.write(require('node:fs').readFileSync(process.argv[1],'utf8'))",
                  [`${root}/installed-version`],
                ),
                start: node(
                  "require('node:fs').writeFileSync(process.argv[1],'started');console.log('started')",
                  [`${root}/startup`],
                ),
                health: node(
                  "if(require('node:fs').readFileSync(process.argv[1],'utf8')!=='started')process.exit(1);console.log('healthy')",
                  [`${root}/startup`],
                ),
              },
            },
          };
          yield* saveConfig(configured);
          const metadata = workerMetadata(configured, "environment-linux");
          yield* coordinator.poll(issued.sessionId, metadata);
          const artifact = yield* publishArtifact(`${root}/build`, {
            name: "demo",
            version,
            os: metadata.os,
            arch: metadata.arch,
          });
          const job = yield* coordinator.enqueue("thread-one", {
            device: "linux-main",
            requestId: "deploy",
            action: { kind: "deploy", application: "demo", sha256: artifact.sha256 },
          });
          const claim = yield* coordinator.poll(issued.sessionId, metadata);
          assert(claim);
          const result = yield* runClaim(configured, claim, transport);
          assert.equal(result.status, "succeeded", result.stderr);
          assert.equal(result.installedVersion, version);
          assert.equal(result.healthy, true);
          assert.match(result.stdout, /installed[\s\S]*started[\s\S]*healthy/);
          const receipts = yield* coordinator.pendingReceipts;
          assert.equal(receipts.length, 1);
          yield* transport.json("receipt", receipts[0], FleetJob);
          assert.equal((yield* coordinator.job(job.id)).status, "succeeded");
          assert.equal((yield* coordinator.devices())[0]!.installed.demo?.version, version);
        }),
      ),
  );
  it.effect("refuses a corrupted artifact before starting the installer", () =>
    run(
      Effect.gen(function* () {
        const { fs, root, worker, issued, coordinator, transport, workerMetadata } = yield* setup;
        yield* fs.writeFileString(`${root}/build`, "abc123");
        const command = {
          executable: process.execPath,
          args: ["-e", "require('node:fs').writeFileSync(process.argv[1],'ran')", `${root}/ran`],
          cwd: root,
          timeoutSeconds: 10,
        };
        const configured: FleetWorkerConfig = {
          ...worker,
          applications: { demo: { install: command, verify: command, approval: "always" } },
        };
        const metadata = workerMetadata(configured, "environment-linux");
        yield* coordinator.poll(issued.sessionId, metadata);
        const artifact = yield* publishArtifact(`${root}/build`, {
          name: "demo",
          version: "abc123",
          os: metadata.os,
          arch: metadata.arch,
        });
        yield* fs.writeFileString(yield* blobPath(artifact.sha256), "tampered");
        yield* coordinator.enqueue("thread-one", {
          device: "linux-main",
          requestId: "corrupt",
          action: { kind: "deploy", application: "demo", sha256: artifact.sha256 },
        });
        const claim = yield* coordinator.poll(issued.sessionId, metadata);
        assert(claim);
        assert.equal(
          (yield* Effect.scoped(execute(configured, claim, transport)).pipe(Effect.result))._tag,
          "Failure",
        );
        assert.equal(yield* fs.exists(`${root}/ran`), false);
      }),
    ),
  );
  it.effect("bounds noisy command output and forces once approval for elevation", () =>
    run(
      Effect.gen(function* () {
        const { root, worker, coordinator, issued, transport, workerMetadata } = yield* setup;
        const configured: FleetWorkerConfig = {
          ...worker,
          tasks: {
            noisy: {
              approval: "always",
              command: {
                executable: process.execPath,
                args: ["-e", "process.stdout.write('x'.repeat(200000))"],
                cwd: root,
                timeoutSeconds: 10,
              },
            },
            elevated: {
              approval: "always",
              command: {
                executable: process.execPath,
                args: ["-e", "process.exit(0)"],
                cwd: root,
                timeoutSeconds: 10,
                requiresElevation: true,
              },
            },
          },
        };
        const metadata = workerMetadata(configured, "environment-linux");
        yield* coordinator.poll(issued.sessionId, metadata);
        yield* coordinator.enqueue("thread-one", {
          device: "linux-main",
          requestId: "noisy",
          action: { kind: "run", task: "noisy" },
        });
        const claim = yield* coordinator.poll(issued.sessionId, metadata);
        assert(claim);
        const result = yield* Effect.scoped(execute(configured, claim, transport));
        assert.equal(result.status, "succeeded");
        assert(result.truncated);
        assert(Buffer.byteLength(result.stdout) < MAX_OUTPUT_BYTES);
        yield* coordinator.receipt(issued.sessionId, {
          jobId: claim.job.id,
          lease: claim.job.lease!,
          result,
        });
        const elevated = yield* coordinator.enqueue("thread-one", {
          device: "linux-main",
          requestId: "elevated",
          action: { kind: "run", task: "elevated" },
        });
        assert.equal(elevated.status, "awaiting-approval");
        assert.equal(yield* coordinator.poll(issued.sessionId, metadata), null);
        assert.equal(
          (yield* Effect.scoped(
            execute(
              configured,
              {
                ...claim,
                job: { ...claim.job, action: { kind: "run", task: "elevated" }, approved: false },
              },
              transport,
            ),
          ).pipe(Effect.result))._tag,
          "Failure",
        );
      }),
    ),
  );
});
