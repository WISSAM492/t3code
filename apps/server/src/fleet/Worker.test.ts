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
  EnvironmentId,
  ProviderInstanceId,
  ThreadId,
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
import { McpInvocationContext } from "../mcp/McpInvocationContext.ts";
import { make as makeTools } from "../mcp/toolkits/fleet/handlers.ts";
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
    ["run", "read", "write", "transfer", "deploy"],
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

const invocation = {
  environmentId: EnvironmentId.make("home"),
  threadId: ThreadId.make("thread-one"),
  providerInstanceId: ProviderInstanceId.make("codex"),
  providerSessionId: "normal-codex-session",
  capabilities: new Set<never>(),
  issuedAt: 0,
};

describe("Fleet outbound worker and HTTP boundary", () => {
  it.effect(
    "stops an exact-build transfer when the source hash has changed, retaining the original",
    () =>
      run(
        Effect.gen(function* () {
          const { fs, root, coordinator, issued, worker, transport, workerMetadata } = yield* setup;
          yield* fs.writeFileString(`${root}/build`, "newer build");
          const jobs = yield* coordinator.transfer("thread-one", {
            requestId: "exact-build",
            from: "linux-main",
            fromPath: `${root}/build`,
            to: "linux-main",
            toPath: `${root}/installed`,
            expectedSha256: NodeCrypto.createHash("sha256").update("inspected build").digest("hex"),
            move: true,
          });
          const capture = yield* coordinator.poll(
            issued.sessionId,
            workerMetadata(worker, "environment-linux"),
          );
          assert(capture);
          const result = yield* runClaim(worker, capture, transport);
          assert.notEqual(result.status, "succeeded");
          yield* coordinator.receipt(issued.sessionId, {
            jobId: capture.job.id,
            lease: capture.job.lease!,
            result,
          });
          assert.equal((yield* coordinator.job(jobs.destination.id)).status, "cancelled");
          assert.equal((yield* coordinator.job(jobs.cleanup!.id)).status, "cancelled");
          assert.equal(yield* fs.readFileString(`${root}/build`), "newer build");
          assert.equal(yield* fs.exists(`${root}/installed`), false);
        }),
      ),
  );

  it.effect(
    "returns an ad hoc command receipt directly to the normal agent without a named task",
    () =>
      run(
        Effect.gen(function* () {
          const { root, worker, coordinator, issued, saveConfig, workerMetadata } = yield* setup;
          const configured = { ...worker, execution: "allow" as const };
          yield* saveConfig(configured);
          yield* coordinator.poll(
            issued.sessionId,
            workerMetadata(configured, "environment-linux"),
          );
          const command = {
            executable: process.execPath,
            args: ["-e", "console.log(JSON.stringify({answer:6*7,cwd:process.cwd()}))"],
            cwd: root,
            timeoutSeconds: 10,
          };
          const job = yield* coordinator.enqueue("thread-one", {
            device: "linux-main",
            requestId: "natural-command",
            action: { kind: "exec", command },
          });
          const tools = yield* makeTools;
          const [result] = yield* Effect.all(
            [
              tools.fleet_run({ device: "linux-main", requestId: "natural-command", command }),
              runCycle(
                "environment-linux",
                yield* HostProcessPlatform,
                yield* HostProcessArchitecture,
              ),
            ],
            { concurrency: "unbounded" },
          );
          assert.equal(result.operationId, job.id);
          assert.equal(result.status, "succeeded");
          assert.equal(JSON.parse(result.result!.stdout).answer, 42);
          assert.equal(JSON.parse(result.result!.stdout).cwd, root);
          assert.deepEqual(yield* coordinator.pendingReceipts, []);
        }).pipe(Effect.provideService(McpInvocationContext, invocation)),
      ),
  );

  it.effect(
    "finds a file by name, reads and compares its hash, and guards edits against stale contents",
    () =>
      run(
        Effect.gen(function* () {
          const { fs, root, coordinator, issued, worker, transport, workerMetadata } = yield* setup;
          yield* fs.makeDirectory(`${root}/Downloads`);
          yield* fs.writeFileString(`${root}/Downloads/report.txt`, "original report");
          const outside = yield* fs.makeTempDirectoryScoped();
          yield* fs.writeFileString(`${outside}/report-secret.txt`, "private");
          yield* fs.symlink(outside, `${root}/escape`);
          const perform = (
            action: Parameters<typeof coordinator.enqueue>[1]["action"],
            requestId: string,
          ) =>
            Effect.gen(function* () {
              const job = yield* coordinator.enqueue("thread-one", {
                device: "linux-main",
                requestId,
                action,
              });
              const claim = yield* coordinator.poll(
                issued.sessionId,
                workerMetadata(worker, "environment-linux"),
              );
              assert(claim);
              const result = yield* runClaim(worker, claim, transport);
              yield* coordinator.receipt(issued.sessionId, {
                jobId: job.id,
                lease: claim.job.lease!,
                result,
              });
              yield* coordinator.forgetReceipt(job.id);
              return result;
            });
          const search = yield* perform(
            { kind: "search", path: root, query: "report", recursive: true },
            "find-report",
          );
          assert.deepEqual(
            search.files?.map((file) => file.path),
            [`${root}/Downloads/report.txt`],
          );
          const read = yield* perform(
            { kind: "read", path: search.files![0]!.path },
            "read-report",
          );
          assert.equal(read.stdout, "original report");
          const stat = yield* perform(
            { kind: "stat", path: search.files![0]!.path },
            "compare-report",
          );
          assert.equal(read.file?.sha256, stat.file?.sha256);
          const written = yield* perform(
            {
              kind: "write",
              path: search.files![0]!.path,
              content: "updated report",
              expectedSha256: read.file!.sha256!,
            },
            "edit-report",
          );
          assert.equal(written.status, "succeeded", written.stderr);
          const stale = yield* perform(
            {
              kind: "write",
              path: search.files![0]!.path,
              content: "lost update",
              expectedSha256: read.file!.sha256!,
            },
            "stale-edit",
          );
          assert.notEqual(stale.status, "succeeded");
          assert.equal(yield* fs.readFileString(`${root}/Downloads/report.txt`), "updated report");
          const collision = yield* perform(
            { kind: "write", path: search.files![0]!.path, content: "replace" },
            "collision",
          );
          assert.notEqual(collision.status, "succeeded");
          const denied = yield* perform(
            { kind: "read", path: `${outside}/report-secret.txt` },
            "outside",
          );
          assert.notEqual(denied.status, "succeeded");
          for (let index = 0; index < 4; index++)
            yield* fs.writeFileString(`${root}/report-${index}`, "data");
          const bounded = yield* perform(
            { kind: "search", path: root, query: "report", recursive: true, limit: 2 },
            "bounded-search",
          );
          assert.equal(bounded.files?.length, 2);
          assert.equal(bounded.truncated, true);
        }),
      ),
  );

  it.effect(
    "moves between enrolled devices only after verified delivery and retains a changed source",
    () =>
      run(
        Effect.gen(function* () {
          const { fs, root, coordinator, issued, worker, transport, auth, workerMetadata } =
            yield* setup;
          const targetRoot = yield* fs.makeTempDirectoryScoped();
          const target = yield* auth.issueSession({ scopes: [AuthFleetDeviceScope] });
          yield* coordinator.enroll("windows-main", target.sessionId);
          yield* coordinator.grant(
            "thread-one",
            "windows-main",
            ["transfer"],
            Date.now() + 86400_000,
          );
          const targetWorker = {
            ...worker,
            tokenFile: `${targetRoot}/token`,
            readRoots: { project: targetRoot },
            writeRoots: { project: targetRoot },
          };
          const targetMetadata = workerMetadata(targetWorker, "environment-windows");
          const targetTransport = makeTransport(targetWorker, target.token);
          yield* coordinator.poll(target.sessionId, targetMetadata);
          for (const changed of [false, true]) {
            const sourcePath = `${root}/source-${changed}.txt`;
            const destinationPath = `${targetRoot}/received-${changed}.txt`;
            yield* fs.writeFileString(sourcePath, "exact bytes");
            const jobs = yield* coordinator.transfer("thread-one", {
              requestId: `move-${changed}`,
              from: "linux-main",
              fromPath: sourcePath,
              to: "windows-main",
              toPath: destinationPath,
              move: true,
            });
            assert(jobs.cleanup);
            assert.equal(yield* coordinator.poll(target.sessionId, targetMetadata), null);
            const capture = yield* coordinator.poll(
              issued.sessionId,
              workerMetadata(worker, "environment-linux"),
            );
            assert(capture);
            const captured = yield* Effect.scoped(execute(worker, capture, transport));
            yield* coordinator.receipt(issued.sessionId, {
              jobId: capture.job.id,
              lease: capture.job.lease!,
              result: captured,
            });
            assert.equal(
              yield* coordinator.poll(
                issued.sessionId,
                workerMetadata(worker, "environment-linux"),
              ),
              null,
            );
            const receive = yield* coordinator.poll(target.sessionId, targetMetadata);
            assert(receive);
            const received = yield* Effect.scoped(execute(targetWorker, receive, targetTransport));
            yield* coordinator.receipt(target.sessionId, {
              jobId: receive.job.id,
              lease: receive.job.lease!,
              result: received,
            });
            assert.equal(yield* fs.readFileString(sourcePath), "exact bytes");
            if (changed) yield* fs.writeFileString(sourcePath, "new local work");
            const cleanup = yield* coordinator.poll(
              issued.sessionId,
              workerMetadata(worker, "environment-linux"),
            );
            assert(cleanup);
            const result = yield* Effect.scoped(execute(worker, cleanup, transport)).pipe(
              Effect.result,
            );
            assert.equal(result._tag, changed ? "Failure" : "Success");
            assert.equal(yield* fs.exists(sourcePath), changed);
            if (changed) assert.equal(yield* fs.readFileString(sourcePath), "new local work");
            assert.equal(yield* fs.readFileString(destinationPath), "exact bytes");
            yield* coordinator.receipt(issued.sessionId, {
              jobId: cleanup.job.id,
              lease: cleanup.job.lease!,
              result:
                result._tag === "Success"
                  ? result.success
                  : {
                      status: "failed",
                      stdout: "",
                      stderr: "source retained",
                      exitCode: null,
                      truncated: false,
                    },
            });
          }
        }),
      ),
  );

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
