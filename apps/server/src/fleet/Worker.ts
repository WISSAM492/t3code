import { ConnectPeers } from "../cloud/ConnectPeers.ts";
import * as NodeCrypto from "node:crypto";
import {
  HostProcessPlatform,
  HostProcessArchitecture,
  HostProcessEnvironment,
  HostProcessWorkingDirectory,
} from "@t3tools/shared/hostProcess";
import {
  FleetArtifact,
  FleetClaim,
  FleetCommand,
  FleetError,
  FleetMetadata,
  FleetResult,
  type FleetWorkerConfig,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Option from "effect/Option";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import packageJson from "../../package.json" with { type: "json" };
import { ServerEnvironmentIdentity } from "../environment/ServerEnvironment.ts";
import { parseConnectOrigin } from "../cloud/origin.ts";
import { ProcessRunner } from "../processRunner.ts";
import { blobPath, readWorkerConfig, storeBlob, UPLOAD_CHUNK_BYTES } from "./Artifacts.ts";
import {
  Coordinator,
  MAX_OUTPUT_BYTES,
  MAX_ARTIFACT_BYTES,
  fail,
  fleetError,
} from "./Coordinator.ts";
const decodeArtifact = Schema.decodeUnknownEffect(FleetArtifact);

export { resolveRootPath } from "./Files.ts";
import { resolveRootPath, hashFile, searchFiles, writeFile } from "./Files.ts";

const canonical = (value: unknown): unknown =>
  Array.isArray(value)
    ? value.map(canonical)
    : value !== null && typeof value === "object"
      ? Object.fromEntries(
          Object.entries(value)
            .sort(([a], [b]) => a.localeCompare(b))
            .map(([key, item]) => [key, canonical(item)]),
        )
      : value;
export type WorkerPolicy = FleetWorkerConfig & { readonly connectPeer?: true };

export const workerPolicyHash = (config: WorkerPolicy) =>
  NodeCrypto.createHash("sha256")
    .update(JSON.stringify(canonical(config)))
    .digest("hex");
export const workerMetadata = (
  config: WorkerPolicy,
  environmentId: string,
  platform: NodeJS.Platform,
  architecture: NodeJS.Architecture,
): FleetMetadata => ({
  environmentId,
  os:
    platform === "win32"
      ? "windows"
      : platform === "darwin"
        ? "darwin"
        : platform === "linux"
          ? "linux"
          : "unknown",
  arch: architecture === "x64" ? "x64" : architecture === "arm64" ? "arm64" : "other",
  t3Version: packageJson.version,
  policyHash: workerPolicyHash(config),
  tasks: Object.fromEntries(
    Object.entries(config.tasks).map(([name, task]) => [
      name,
      task.command.requiresElevation ? "once" : task.approval,
    ]),
  ),
  applications: Object.fromEntries(
    Object.entries(config.applications).map(([name, app]) => [
      name,
      [app.install, app.verify, app.start, app.health].some((command) => command?.requiresElevation)
        ? "once"
        : app.approval,
    ]),
  ),
  readRoots: Object.keys(config.readRoots),
  writeRoots: Object.keys(config.writeRoots),
  execution: config.execution ?? "disabled",
  paths: { read: config.readRoots, write: config.writeRoots },
});

export const makeTransport = (config: FleetWorkerConfig, token: string) => {
  const origin = parseConnectOrigin(config.coordinator);
  if (!origin)
    throw new FleetError({ message: "Use an HTTPS Connect origin, or HTTP on loopback." });
  if (!token || /\s/.test(token))
    throw new FleetError({ message: "Invalid Fleet credential file." });
  const request = (endpoint: string, init: RequestInit = {}) =>
    Effect.tryPromise({
      try: (signal) =>
        fetch(new URL(`/api/fleet/${endpoint}`, origin), {
          ...init,
          signal,
          redirect: "error",
          headers: { ...init.headers, authorization: `Bearer ${token}` },
        }),
      catch: () => new FleetError({ message: "Fleet coordinator is unavailable." }),
    }).pipe(
      Effect.timeout(endpoint === "artifact" ? "5 minutes" : "15 seconds"),
      Effect.flatMap((response) =>
        response.ok
          ? Effect.succeed(response)
          : fail(`Fleet coordinator rejected request (${response.status}).`),
      ),
      Effect.mapError(fleetError),
    );
  const json = <A>(endpoint: string, value: unknown, schema: Schema.Decoder<A, never>) =>
    request(endpoint, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(value),
    }).pipe(
      Effect.flatMap((response) =>
        Effect.tryPromise({ try: () => response.json(), catch: fleetError }),
      ),
      Effect.flatMap(Schema.decodeUnknownEffect(schema)),
      Effect.timeout("15 seconds"),
      Effect.mapError(fleetError),
    );
  return { request, json };
};
export type Transport = ReturnType<typeof makeTransport>;
const success = (fields: Partial<FleetResult> = {}): FleetResult => ({
  status: "succeeded",
  stdout: "",
  stderr: "",
  exitCode: 0,
  truncated: false,
  ...fields,
});

const command = (input: FleetCommand, claim: FleetClaim, artifactPath?: string) =>
  Effect.gen(function* () {
    if (input.requiresElevation && !claim.job.approved)
      return yield* fail("Elevation requires approval for this exact action.");
    const path = yield* Path.Path;
    if (!path.isAbsolute(input.cwd))
      return yield* fail("Fleet command working directories must be absolute.");
    const runner = yield* ProcessRunner;
    // Artifact substitution is one argv value, never interpolated into shell source.
    const args = input.args.map((arg) => (arg === "{artifact}" ? (artifactPath ?? arg) : arg));
    const result = yield* runner.run({
      command: input.executable,
      args,
      cwd: input.cwd,
      timeout: `${input.timeoutSeconds} seconds`,
      maxOutputBytes: MAX_OUTPUT_BYTES / 4,
      outputMode: "truncate",
      timeoutBehavior: "timedOutResult",
    });
    return success({
      status: result.code === 0 && !result.timedOut ? "succeeded" : "failed",
      stdout: result.stdout,
      stderr: result.stderr,
      exitCode: result.code,
      truncated: result.stdoutTruncated || result.stderrTruncated,
    });
  }).pipe(Effect.mapError(fleetError));

const download = (claim: FleetClaim, transport: Transport) =>
  Effect.gen(function* () {
    const expected = claim.artifact;
    if (!expected) return yield* fail("No artifact was assigned to this execution.");
    const response = yield* transport.request("artifact", {
      headers: { "x-fleet-job": claim.job.id, "x-fleet-lease": claim.job.lease! },
    });
    if (!response.body) return yield* fail("Empty artifact response.");
    const stored = yield* storeBlob(
      Stream.fromReadableStream({ evaluate: () => response.body!, onError: fleetError }),
    ).pipe(Effect.timeout("5 minutes"));
    if (stored.sha256 !== expected.sha256 || stored.size !== expected.size)
      return yield* fail("Transferred artifact hash or size did not match the manifest.");
    return yield* blobPath(expected.sha256);
  });

const resolveWorkerPath = (
  config: WorkerPolicy,
  roots: Readonly<Record<string, string>>,
  root: string | undefined,
  input: string,
  writing = false,
  directory = false,
) =>
  Effect.gen(function* () {
    // Native Connect actions have the same normal-account access as a T3 terminal. Legacy policies keep their root restrictions.
    if (!config.connectPeer) return yield* resolveRootPath(roots, root, input, writing, directory);
    const path = yield* Path.Path;
    if (root || !path.isAbsolute(input))
      return yield* fail("Use an absolute path on the connected device.");
    if (input.includes("\0")) return yield* fail("Invalid device path.");
    const fs = yield* FileSystem.FileSystem;
    const candidate = path.resolve(input);
    if (writing)
      return path.join(yield* fs.realPath(path.dirname(candidate)), path.basename(candidate));
    const real = yield* fs.realPath(candidate);
    const info = yield* fs.stat(real);
    if (directory ? info.type !== "Directory" : info.type !== "File")
      return yield* fail(directory ? "Path must be a directory." : "Path must be a regular file.");
    return real;
  });

export const execute = (config: WorkerPolicy, claim: FleetClaim, transport: Transport) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const action = claim.job.action;
    if (action.kind === "exec") {
      const approval = config.execution ?? "disabled";
      if (approval === "disabled") return yield* fail("Commands are disabled on this device.");
      if (
        (approval === "ask" || action.command.requiresElevation) &&
        (!claim.job.approved || claim.job.approvedPolicyHash !== workerPolicyHash(config))
      )
        return yield* fail("This command requires owner approval under the current device policy.");
      return yield* command(action.command, claim);
    }
    if (action.kind === "search") {
      const base = yield* resolveWorkerPath(
        config,
        config.readRoots,
        action.root,
        action.path,
        false,
        true,
      );
      return success(yield* searchFiles(base, action));
    }
    if (action.kind === "stat") {
      const file = yield* resolveWorkerPath(config, config.readRoots, action.root, action.path);
      return success({ file: yield* hashFile(file) });
    }
    if (action.kind === "write") {
      if (Buffer.byteLength(action.content) > MAX_OUTPUT_BYTES)
        return yield* fail("Write content exceeds 64 KiB; transfer larger files instead.");
      const file = yield* resolveWorkerPath(
        config,
        config.writeRoots,
        action.root,
        action.path,
        true,
      );
      return success({
        file: yield* writeFile(
          file,
          Buffer.from(action.content),
          action.overwrite,
          action.expectedSha256,
        ),
      });
    }
    if (action.kind === "remove-source") {
      const expected = claim.artifact;
      if (!expected) return yield* fail("Move has no verified source manifest.");
      const file = yield* resolveWorkerPath(config, config.writeRoots, action.root, action.path);
      const original = action.root
        ? path.resolve(config.writeRoots[action.root]!, action.path)
        : action.path;
      if (Option.isSome(yield* fs.readLink(original).pipe(Effect.option)))
        return yield* fail("Move source is a symlink; its target has been retained.");
      const current = yield* hashFile(file);
      if (current.sha256 !== expected.sha256 || current.size !== expected.size)
        return yield* fail("Source changed after transfer; the source has been retained.");
      yield* fs.remove(file);
      return success({ artifact: current.sha256, file: current });
    }
    if (action.kind === "run") {
      const task = config.tasks[action.task];
      if (
        !task ||
        (task.approval !== "always" &&
          (!claim.job.approved || claim.job.approvedPolicyHash !== workerPolicyHash(config)))
      )
        return yield* fail("Task is not approved under the current device policy.");
      return yield* command(task.command, claim);
    }
    if (action.kind === "read") {
      const file = yield* resolveWorkerPath(config, config.readRoots, action.root, action.path);
      if ((yield* fs.stat(file)).size > BigInt(MAX_OUTPUT_BYTES))
        return yield* fail("File is too large for readFile; use transfer instead.");
      const bytes = yield* fs.readFile(file);
      return success({
        stdout: Buffer.from(bytes).toString("utf8"),
        file: {
          path: file,
          type: "file",
          size: bytes.byteLength,
          sha256: NodeCrypto.createHash("sha256").update(bytes).digest("hex"),
        },
      });
    }
    if (action.kind === "capture") {
      const file = yield* resolveWorkerPath(config, config.readRoots, action.root, action.path);
      if ((yield* fs.stat(file)).size > BigInt(MAX_ARTIFACT_BYTES))
        return yield* fail("File exceeds the 1 GiB artifact limit.");
      const input = yield* fs.open(file, { flag: "r" });
      const hash = NodeCrypto.createHash("sha256");
      let offset = 0;
      while (true) {
        const chunk = yield* input.readAlloc(UPLOAD_CHUNK_BYTES);
        const bytes = Option.getOrElse(chunk, () => new Uint8Array(0));
        const response = yield* transport.request("artifact", {
          method: "PUT",
          headers: {
            "x-fleet-job": claim.job.id,
            "x-fleet-lease": claim.job.lease!,
            "x-fleet-offset": String(offset),
            "x-fleet-final": Option.isNone(chunk) ? "1" : "0",
          },
          body: Buffer.from(bytes),
        });
        const result = yield* Effect.tryPromise({ try: () => response.json(), catch: fleetError });
        if (Option.isNone(chunk)) {
          const build = yield* decodeArtifact(result);
          if (build.sha256 !== hash.digest("hex") || build.size !== offset)
            return yield* fail("Uploaded bytes did not match the source file stream.");
          if (action.expectedSha256 && build.sha256 !== action.expectedSha256)
            return yield* fail("Source does not match the requested SHA-256; transfer stopped.");
          return success({
            artifact: build.sha256,
            file: { path: file, type: "file", size: offset, sha256: build.sha256 },
          });
        }
        hash.update(bytes);
        offset += bytes.byteLength;
      }
    }
    if (action.kind === "receive") {
      const destination = yield* resolveWorkerPath(
        config,
        config.writeRoots,
        action.root,
        action.path,
        true,
      );
      const source = yield* download(claim, transport);
      const temporary = yield* fs.makeTempFileScoped({
        directory: path.dirname(destination),
        prefix: ".fleet-",
      });
      yield* Stream.run(fs.stream(source), fs.sink(temporary));
      yield* fs.chmod(temporary, 0o600);
      if (action.overwrite) yield* fs.rename(temporary, destination);
      else yield* fs.link(temporary, destination);
      const received = yield* hashFile(destination);
      if (received.sha256 !== claim.artifact!.sha256)
        return yield* fail("Destination verification failed; the source has been retained.");
      return success({ artifact: received.sha256, file: received });
    }
    const recipe = config.applications[action.application];
    const build = claim.artifact;
    if (!recipe || !build || build.sha256 !== action.sha256 || build.name !== action.application)
      return yield* fail("No matching deployment recipe and artifact.");
    const metadata = workerMetadata(
      config,
      "",
      yield* HostProcessPlatform,
      yield* HostProcessArchitecture,
    );
    if (metadata.os !== build.os || metadata.arch !== build.arch)
      return yield* fail("Artifact does not match this device platform.");
    if (
      metadata.applications[action.application] !== "always" &&
      (!claim.job.approved || claim.job.approvedPolicyHash !== metadata.policyHash)
    )
      return yield* fail("Deployment requires approval for the current device policy.");
    const file = yield* download(claim, transport);
    const installation = yield* command(recipe.install, claim, file);
    if (installation.status !== "succeeded") return installation;
    const verification = yield* command(recipe.verify, claim, file);
    if (
      verification.status !== "succeeded" ||
      verification.truncated ||
      verification.stdout.trim() !== build.version
    )
      return {
        ...verification,
        status: "failed" as const,
        stderr: "Installed version verification did not match the requested version.",
      };
    let stdout = installation.stdout;
    let stderr = installation.stderr;
    let truncated = installation.truncated;
    if (recipe.start) {
      const started = yield* command(recipe.start, claim, file);
      if (started.status !== "succeeded") return started;
      stdout += started.stdout;
      stderr += started.stderr;
      truncated ||= started.truncated;
    }
    let healthy = false;
    if (recipe.health) {
      const health = yield* command(recipe.health, claim, file);
      if (health.status !== "succeeded")
        return {
          ...health,
          installedVersion: build.version,
          artifact: build.sha256,
          healthy: false,
        };
      healthy = true;
      stdout += health.stdout;
      stderr += health.stderr;
      truncated ||= health.truncated;
    }
    return success({
      stdout,
      stderr,
      truncated,
      installedVersion: build.version,
      artifact: build.sha256,
      healthy,
    });
  }).pipe(Effect.mapError(fleetError));

export const runClaim = (
  config: WorkerPolicy,
  claim: FleetClaim,
  transport: Transport,
  coordinatorId = "",
) =>
  Effect.gen(function* () {
    const coordinator = yield* Coordinator;
    yield* coordinator.begin(claim.job, coordinatorId);
    const renew = Effect.gen(function* () {
      const current = yield* readWorkerConfig;
      if (
        config.connectPeer
          ? current !== null
          : !current || workerPolicyHash(current) !== workerPolicyHash(config)
      )
        return yield* fail("Worker policy changed or worker was disabled.");
      yield* transport.json(
        "renew",
        { jobId: claim.job.id, lease: claim.job.lease },
        Schema.Unknown,
      );
    });
    const keepAlive = renew.pipe(
      Effect.repeat({ schedule: Schedule.spaced("5 seconds") }),
      Effect.andThen(Effect.never),
      Effect.delay("5 seconds"),
    );
    const result = yield* renew.pipe(
      Effect.andThen(
        Effect.scoped(execute(config, claim, transport)).pipe(Effect.raceFirst(keepAlive)),
      ),
      Effect.catch((error) =>
        Effect.succeed(
          success({
            status: "uncertain",
            exitCode: null,
            stderr: `${fleetError(error).message} Inspect the device before retrying.`,
          }),
        ),
      ),
    );
    const cut = (text: string) =>
      Buffer.byteLength(text) > MAX_OUTPUT_BYTES
        ? `${Buffer.from(text)
            .subarray(0, MAX_OUTPUT_BYTES - 4)
            .toString("utf8")}…`
        : text;
    const bounded = {
      ...result,
      stdout: cut(result.stdout),
      stderr: cut(result.stderr),
      truncated:
        result.truncated ||
        Buffer.byteLength(result.stdout) > MAX_OUTPUT_BYTES ||
        Buffer.byteLength(result.stderr) > MAX_OUTPUT_BYTES,
    };
    yield* coordinator.finish(claim.job.id, bounded);
    return bounded;
  });

const flushReceipts = (transport: Transport, coordinatorId = "") =>
  Effect.gen(function* () {
    const coordinator = yield* Coordinator;
    for (const receipt of yield* coordinator.pendingReceiptsFor(coordinatorId)) {
      yield* transport.json("receipt", receipt, Schema.Unknown);
      yield* coordinator.forgetReceipt(receipt.jobId);
    }
  });

export const runCycle = (
  environmentId: string,
  platform: NodeJS.Platform,
  architecture: NodeJS.Architecture,
) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const config = yield* readWorkerConfig;
    if (!config) return;
    const token = (yield* fs.readFileString(config.tokenFile)).trim();
    const transport = yield* Effect.try({
      try: () => makeTransport(config, token),
      catch: fleetError,
    });
    yield* flushReceipts(transport);
    const claim = yield* transport.json(
      "poll",
      workerMetadata(config, environmentId, platform, architecture),
      Schema.NullOr(FleetClaim),
    );
    if (claim) {
      yield* runClaim(config, claim, transport);
      yield* flushReceipts(transport);
    }
  });

/** The existing server also executes for its connected peers, including its own workspace. */
export const runConnectCycle = (
  environmentId: string,
  platform: NodeJS.Platform,
  architecture: NodeJS.Architecture,
) =>
  Effect.gen(function* () {
    const peers = yield* ConnectPeers;
    const coordinator = yield* Coordinator;
    const connected = yield* peers.list;
    yield* coordinator.connectDevices(connected.map((peer) => peer.environmentId));
    if (!connected.length) return;
    const environment = yield* HostProcessEnvironment;
    const cwd = yield* HostProcessWorkingDirectory;
    const home = (platform === "win32" ? environment.USERPROFILE : environment.HOME) ?? cwd;
    const configured = yield* readWorkerConfig;
    const config: WorkerPolicy = configured ?? {
      version: 1,
      coordinator: "connect",
      tokenFile: "",
      connectPeer: true,
      readRoots: { home, workspace: cwd },
      writeRoots: { home, workspace: cwd },
      execution: "allow",
      tasks: {},
      applications: {},
    };
    yield* Effect.forEach(
      connected,
      (peer) =>
        Effect.gen(function* () {
          const transport = peers.transport(peer.environmentId);
          yield* flushReceipts(transport, peer.environmentId);
          const claim = yield* transport.json(
            "poll",
            workerMetadata(config, environmentId, platform, architecture),
            Schema.NullOr(FleetClaim),
          );
          if (claim) {
            yield* runClaim(config, claim, transport, peer.environmentId);
            yield* flushReceipts(transport, peer.environmentId);
          }
        }).pipe(Effect.catch((error) => Effect.logDebug(fleetError(error).message))),
      { concurrency: 3, discard: true },
    );
  });

/** One outbound worker inside the existing T3 server; no extra listening socket or daemon. */
export const layer = Layer.effectDiscard(
  Effect.gen(function* () {
    const coordinator = yield* Coordinator;
    const identity = yield* ServerEnvironmentIdentity;
    const environmentId = yield* identity.getEnvironmentId;
    const platform = yield* HostProcessPlatform;
    const architecture = yield* HostProcessArchitecture;
    yield* coordinator.recover;
    const lastErrors = new Map<string, string>();
    const checked = <A, E, R>(name: string, effect: Effect.Effect<A, E, R>) =>
      effect.pipe(
        Effect.tap(() =>
          Effect.sync(() => {
            lastErrors.delete(name);
          }),
        ),
        Effect.catch((error) =>
          Effect.gen(function* () {
            const message = fleetError(error).message;
            if (message !== lastErrors.get(name)) {
              lastErrors.set(name, message);
              yield* Effect.logWarning(message);
            }
          }),
        ),
      );
    // An older relay must not disable an already configured legacy worker.
    const cycle = Effect.all(
      [
        checked("connect", runConnectCycle(environmentId, platform, architecture)),
        checked("legacy", runCycle(environmentId, platform, architecture)),
      ],
      { concurrency: 2, discard: true },
    );
    yield* cycle.pipe(Effect.repeat({ schedule: Schedule.spaced("5 seconds") }), Effect.forkScoped);
  }),
);
