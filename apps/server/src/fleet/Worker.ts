import * as NodeCrypto from "node:crypto";
import { HostProcessPlatform, HostProcessArchitecture } from "@t3tools/shared/hostProcess";
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
import { resolveForwardUrl } from "../forward/client.ts";
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

/** Resolve real paths on both sides of the boundary; a symlink cannot turn an allowed folder into a new capability. */
export const resolveRootPath = (
  roots: Readonly<Record<string, string>>,
  root: string,
  relative: string,
  writing = false,
) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const directory = roots[root];
    if (
      !directory ||
      !path.isAbsolute(directory) ||
      path.isAbsolute(relative) ||
      !relative ||
      relative.includes("\0")
    )
      return yield* fail("Use a configured root and a relative file path.");
    const base = yield* fs.realPath(directory);
    const candidate = path.resolve(base, relative);
    const inside = (value: string) => {
      const rel = path.relative(base, value);
      return (
        rel !== "" && !rel.startsWith(`..${path.sep}`) && rel !== ".." && !path.isAbsolute(rel)
      );
    };
    if (!inside(candidate)) return yield* fail("File escapes its approved root.");
    if (writing) {
      const parent = yield* fs.realPath(path.dirname(candidate));
      if (parent !== base && !inside(parent))
        return yield* fail("Destination folder escapes its approved root.");
      return path.join(parent, path.basename(candidate));
    }
    const real = yield* fs.realPath(candidate);
    if (!inside(real)) return yield* fail("File symlink escapes its approved root.");
    if ((yield* fs.stat(real)).type !== "File")
      return yield* fail("Fleet transfers regular files only.");
    return real;
  }).pipe(Effect.mapError(fleetError));

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
export const workerPolicyHash = (config: FleetWorkerConfig) =>
  NodeCrypto.createHash("sha256")
    .update(JSON.stringify(canonical(config)))
    .digest("hex");
export const workerMetadata = (
  config: FleetWorkerConfig,
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
});

export const makeTransport = (config: FleetWorkerConfig, token: string) => {
  const origin = resolveForwardUrl(config.coordinator);
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

export const execute = (config: FleetWorkerConfig, claim: FleetClaim, transport: Transport) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const action = claim.job.action;
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
      const file = yield* resolveRootPath(config.readRoots, action.root, action.path);
      if ((yield* fs.stat(file)).size > BigInt(MAX_OUTPUT_BYTES))
        return yield* fail("File is too large for readFile; use transfer instead.");
      return success({ stdout: yield* fs.readFileString(file) });
    }
    if (action.kind === "capture") {
      const file = yield* resolveRootPath(config.readRoots, action.root, action.path);
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
          return success({ artifact: build.sha256 });
        }
        hash.update(bytes);
        offset += bytes.byteLength;
      }
    }
    if (action.kind === "receive") {
      const destination = yield* resolveRootPath(config.writeRoots, action.root, action.path, true);
      const source = yield* download(claim, transport);
      const temporary = yield* fs.makeTempFileScoped({
        directory: path.dirname(destination),
        prefix: ".fleet-",
      });
      yield* Stream.run(fs.stream(source), fs.sink(temporary));
      yield* fs.chmod(temporary, 0o600);
      // Linking is atomic and refuses existing destinations, including symlinks.
      yield* fs.link(temporary, destination);
      return success({ artifact: claim.artifact!.sha256 });
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

export const runClaim = (config: FleetWorkerConfig, claim: FleetClaim, transport: Transport) =>
  Effect.gen(function* () {
    const coordinator = yield* Coordinator;
    yield* coordinator.begin(claim.job);
    const renew = Effect.gen(function* () {
      const current = yield* readWorkerConfig;
      if (!current || workerPolicyHash(current) !== workerPolicyHash(config))
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

const flushReceipts = (transport: Transport) =>
  Effect.gen(function* () {
    const coordinator = yield* Coordinator;
    for (const receipt of yield* coordinator.pendingReceipts) {
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

/** One outbound worker inside the existing T3 server; no extra listening socket or daemon. */
export const layer = Layer.effectDiscard(
  Effect.gen(function* () {
    const coordinator = yield* Coordinator;
    const identity = yield* ServerEnvironmentIdentity;
    const environmentId = yield* identity.getEnvironmentId;
    const platform = yield* HostProcessPlatform;
    const architecture = yield* HostProcessArchitecture;
    yield* coordinator.recover;
    let lastError = "";
    const cycle = runCycle(environmentId, platform, architecture).pipe(
      Effect.tap(() =>
        Effect.sync(() => {
          lastError = "";
        }),
      ),
      Effect.catch((error) =>
        Effect.gen(function* () {
          const message = fleetError(error).message;
          if (message !== lastError) {
            lastError = message;
            yield* Effect.logWarning(message);
          }
        }),
      ),
    );
    yield* cycle.pipe(Effect.repeat({ schedule: Schedule.spaced("5 seconds") }), Effect.forkScoped);
  }),
);
