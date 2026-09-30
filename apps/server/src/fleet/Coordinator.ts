import * as NodeCrypto from "node:crypto";
import {
  FleetAction,
  FleetArtifact,
  FleetCapability,
  FleetClaim,
  FleetDevice,
  FleetError,
  FleetJob,
  FleetMetadata,
  FleetReceipt,
  FleetRequest,
  FleetResult,
  FleetTransferRequest,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export const LEASE_MS = 30_000;
export const MAX_ARTIFACT_BYTES = 1024 * 1024 * 1024;
export const MAX_OUTPUT_BYTES = 64 * 1024;
export const fail = (message: string) => Effect.fail(new FleetError({ message }));
export const fleetError = (error: unknown) =>
  error instanceof FleetError ? error : new FleetError({ message: "Fleet operation failed." });
const decodeJob = Schema.decodeUnknownEffect(Schema.fromJsonString(FleetJob));
const decodeMetadata = Schema.decodeUnknownEffect(Schema.fromJsonString(FleetMetadata));
const decodeArtifact = Schema.decodeUnknownEffect(Schema.fromJsonString(FleetArtifact));
const decodeCapabilities = Schema.decodeUnknownEffect(
  Schema.fromJsonString(Schema.Array(FleetCapability)),
);
const decodeInstalled = Schema.decodeUnknownEffect(
  Schema.fromJsonString(FleetDevice.fields.installed),
);
const decodeDevice = Schema.decodeUnknownEffect(FleetDevice);
const decodeResult = Schema.decodeUnknownEffect(Schema.fromJsonString(FleetResult));
const capability = (action: FleetAction): FleetCapability =>
  action.kind === "capture" || action.kind === "receive" || action.kind === "remove-source"
    ? "transfer"
    : action.kind === "exec"
      ? "run"
      : action.kind === "search" || action.kind === "stat"
        ? "read"
        : action.kind;

export const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const now = Clock.currentTimeMillis;
  const changes = yield* PubSub.sliding<void>(1);
  const changed = PubSub.publish(changes, undefined);
  const save = (job: FleetJob) =>
    sql`UPDATE fleet_jobs SET status=${job.status}, payload=${JSON.stringify(job)} WHERE id=${job.id}`;
  const jobById = (id: string) =>
    Effect.gen(function* () {
      const rows = yield* sql<{ payload: string }>`SELECT payload FROM fleet_jobs WHERE id=${id}`;
      if (!rows[0]) return yield* fail("Fleet job does not exist.");
      return yield* decodeJob(rows[0].payload);
    });
  const authorized = (threadId: string, device: string, action: FleetAction) =>
    Effect.gen(function* () {
      const time = yield* now;
      const grants = yield* sql<{
        capabilities: string;
      }>`SELECT capabilities FROM fleet_grants WHERE thread_id=${threadId} AND device=${device} AND expires_at>${time}`;
      const capabilities = grants[0] ? yield* decodeCapabilities(grants[0].capabilities) : [];
      return capabilities.includes(capability(action));
    });
  const requireGrant = (threadId: string, device: string, action: FleetAction) =>
    Effect.gen(function* () {
      if (!(yield* authorized(threadId, device, action)))
        return yield* fail(
          "Connect this device to your T3 Connect account to use it. Legacy workers need an explicit thread grant.",
        );
    });
  const deviceBySession = (sessionId: string) =>
    Effect.gen(function* () {
      const rows = yield* sql<{
        id: string;
        metadata: string | null;
      }>`SELECT id, metadata FROM fleet_devices WHERE session_id=${sessionId}`;
      if (!rows[0]) return yield* fail("Fleet device credential is not enrolled.");
      return {
        id: rows[0].id,
        metadata: rows[0].metadata ? yield* decodeMetadata(rows[0].metadata) : null,
      };
    });
  const artifact = (sha256: string) =>
    Effect.gen(function* () {
      const rows = yield* sql<{
        payload: string;
      }>`SELECT payload FROM fleet_artifacts WHERE sha256=${sha256}`;
      return rows[0] ? yield* decodeArtifact(rows[0].payload) : null;
    });
  const enqueue = (threadId: string, request: FleetRequest) =>
    sql.withTransaction(
      Effect.gen(function* () {
        yield* requireGrant(threadId, request.device, request.action);
        const previous = yield* sql<{
          payload: string;
        }>`SELECT payload FROM fleet_jobs WHERE thread_id=${threadId} AND request_id=${request.requestId}`;
        if (previous[0]) {
          const job = yield* decodeJob(previous[0].payload);
          if (
            job.device !== request.device ||
            JSON.stringify(job.action) !== JSON.stringify(request.action)
          )
            return yield* fail("Request ID is already used for a different action.");
          return job;
        }
        const time = yield* now;
        const device = yield* sql<{
          metadata: string | null;
          session_id: string;
        }>`SELECT metadata, session_id FROM fleet_devices WHERE id=${request.device}`;
        if (!device[0]) return yield* fail("Unknown Fleet device.");
        const metadata = device[0].metadata ? yield* decodeMetadata(device[0].metadata) : null;
        if (metadata) {
          const action = request.action;
          if (action.kind === "run" && !metadata.tasks[action.task])
            return yield* fail("Device has no approved task with that name.");
          if (action.kind === "exec" && (!metadata.execution || metadata.execution === "disabled"))
            return yield* fail("This device policy disables commands.");
          if (action.kind === "deploy" && !metadata.applications[action.application])
            return yield* fail("Device has no deployment recipe for that application.");
          if (
            (action.kind === "read" ||
              action.kind === "capture" ||
              action.kind === "search" ||
              action.kind === "stat") &&
            action.root !== undefined &&
            !metadata.readRoots.includes(action.root)
          )
            return yield* fail("Device has no approved read root with that name.");
          if (
            (action.kind === "receive" ||
              action.kind === "write" ||
              action.kind === "remove-source") &&
            action.root !== undefined &&
            !metadata.writeRoots.includes(action.root)
          )
            return yield* fail("Device has no approved write root with that name.");
        }
        let needsApproval = false;
        if (request.action.kind === "run")
          needsApproval = metadata?.tasks[request.action.task] !== "always";
        if (request.action.kind === "exec" && request.action.afterOperationId) {
          const prerequisite = yield* jobById(request.action.afterOperationId);
          if (prerequisite.threadId !== threadId)
            return yield* fail("A command can depend only on this thread's own operation.");
        }
        if (request.action.kind === "exec")
          needsApproval =
            (metadata
              ? metadata.execution !== "allow"
              : device[0].session_id !== `connect:${request.device}`) ||
            request.action.command.requiresElevation === true;
        if (request.action.kind === "deploy") {
          needsApproval = metadata?.applications[request.action.application] !== "always";
          const build = yield* artifact(request.action.sha256);
          if (!build || build.name !== request.action.application)
            return yield* fail("Publish a matching immutable application artifact first.");
          if (metadata && (metadata.os !== build.os || metadata.arch !== build.arch))
            return yield* fail("Artifact OS or architecture does not match the device.");
        }
        if (request.action.kind === "receive" || request.action.kind === "remove-source") {
          const source = yield* jobById(request.action.sourceJobId);
          if (source.threadId !== threadId || source.action.kind !== "capture")
            return yield* fail("Transfer source must be this thread's file capture.");
          if (request.action.kind === "remove-source") {
            const destination = yield* jobById(request.action.destinationJobId);
            if (
              destination.threadId !== threadId ||
              destination.action.kind !== "receive" ||
              destination.action.sourceJobId !== source.id ||
              request.device !== source.device ||
              request.action.path !== source.action.path ||
              request.action.root !== source.action.root
            )
              return yield* fail("A move can remove only its own captured source after delivery.");
          }
        }
        const job: FleetJob = {
          id: NodeCrypto.randomUUID(),
          threadId,
          requestId: request.requestId,
          device: request.device,
          action: request.action,
          status: needsApproval ? "awaiting-approval" : "queued",
          approved: false,
          approvedPolicyHash: null,
          capturedArtifact: null,
          createdAt: time,
          expiresAt: time + 7 * 86400_000,
          lease: null,
          leaseExpiresAt: null,
          result: null,
        };
        yield* sql`INSERT INTO fleet_jobs (id, thread_id, request_id, device, status, payload) VALUES (${job.id}, ${threadId}, ${request.requestId}, ${request.device}, ${job.status}, ${JSON.stringify(job)})`;
        return job;
      }),
    );
  const registerArtifact = (build: FleetArtifact) =>
    sql.withTransaction(
      Effect.gen(function* () {
        const existing = yield* artifact(build.sha256);
        if (existing) {
          if (JSON.stringify(existing) !== JSON.stringify(build))
            return yield* fail("Artifact hash is already registered with different metadata.");
          return existing;
        }
        const old =
          yield* sql`SELECT sha256 FROM fleet_artifacts WHERE name=${build.name} AND version=${build.version} AND os=${build.os} AND arch=${build.arch}`;
        if (old.length)
          return yield* fail(
            "This exact application version and platform already names a different artifact.",
          );
        yield* sql`INSERT INTO fleet_artifacts (sha256, name, version, os, arch, payload) VALUES (${build.sha256}, ${build.name}, ${build.version}, ${build.os}, ${build.arch}, ${JSON.stringify(build)})`;
        return build;
      }),
    );
  const checkLease = (sessionId: string, id: string, lease: string) =>
    Effect.gen(function* () {
      const device = yield* deviceBySession(sessionId);
      const job = yield* jobById(id);
      if (
        job.device !== device.id ||
        job.lease !== lease ||
        job.status !== "running" ||
        (job.leaseExpiresAt ?? 0) <= (yield* now)
      )
        return yield* fail("Invalid or expired Fleet execution lease.");
      yield* requireGrant(job.threadId, job.device, job.action);
      if (job.action.kind === "receive" || job.action.kind === "remove-source") {
        const source = yield* jobById(job.action.sourceJobId);
        yield* requireGrant(source.threadId, source.device, source.action);
        if (job.action.kind === "remove-source") {
          const destination = yield* jobById(job.action.destinationJobId);
          yield* requireGrant(destination.threadId, destination.device, destination.action);
          if (
            destination.status !== "succeeded" ||
            !source.capturedArtifact ||
            destination.result?.artifact !== source.capturedArtifact.sha256
          )
            return yield* fail("Source removal requires a verified successful delivery.");
        }
      }
      return job;
    });
  const refresh = sql.withTransaction(
    Effect.gen(function* () {
      const time = yield* now;
      const rows = yield* sql<{
        payload: string;
      }>`SELECT payload FROM fleet_jobs WHERE status IN ('queued','awaiting-approval','running')`;
      for (const row of rows) {
        const job = yield* decodeJob(row.payload);
        if (job.status !== "running" && job.action.kind === "exec" && job.action.afterOperationId) {
          const prerequisite = yield* jobById(job.action.afterOperationId);
          if (["failed", "cancelled", "uncertain"].includes(prerequisite.status)) {
            yield* save({
              ...job,
              status: "cancelled",
              result: {
                status: "failed",
                stdout: "",
                stderr: "The prerequisite did not succeed; command was not run.",
                exitCode: null,
                truncated: false,
              },
            });
            continue;
          }
        }
        if (
          job.status === "queued" &&
          (job.action.kind === "receive" || job.action.kind === "remove-source")
        ) {
          const source = yield* jobById(job.action.sourceJobId);
          const destination =
            job.action.kind === "remove-source"
              ? yield* jobById(job.action.destinationJobId)
              : null;
          if (
            [source, destination].some(
              (dependency) =>
                dependency && ["failed", "cancelled", "uncertain"].includes(dependency.status),
            )
          ) {
            yield* save({
              ...job,
              status: "cancelled",
              result: {
                status: "failed",
                stdout: "",
                stderr: "Transfer prerequisite did not succeed; the source has been retained.",
                exitCode: null,
                truncated: false,
              },
            });
            continue;
          }
        }
        if (job.status === "running" && (job.leaseExpiresAt ?? 0) <= time)
          yield* save({ ...job, status: "uncertain" });
        else if (
          job.status !== "running" &&
          (job.expiresAt <= time || !(yield* authorized(job.threadId, job.device, job.action)))
        )
          yield* save({ ...job, status: "cancelled" });
      }
    }),
  );
  return {
    // Connect owns these bindings. Human labels never replace stable environment identities.
    connectDevices: (environmentIds: ReadonlyArray<string>, threadId?: string) =>
      sql
        .withTransaction(
          Effect.gen(function* () {
            const time = yield* now;
            const expires = time + 7 * 24 * 60 * 60 * 1000;
            const previous = yield* sql<{
              id: string;
            }>`SELECT id FROM fleet_devices WHERE session_id LIKE 'connect:%'`;
            for (const device of previous) {
              if (!environmentIds.includes(device.id))
                yield* sql`UPDATE fleet_grants SET expires_at=0 WHERE device=${device.id}`;
            }
            for (const id of environmentIds) {
              const binding = `connect:${id}`;
              yield* sql`INSERT INTO fleet_devices (id, session_id) VALUES (${id}, ${binding}) ON CONFLICT(id) DO NOTHING`;
              const rows = yield* sql<{
                session_id: string;
              }>`SELECT session_id FROM fleet_devices WHERE id=${id}`;
              if (rows[0]?.session_id !== binding)
                return yield* fail("Connect device identity collides with a legacy enrollment.");
              yield* sql`UPDATE fleet_grants SET expires_at=${expires} WHERE device=${id}`;
              if (threadId)
                yield* sql`INSERT INTO fleet_grants (thread_id, device, capabilities, expires_at) VALUES (${threadId}, ${id}, ${JSON.stringify(["run", "read", "write", "transfer"])}, ${expires}) ON CONFLICT(thread_id, device) DO UPDATE SET capabilities=excluded.capabilities, expires_at=excluded.expires_at`;
            }
          }),
        )
        .pipe(
          Effect.tap(() => changed),
          Effect.mapError(fleetError),
        ),
    authorized: (threadId: string, device: string, action: FleetAction) =>
      authorized(threadId, device, action).pipe(Effect.mapError(fleetError)),
    enroll: (id: string, sessionId: string) =>
      sql`INSERT INTO fleet_devices (id, session_id) VALUES (${id}, ${sessionId})`.pipe(
        Effect.asVoid,
        Effect.mapError(fleetError),
      ),
    rotateCredential: (id: string, sessionId: string) =>
      sql
        .withTransaction(
          Effect.gen(function* () {
            const rows = yield* sql<{
              session_id: string;
            }>`SELECT session_id FROM fleet_devices WHERE id=${id}`;
            if (!rows[0]) return yield* fail("Unknown Fleet device.");
            yield* sql`UPDATE fleet_devices SET session_id=${sessionId}, last_seen=NULL WHERE id=${id}`;
            return rows[0].session_id;
          }),
        )
        .pipe(Effect.mapError(fleetError)),
    grant: (
      threadId: string,
      device: string,
      capabilities: ReadonlyArray<FleetCapability>,
      expiresAt: number,
    ) =>
      sql`INSERT INTO fleet_grants (thread_id, device, capabilities, expires_at) VALUES (${threadId}, ${device}, ${JSON.stringify(capabilities)}, ${expiresAt}) ON CONFLICT(thread_id, device) DO UPDATE SET capabilities=excluded.capabilities, expires_at=excluded.expires_at`.pipe(
        Effect.asVoid,
        Effect.mapError(fleetError),
      ),
    revoke: (threadId: string, device: string) =>
      sql
        .withTransaction(
          Effect.gen(function* () {
            yield* sql`DELETE FROM fleet_grants WHERE thread_id=${threadId} AND device=${device}`;
            const jobs = yield* sql<{
              payload: string;
            }>`SELECT payload FROM fleet_jobs WHERE thread_id=${threadId} AND device=${device} AND status IN ('queued','awaiting-approval','running')`;
            for (const row of jobs) {
              const job = yield* decodeJob(row.payload);
              yield* save({ ...job, status: job.status === "running" ? "uncertain" : "cancelled" });
            }
          }),
        )
        .pipe(
          Effect.tap(() => changed),
          Effect.mapError(fleetError),
        ),
    devices: (threadId?: string) =>
      Effect.gen(function* () {
        const time = yield* now;
        const rows =
          threadId === undefined
            ? yield* sql<{
                id: string;
                metadata: string | null;
                last_seen: number | null;
                installed: string;
              }>`SELECT id, metadata, last_seen, installed FROM fleet_devices ORDER BY id`
            : yield* sql<{
                id: string;
                metadata: string | null;
                last_seen: number | null;
                installed: string;
              }>`SELECT d.id, d.metadata, d.last_seen, d.installed FROM fleet_devices d JOIN fleet_grants g ON g.device=d.id WHERE g.thread_id=${threadId} AND g.expires_at>${time} ORDER BY d.id`;
        return yield* Effect.forEach(rows, (row) =>
          Effect.gen(function* () {
            const grants =
              threadId === undefined
                ? []
                : yield* sql<{
                    capabilities: string;
                  }>`SELECT capabilities FROM fleet_grants WHERE thread_id=${threadId} AND device=${row.id}`;
            return yield* decodeDevice({
              id: row.id,
              metadata: row.metadata ? JSON.parse(row.metadata) : null,
              lastSeen: row.last_seen,
              online: row.last_seen !== null && time - row.last_seen < LEASE_MS,
              installed: JSON.parse(row.installed),
              permissions:
                threadId === undefined
                  ? FleetCapability.literals
                  : grants[0]
                    ? yield* decodeCapabilities(grants[0].capabilities)
                    : [],
            });
          }),
        );
      }).pipe(Effect.mapError(fleetError)),
    enqueue: (threadId: string, request: FleetRequest) =>
      enqueue(threadId, request).pipe(Effect.mapError(fleetError)),
    transfer: (threadId: string, input: typeof FleetTransferRequest.Type) =>
      sql
        .withTransaction(
          Effect.gen(function* () {
            if (
              input.move &&
              input.from === input.to &&
              input.fromRoot === input.toRoot &&
              input.fromPath === input.toPath
            )
              return yield* fail("A move needs a different destination file.");
            const source = yield* enqueue(threadId, {
              requestId: `${input.requestId}.source`,
              device: input.from,
              action: {
                kind: "capture",
                ...(input.fromRoot ? { root: input.fromRoot } : {}),
                path: input.fromPath,
                ...(input.move ? { move: true } : {}),
                ...(input.expectedSha256 ? { expectedSha256: input.expectedSha256 } : {}),
              },
            });
            const destination = yield* enqueue(threadId, {
              requestId: `${input.requestId}.target`,
              device: input.to,
              action: {
                kind: "receive",
                ...(input.toRoot ? { root: input.toRoot } : {}),
                path: input.toPath,
                sourceJobId: source.id,
                ...(input.overwrite ? { overwrite: true } : {}),
              },
            });
            const cleanup = input.move
              ? yield* enqueue(threadId, {
                  requestId: `${input.requestId}.cleanup`,
                  device: input.from,
                  action: {
                    kind: "remove-source",
                    ...(input.fromRoot ? { root: input.fromRoot } : {}),
                    path: input.fromPath,
                    sourceJobId: source.id,
                    destinationJobId: destination.id,
                  },
                })
              : undefined;
            return { source, destination, ...(cleanup ? { cleanup } : {}) };
          }),
        )
        .pipe(Effect.mapError(fleetError)),
    jobs: (threadId?: string) =>
      Effect.gen(function* () {
        yield* refresh;
        const rows =
          threadId === undefined
            ? yield* sql<{
                payload: string;
              }>`SELECT payload FROM fleet_jobs ORDER BY rowid DESC LIMIT 100`
            : yield* sql<{
                payload: string;
              }>`SELECT payload FROM fleet_jobs WHERE thread_id=${threadId} ORDER BY rowid DESC LIMIT 100`;
        return yield* Effect.forEach(rows, (row) => decodeJob(row.payload));
      }).pipe(Effect.mapError(fleetError)),
    approve: (id: string, allow: boolean) =>
      sql
        .withTransaction(
          Effect.gen(function* () {
            const job = yield* jobById(id);
            if (job.status !== "awaiting-approval")
              return yield* fail("Only a pending approval can be decided.");
            yield* requireGrant(job.threadId, job.device, job.action);
            const rows = yield* sql<{
              metadata: string | null;
            }>`SELECT metadata FROM fleet_devices WHERE id=${job.device}`;
            const metadata = rows[0]?.metadata ? yield* decodeMetadata(rows[0].metadata) : null;
            if (allow && !metadata)
              return yield* fail("Device must connect before its action policy can be approved.");
            const updated: FleetJob = {
              ...job,
              status: allow ? "queued" : "cancelled",
              approved: allow,
              approvedPolicyHash: metadata?.policyHash ?? null,
            };
            yield* save(updated);
            return updated;
          }),
        )
        .pipe(
          Effect.tap(() => changed),
          Effect.mapError(fleetError),
        ),
    cancel: (id: string) =>
      sql
        .withTransaction(
          Effect.gen(function* () {
            const job = yield* jobById(id);
            if (job.status === "running")
              return yield* fail(
                "A running action has an uncertain outcome; revoke its thread grant to stop it.",
              );
            if (!["queued", "awaiting-approval"].includes(job.status))
              return yield* fail("Action is already terminal.");
            yield* save({ ...job, status: "cancelled" });
          }),
        )
        .pipe(
          Effect.tap(() => changed),
          Effect.mapError(fleetError),
        ),
    poll: (sessionId: string, metadata: FleetMetadata) =>
      sql
        .withTransaction(
          Effect.gen(function* () {
            const device = yield* deviceBySession(sessionId);
            if (device.metadata && device.metadata.environmentId !== metadata.environmentId)
              return yield* fail("Enrolled device identity changed.");
            const time = yield* now;
            yield* sql`UPDATE fleet_devices SET metadata=${JSON.stringify(metadata)}, last_seen=${time} WHERE id=${device.id}`;
            const rows = yield* sql<{
              payload: string;
            }>`SELECT payload FROM fleet_jobs WHERE device=${device.id} AND status IN ('queued','running') ORDER BY rowid`;
            let selected: FleetClaim | null = null;
            for (const row of rows) {
              const job = yield* decodeJob(row.payload);
              if (job.status === "running") {
                if ((job.leaseExpiresAt ?? 0) <= time) yield* save({ ...job, status: "uncertain" });
                else return null;
                continue;
              }
              if (
                job.expiresAt <= time ||
                !(yield* authorized(job.threadId, job.device, job.action))
              ) {
                yield* save({ ...job, status: "cancelled" });
                continue;
              }
              if (job.action.kind === "exec" && job.action.afterOperationId) {
                const prerequisite = yield* jobById(job.action.afterOperationId);
                if (["failed", "cancelled", "uncertain"].includes(prerequisite.status)) {
                  yield* save({
                    ...job,
                    status: "cancelled",
                    result: {
                      status: "failed",
                      stdout: "",
                      stderr: "The prerequisite did not succeed; command was not run.",
                      exitCode: null,
                      truncated: false,
                    },
                  });
                  continue;
                }
                if (prerequisite.status !== "succeeded") continue;
              }
              let build: FleetArtifact | null = null;
              if (job.action.kind === "receive" || job.action.kind === "remove-source") {
                const source = yield* jobById(job.action.sourceJobId);
                if (["failed", "cancelled", "uncertain"].includes(source.status)) {
                  yield* save({ ...job, status: "cancelled" });
                  continue;
                }
                if (source.status !== "succeeded" || !source.result?.artifact) continue;
                build = source.capturedArtifact;
                if (!(yield* authorized(source.threadId, source.device, source.action))) {
                  yield* save({ ...job, status: "cancelled" });
                  continue;
                }
              }
              if (job.action.kind === "remove-source") {
                const destination = yield* jobById(job.action.destinationJobId);
                if (["failed", "cancelled", "uncertain"].includes(destination.status)) {
                  yield* save({ ...job, status: "cancelled" });
                  continue;
                }
                if (destination.status !== "succeeded") continue;
                const source = yield* jobById(job.action.sourceJobId);
                const normalize = (value: string) =>
                  metadata.os === "windows" ? value.replaceAll("\\", "/").toLowerCase() : value;
                if (
                  source.device === destination.device &&
                  source.result?.file &&
                  destination.result?.file &&
                  normalize(source.result.file.path) === normalize(destination.result.file.path)
                ) {
                  yield* save({
                    ...job,
                    status: "failed",
                    result: {
                      status: "failed",
                      stdout: "",
                      stderr: "Source and destination are the same file; source retained.",
                      exitCode: null,
                      truncated: false,
                    },
                  });
                  continue;
                }
                if (
                  !build ||
                  destination.result?.artifact !== build.sha256 ||
                  !(yield* authorized(destination.threadId, destination.device, destination.action))
                ) {
                  yield* save({ ...job, status: "cancelled" });
                  continue;
                }
              }
              if (job.action.kind === "deploy") {
                build = yield* artifact(job.action.sha256);
                if (!build || metadata.os !== build.os || metadata.arch !== build.arch) {
                  yield* save({ ...job, status: "failed" });
                  continue;
                }
              }
              const approval =
                job.action.kind === "exec"
                  ? metadata.execution === "allow" && !job.action.command.requiresElevation
                    ? "always"
                    : metadata.execution === "ask" || metadata.execution === "allow"
                      ? "once"
                      : undefined
                  : job.action.kind === "run"
                    ? metadata.tasks[job.action.task]
                    : job.action.kind === "deploy"
                      ? metadata.applications[job.action.application]
                      : "always";
              if (approval === undefined) {
                yield* save({
                  ...job,
                  status: "failed",
                  result: {
                    status: "failed",
                    stdout: "",
                    stderr: "Device no longer allows that command, task, or application recipe.",
                    exitCode: null,
                    truncated: false,
                  },
                });
                continue;
              }
              if (
                approval !== "always" &&
                (!job.approved || job.approvedPolicyHash !== metadata.policyHash)
              ) {
                yield* save({
                  ...job,
                  status: "awaiting-approval",
                  approved: false,
                  approvalRevision: (job.approvalRevision ?? 0) + 1,
                });
                continue;
              }
              const claimed: FleetJob = {
                ...job,
                status: "running",
                lease: NodeCrypto.randomUUID(),
                leaseExpiresAt: time + LEASE_MS,
              };
              selected = { job: claimed, artifact: build };
              break;
            }
            if (selected) {
              yield* save(selected.job);
              if (selected.job.action.kind === "deploy") {
                const current = yield* sql<{
                  installed: string;
                }>`SELECT installed FROM fleet_devices WHERE id=${selected.job.device}`;
                const installed = yield* decodeInstalled(current[0]!.installed);
                const rest = { ...installed };
                delete rest[selected.job.action.application];
                yield* sql`UPDATE fleet_devices SET installed=${JSON.stringify(rest)} WHERE id=${selected.job.device}`;
              }
            }
            return selected;
          }),
        )
        .pipe(
          Effect.tap((claim) => (claim ? changed : Effect.void)),
          Effect.mapError(fleetError),
        ),
    renew: (sessionId: string, id: string, lease: string) =>
      sql
        .withTransaction(
          Effect.gen(function* () {
            const job = yield* checkLease(sessionId, id, lease);
            if (job.status !== "running")
              return yield* fail("Action lease expired. Report its receipt; do not rerun it.");
            yield* save({ ...job, leaseExpiresAt: (yield* now) + LEASE_MS });
            const device = yield* deviceBySession(sessionId);
            yield* sql`UPDATE fleet_devices SET last_seen=${yield* now} WHERE id=${device.id}`;
          }),
        )
        .pipe(Effect.mapError(fleetError)),
    receipt: (sessionId: string, receipt: FleetReceipt) =>
      sql
        .withTransaction(
          Effect.gen(function* () {
            const device = yield* deviceBySession(sessionId);
            const job = yield* jobById(receipt.jobId);
            if (job.device !== device.id || job.lease !== receipt.lease)
              return yield* fail("Receipt belongs to a different execution.");
            if (job.result) {
              if (JSON.stringify(job.result) !== JSON.stringify(receipt.result))
                return yield* fail("Execution receipt is immutable.");
              return job;
            }
            if (!["running", "uncertain"].includes(job.status))
              return yield* fail("Action has no live execution.");
            const result = receipt.result;
            if (
              Buffer.byteLength(result.stdout) > MAX_OUTPUT_BYTES ||
              Buffer.byteLength(result.stderr) > MAX_OUTPUT_BYTES
            )
              return yield* fail("Receipt output is too large.");
            if (
              result.status === "succeeded" &&
              job.action.kind === "capture" &&
              (!job.capturedArtifact || result.artifact !== job.capturedArtifact.sha256)
            )
              return yield* fail("Capture receipt does not match this job's uploaded bytes.");
            if (
              result.status === "succeeded" &&
              (job.action.kind === "receive" || job.action.kind === "remove-source")
            ) {
              const source = yield* jobById(job.action.sourceJobId);
              if (!source.capturedArtifact || result.artifact !== source.capturedArtifact.sha256)
                return yield* fail("Delivery receipt does not match the captured source hash.");
            }
            if (
              job.action.kind === "deploy" &&
              (result.status === "succeeded" || result.installedVersion !== undefined)
            ) {
              const build = yield* artifact(job.action.sha256);
              if (
                !build ||
                result.installedVersion !== build.version ||
                result.artifact !== build.sha256
              )
                return yield* fail("Installed version did not match the immutable artifact.");
              const rows = yield* sql<{
                installed: string;
              }>`SELECT installed FROM fleet_devices WHERE id=${device.id}`;
              const installed = yield* decodeInstalled(rows[0]!.installed);
              yield* sql`UPDATE fleet_devices SET installed=${JSON.stringify({ ...installed, [job.action.application]: { version: build.version, sha256: build.sha256, healthy: result.healthy === true } })} WHERE id=${device.id}`;
            }
            const updated: FleetJob = { ...job, status: result.status, result };
            yield* save(updated);
            return updated;
          }),
        )
        .pipe(
          Effect.tap(() => changed),
          Effect.mapError(fleetError),
        ),
    checkLease: (sessionId: string, id: string, lease: string) =>
      checkLease(sessionId, id, lease).pipe(Effect.mapError(fleetError)),
    captured: (sessionId: string, id: string, lease: string, build: FleetArtifact) =>
      sql
        .withTransaction(
          Effect.gen(function* () {
            const job = yield* checkLease(sessionId, id, lease);
            if (job.action.kind !== "capture" || job.status !== "running")
              return yield* fail("This action cannot upload files.");
            if (job.capturedArtifact && job.capturedArtifact.sha256 !== build.sha256)
              return yield* fail("A capture cannot change its uploaded artifact.");
            yield* save({ ...job, capturedArtifact: build });
            return build;
          }),
        )
        .pipe(Effect.mapError(fleetError)),
    artifact: (hash: string) => artifact(hash).pipe(Effect.mapError(fleetError)),
    job: (id: string) => refresh.pipe(Effect.andThen(jobById(id)), Effect.mapError(fleetError)),
    wait: (id: string, waitForApproval = false) =>
      Effect.gen(function* () {
        const subscription = yield* PubSub.subscribe(changes);
        while (true) {
          yield* refresh;
          const job = yield* jobById(id);
          if (
            !["queued", "running"].includes(job.status) &&
            !(waitForApproval && job.status === "awaiting-approval")
          )
            return job;
          const devices = yield* sql<{
            last_seen: number | null;
          }>`SELECT last_seen FROM fleet_devices WHERE id=${job.device}`;
          if (
            job.status === "queued" &&
            (devices[0]?.last_seen == null || (yield* now) - devices[0].last_seen >= LEASE_MS)
          )
            return job;
          yield* PubSub.take(subscription);
        }
      }).pipe(
        Effect.scoped,
        Effect.timeoutOption("25 seconds"),
        Effect.andThen(refresh),
        Effect.andThen(jobById(id)),
        Effect.mapError(fleetError),
      ),
    registerArtifact: (build: FleetArtifact) =>
      registerArtifact(build).pipe(Effect.mapError(fleetError)),
    recover:
      sql`UPDATE fleet_execution SET result=${JSON.stringify({ status: "uncertain", stdout: "", stderr: "Target restarted while action was running. Inspect before retrying.", exitCode: null, truncated: false } satisfies FleetResult)} WHERE result IS NULL`.pipe(
        Effect.asVoid,
        Effect.mapError(fleetError),
      ),
    pendingReceipts: sql<{
      job_id: string;
      lease: string;
      result: string;
    }>`SELECT job_id, lease, result FROM fleet_execution WHERE result IS NOT NULL`.pipe(
      Effect.flatMap((rows) =>
        Effect.forEach(rows, (row) =>
          decodeResult(row.result).pipe(
            Effect.map((result) => ({ jobId: row.job_id, lease: row.lease, result })),
          ),
        ),
      ),
      Effect.mapError(fleetError),
    ),
    pendingReceiptsFor: (coordinatorId: string) =>
      sql<{
        job_id: string;
        lease: string;
        result: string;
      }>`SELECT job_id, lease, result FROM fleet_execution WHERE result IS NOT NULL AND coordinator_id=${coordinatorId}`.pipe(
        Effect.flatMap((rows) =>
          Effect.forEach(rows, (row) =>
            decodeResult(row.result).pipe(
              Effect.map((result) => ({ jobId: row.job_id, lease: row.lease, result })),
            ),
          ),
        ),
        Effect.mapError(fleetError),
      ),
    forgetReceipt: (id: string) =>
      sql`DELETE FROM fleet_execution WHERE job_id=${id}`.pipe(
        Effect.asVoid,
        Effect.mapError(fleetError),
      ),
    begin: (job: FleetJob, coordinatorId = "") =>
      sql`INSERT INTO fleet_execution (job_id, lease, coordinator_id) VALUES (${job.id}, ${job.lease}, ${coordinatorId})`.pipe(
        Effect.asVoid,
        Effect.mapError(fleetError),
      ),
    finish: (id: string, result: FleetResult) =>
      sql`UPDATE fleet_execution SET result=${JSON.stringify(result)} WHERE job_id=${id}`.pipe(
        Effect.asVoid,
        Effect.mapError(fleetError),
      ),
  };
});

export class Coordinator extends Context.Service<Coordinator, Effect.Success<typeof make>>()(
  "t3/fleet/Coordinator",
) {}
export const layer = Layer.effect(Coordinator, make);
