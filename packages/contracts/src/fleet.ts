import * as Schema from "effect/Schema";
import * as Effect from "effect/Effect";
import { ExecutionEnvironmentPlatformArch, ExecutionEnvironmentPlatformOs } from "./environment.ts";

const Name = Schema.String.check(Schema.isPattern(/^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,79}$/));
const FilePath = Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(4096));
export const FleetHash = Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/));
export const FleetCapability = Schema.Literals(["run", "read", "write", "transfer", "deploy"]);
export type FleetCapability = typeof FleetCapability.Type;
export const FleetCommand = Schema.Struct({
  executable: Schema.String,
  args: Schema.Array(Schema.String),
  cwd: Schema.String,
  timeoutSeconds: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 3600 })),
  requiresElevation: Schema.optionalKey(Schema.Boolean),
});
export type FleetCommand = typeof FleetCommand.Type;
// Preserve older configurations and durable actions; agent tools use device primitives.
export const FleetTask = Schema.Struct({
  command: FleetCommand,
  approval: Schema.Literals(["once", "always"]),
});
export const FleetApplication = Schema.Struct({
  install: FleetCommand,
  verify: FleetCommand,
  start: Schema.optionalKey(FleetCommand),
  health: Schema.optionalKey(FleetCommand),
  approval: Schema.Literals(["once", "always"]),
});
export const FleetWorkerConfig = Schema.Struct({
  version: Schema.Literal(1),
  coordinator: Schema.String,
  tokenFile: Schema.String,
  readRoots: Schema.Record(Name, Schema.String),
  writeRoots: Schema.Record(Name, Schema.String),
  // Older fixed-task configurations must explicitly opt into arbitrary commands.
  execution: Schema.optionalKey(Schema.Literals(["disabled", "ask", "allow"])),
  tasks: Schema.Record(Name, FleetTask).pipe(Schema.withDecodingDefault(Effect.succeed({}))),
  applications: Schema.Record(Name, FleetApplication).pipe(
    Schema.withDecodingDefault(Effect.succeed({})),
  ),
});
export type FleetWorkerConfig = typeof FleetWorkerConfig.Type;
export const FleetMetadata = Schema.Struct({
  environmentId: Schema.String,
  os: ExecutionEnvironmentPlatformOs,
  arch: ExecutionEnvironmentPlatformArch,
  t3Version: Schema.String,
  policyHash: FleetHash,
  tasks: Schema.Record(Name, Schema.Literals(["once", "always"])),
  applications: Schema.Record(Name, Schema.Literals(["once", "always"])),
  readRoots: Schema.Array(Name),
  writeRoots: Schema.Array(Name),
  execution: Schema.optionalKey(Schema.Literals(["disabled", "ask", "allow"])),
  paths: Schema.optionalKey(
    Schema.Struct({
      read: Schema.Record(Name, Schema.String),
      write: Schema.Record(Name, Schema.String),
    }),
  ),
});
export type FleetMetadata = typeof FleetMetadata.Type;
export const FleetArtifact = Schema.Struct({
  sha256: FleetHash,
  size: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  name: Name,
  version: Schema.String.check(Schema.isPattern(/^[a-zA-Z0-9][a-zA-Z0-9_.+-]{0,127}$/)),
  os: ExecutionEnvironmentPlatformOs,
  arch: ExecutionEnvironmentPlatformArch,
});
export type FleetArtifact = typeof FleetArtifact.Type;
export const FleetAction = Schema.Union([
  Schema.Struct({ kind: Schema.Literal("run"), task: Name }),
  Schema.Struct({
    kind: Schema.Literal("exec"),
    command: FleetCommand,
    afterOperationId: Schema.optionalKey(Schema.String.check(Schema.isMinLength(1))),
  }),
  Schema.Struct({ kind: Schema.Literal("read"), root: Schema.optionalKey(Name), path: FilePath }),
  Schema.Struct({
    kind: Schema.Literal("search"),
    root: Schema.optionalKey(Name),
    path: FilePath,
    query: Schema.optionalKey(Schema.String.check(Schema.isMaxLength(256))),
    recursive: Schema.optionalKey(Schema.Boolean),
    limit: Schema.optionalKey(Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 200 }))),
  }),
  Schema.Struct({ kind: Schema.Literal("stat"), root: Schema.optionalKey(Name), path: FilePath }),
  Schema.Struct({
    kind: Schema.Literal("write"),
    root: Schema.optionalKey(Name),
    path: FilePath,
    content: Schema.String.check(Schema.isMaxLength(64 * 1024)),
    overwrite: Schema.optionalKey(Schema.Boolean),
    expectedSha256: Schema.optionalKey(FleetHash),
  }),
  Schema.Struct({
    kind: Schema.Literal("capture"),
    root: Schema.optionalKey(Name),
    path: FilePath,
    move: Schema.optionalKey(Schema.Boolean),
    expectedSha256: Schema.optionalKey(FleetHash),
  }),
  Schema.Struct({
    kind: Schema.Literal("receive"),
    root: Schema.optionalKey(Name),
    path: FilePath,
    sourceJobId: Schema.String,
    overwrite: Schema.optionalKey(Schema.Boolean),
  }),
  Schema.Struct({
    kind: Schema.Literal("remove-source"),
    root: Schema.optionalKey(Name),
    path: FilePath,
    sourceJobId: Schema.String,
    destinationJobId: Schema.String,
  }),
  Schema.Struct({ kind: Schema.Literal("deploy"), application: Name, sha256: FleetHash }),
]);
export type FleetAction = typeof FleetAction.Type;
export const FleetRequest = Schema.Struct({ requestId: Name, device: Name, action: FleetAction });
export type FleetRequest = typeof FleetRequest.Type;
export const FleetTransferRequest = Schema.Struct({
  requestId: Name,
  from: Name,
  fromRoot: Schema.optionalKey(Name),
  fromPath: FilePath,
  to: Name,
  toRoot: Schema.optionalKey(Name),
  toPath: FilePath,
  move: Schema.optionalKey(Schema.Boolean),
  overwrite: Schema.optionalKey(Schema.Boolean),
  expectedSha256: Schema.optionalKey(FleetHash),
});
export const FleetFile = Schema.Struct({
  path: FilePath,
  type: Schema.Literals(["file", "directory", "other"]),
  size: Schema.Number,
  sha256: Schema.optionalKey(FleetHash),
});
export const FleetResult = Schema.Struct({
  status: Schema.Literals(["succeeded", "failed", "uncertain"]),
  stdout: Schema.String,
  stderr: Schema.String,
  exitCode: Schema.NullOr(Schema.Int),
  truncated: Schema.Boolean,
  artifact: Schema.optionalKey(FleetHash),
  installedVersion: Schema.optionalKey(Schema.String),
  healthy: Schema.optionalKey(Schema.Boolean),
  file: Schema.optionalKey(FleetFile),
  files: Schema.optionalKey(Schema.Array(FleetFile)),
});
export type FleetResult = typeof FleetResult.Type;
export const FleetJob = Schema.Struct({
  id: Schema.String,
  threadId: Schema.String,
  requestId: Schema.String,
  device: Schema.String,
  action: FleetAction,
  status: Schema.Literals([
    "queued",
    "awaiting-approval",
    "running",
    "succeeded",
    "failed",
    "cancelled",
    "uncertain",
  ]),
  approved: Schema.Boolean,
  createdAt: Schema.Number,
  expiresAt: Schema.Number,
  lease: Schema.NullOr(Schema.String),
  leaseExpiresAt: Schema.NullOr(Schema.Number),
  approvedPolicyHash: Schema.NullOr(FleetHash),
  capturedArtifact: Schema.NullOr(FleetArtifact),
  result: Schema.NullOr(FleetResult),
});
export type FleetJob = typeof FleetJob.Type;
export const FleetOperation = Schema.Struct({
  operationId: FleetJob.fields.id,
  device: FleetJob.fields.device,
  status: FleetJob.fields.status,
  result: FleetJob.fields.result,
});
export const FleetDevice = Schema.Struct({
  id: Schema.String,
  metadata: Schema.NullOr(FleetMetadata),
  lastSeen: Schema.NullOr(Schema.Number),
  online: Schema.Boolean,
  permissions: Schema.Array(FleetCapability),
  installed: Schema.Record(
    Schema.String,
    Schema.Struct({ version: Schema.String, sha256: FleetHash, healthy: Schema.Boolean }),
  ),
});
export const FleetClaim = Schema.Struct({ job: FleetJob, artifact: Schema.NullOr(FleetArtifact) });
export type FleetClaim = typeof FleetClaim.Type;
export const FleetReceipt = Schema.Struct({
  jobId: Schema.String,
  lease: Schema.String,
  result: FleetResult,
});
export type FleetReceipt = typeof FleetReceipt.Type;
export class FleetError extends Schema.TaggedError<FleetError>()("FleetError", {
  message: Schema.String,
}) {}
