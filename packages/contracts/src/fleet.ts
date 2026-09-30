import * as Schema from "effect/Schema";
import { ExecutionEnvironmentPlatformArch, ExecutionEnvironmentPlatformOs } from "./environment.ts";

const Name = Schema.String.check(Schema.isPattern(/^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,79}$/));
const FilePath = Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(4096));
export const FleetHash = Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/));
export const FleetCapability = Schema.Literals(["run", "read", "transfer", "deploy"]);
export type FleetCapability = typeof FleetCapability.Type;
export const FleetCommand = Schema.Struct({
  executable: Schema.String,
  args: Schema.Array(Schema.String),
  cwd: Schema.String,
  timeoutSeconds: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 3600 })),
  requiresElevation: Schema.optionalKey(Schema.Boolean),
});
export type FleetCommand = typeof FleetCommand.Type;
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
  tasks: Schema.Record(Name, FleetTask),
  applications: Schema.Record(Name, FleetApplication),
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
  Schema.Struct({ kind: Schema.Literal("read"), root: Name, path: FilePath }),
  Schema.Struct({ kind: Schema.Literal("capture"), root: Name, path: FilePath }),
  Schema.Struct({
    kind: Schema.Literal("receive"),
    root: Name,
    path: FilePath,
    sourceJobId: Schema.String,
  }),
  Schema.Struct({ kind: Schema.Literal("deploy"), application: Name, sha256: FleetHash }),
]);
export type FleetAction = typeof FleetAction.Type;
export const FleetRequest = Schema.Struct({ requestId: Name, device: Name, action: FleetAction });
export type FleetRequest = typeof FleetRequest.Type;
export const FleetTransferRequest = Schema.Struct({
  requestId: Name,
  from: Name,
  fromRoot: Name,
  fromPath: FilePath,
  to: Name,
  toRoot: Name,
  toPath: FilePath,
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
