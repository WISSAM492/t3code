import {
  FleetArtifact,
  FleetDevice,
  FleetError,
  FleetHash,
  FleetJob,
  FleetRequest,
  FleetTransferRequest,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Tool from "effect/unstable/ai/Tool";
import * as Toolkit from "effect/unstable/ai/Toolkit";
import { Coordinator } from "../../../fleet/Coordinator.ts";
import { McpInvocationContext } from "../../McpInvocationContext.ts";
import { ServerConfig } from "../../../config.ts";
const dependencies = [
  Coordinator,
  McpInvocationContext,
  FileSystem.FileSystem,
  Path.Path,
  ServerConfig,
];
const Devices = Tool.make("fleet_devices", {
  description:
    "List devices this AI thread has been granted access to, their platform, online status, approved task names, roots, and last verified installations. Empty means the owner must grant this thread access with t3 fleet grant.",
  success: Schema.Array(FleetDevice),
  failure: FleetError,
  dependencies,
}).annotate(Tool.Readonly, true);
const Status = Tool.make("fleet_status", {
  description:
    "Read the latest 20 Fleet jobs for this thread, or pass jobId for one specific job and more output. Output is bounded; truncated=true indicates omitted output. queued means waiting for an online target; uncertain means inspect before retrying. Never silently rerun uncertain actions. Only the owner can approve actions with t3 fleet approve.",
  parameters: Schema.Struct({ jobId: Schema.optionalKey(Schema.String) }),
  success: Schema.Array(FleetJob),
  failure: FleetError,
  dependencies,
}).annotate(Tool.Readonly, true);
const Run = Tool.make("fleet_run", {
  description:
    "Queue an owner-configured named task on a granted device. Use approved tasks for tests, logs and application management; executable paths, shell source and arguments cannot be supplied by the AI. Supply a unique requestId; reuse it only for the identical action to avoid duplicate execution.",
  parameters: Schema.Struct({
    requestId: FleetRequest.fields.requestId,
    device: FleetRequest.fields.device,
    task: Schema.String,
  }),
  success: FleetJob,
  failure: FleetError,
  dependencies,
}).annotate(Tool.Idempotent, true);
const Read = Tool.make("fleet_read_file", {
  description:
    "Queue reading a file within a device's named approved read root. path must be relative. Results (max 64 KiB) arrive in fleet_status. For large or binary files use fleet_transfer.",
  parameters: Schema.Struct({
    requestId: FleetRequest.fields.requestId,
    device: FleetRequest.fields.device,
    root: Schema.String,
    path: Schema.String,
  }),
  success: FleetJob,
  failure: FleetError,
  dependencies,
}).annotate(Tool.Idempotent, true);
const Transfer = Tool.make("fleet_transfer", {
  description:
    "Durably transfer a regular file between two granted devices through their existing T3 connection. Use named roots and relative paths. Offline recipients stay queued. SHA-256 is verified; existing destination files are never overwritten. Use the same requestId when resuming this exact transfer.",
  parameters: FleetTransferRequest,
  success: Schema.Struct({ source: FleetJob, destination: FleetJob }),
  failure: FleetError,
  dependencies,
}).annotate(Tool.Idempotent, true);
const Deploy = Tool.make("fleet_deploy", {
  description:
    "Queue an exact published application artifact on a granted device using its owner-configured platform recipe. Supply the SHA-256 returned by fleet_publish, never latest. Installation succeeds only when the target's actual version probe matches the immutable manifest. Start/health commands run if configured. Query fleet_status for approval and receipts. Request one job per platform/device with the corresponding artifact hash.",
  parameters: Schema.Struct({
    requestId: FleetRequest.fields.requestId,
    device: FleetRequest.fields.device,
    application: Schema.String,
    sha256: FleetHash,
  }),
  success: FleetJob,
  failure: FleetError,
  dependencies,
}).annotate(Tool.Idempotent, true);
const Publish = Tool.make("fleet_publish", {
  description:
    "Publish a build file from the coordinator's named approved read root. Use an immutable full Git commit or build version and explicit platform. Publication records SHA-256 and refuses changing bytes under an existing version/platform. The thread needs a deployment grant. Installer scripts are configured by the owner on each device; this tool does not invent installers.",
  parameters: Schema.Struct({
    device: FleetRequest.fields.device,
    root: Schema.String,
    path: Schema.String,
    name: FleetArtifact.fields.name,
    version: FleetArtifact.fields.version,
    os: FleetArtifact.fields.os,
    arch: FleetArtifact.fields.arch,
  }),
  success: FleetArtifact,
  failure: FleetError,
  dependencies,
}).annotate(Tool.Idempotent, true);
export const FleetToolkit = Toolkit.make(Devices, Status, Run, Read, Transfer, Deploy, Publish);
