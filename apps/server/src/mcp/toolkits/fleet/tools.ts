import {
  FleetCommand,
  FleetDevice,
  FleetError,
  FleetHash,
  FleetMetadata,
  FleetOperation,
  FleetRequest,
  FleetTransferRequest,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import * as Tool from "effect/unstable/ai/Tool";
import * as Toolkit from "effect/unstable/ai/Toolkit";
import { Coordinator } from "../../../fleet/Coordinator.ts";
import { McpInvocationContext } from "../../McpInvocationContext.ts";
const dependencies = [Coordinator, McpInvocationContext];
const device = FleetRequest.fields.device;
const requestId = Schema.optionalKey(FleetRequest.fields.requestId);
const path = Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(4096));
const use =
  "Use device tools only when the user's request explicitly asks for or requires another device. Continue in the current T3 thread; no task or recipe setup is needed. ";
const completion =
  "Returns the result when completed within 25 seconds; otherwise use fleet_result with operationId. Offline devices stay queued. Never automatically repeat an uncertain action. Reuse requestId only for the identical request to avoid duplicate side effects.";
const Devices = Tool.make("fleet_devices", {
  description:
    use +
    "Discover devices already linked to the same T3 Connect account: stable IDs, human labels, OS, architecture, availability, command policy and starting paths. current identifies this thread's environment. Use labels and OS to choose the machine the user named; use its returned ID in device tools. No Fleet enrollment, joining or token setup is needed. An empty list means connect your environments through T3 Connect.",
  success: Schema.Array(
    Schema.Struct({
      id: FleetDevice.fields.id,
      label: Schema.String,
      current: Schema.Boolean,
      online: Schema.Boolean,
      os: Schema.NullOr(FleetMetadata.fields.os),
      arch: Schema.NullOr(FleetMetadata.fields.arch),
      permissions: FleetDevice.fields.permissions,
      execution: Schema.NullOr(Schema.Literals(["disabled", "ask", "allow"])),
      paths: Schema.NullOr(
        Schema.Struct({
          read: Schema.Record(Schema.String, Schema.String),
          write: Schema.Record(Schema.String, Schema.String),
        }),
      ),
    }),
  ),
  failure: FleetError,
  dependencies,
}).annotate(Tool.Readonly, true);
const Result = Tool.make("fleet_result", {
  description:
    "Wait for a device operation's result, up to 25 seconds. Only this thread's operations are accessible. awaiting-approval requires owner approval; queued means waiting for the device or a transfer prerequisite. succeeded means the target returned a verified receipt. uncertain means inspect before considering a new attempt.",
  parameters: Schema.Struct({ operationId: Schema.String }),
  success: FleetOperation,
  failure: FleetError,
  dependencies,
}).annotate(Tool.Readonly, true);
const Run = Tool.make("fleet_run", {
  description:
    use +
    "Execute a command on a connected Linux, macOS or Windows device as its normal T3 account. Supply an executable and argv; for shell syntax use that platform's shell explicitly (for example powershell.exe -NoProfile -NonInteractive -Command ... on Windows). cwd is an absolute target path. This supports tests, installers, app APIs, logs and normal development without predefined tasks. Prefer direct CLI/API calls over GUI interaction. For installations, transfer the exact build and verify the installed version afterwards. Supply afterOperationId to wait for a pending transfer or other prerequisite before running the command; a failed prerequisite cancels it. requiresElevation requests once approval; it does not grant OS administrator rights. Command permission permits normal account access beyond the file-tool roots. " +
    completion,
  parameters: Schema.Struct({
    requestId,
    device,
    command: FleetCommand,
    afterOperationId: Schema.optionalKey(Schema.String.check(Schema.isMinLength(1))),
  }),
  success: FleetOperation,
  failure: FleetError,
  dependencies,
});
const Search = Tool.make("fleet_search_files", {
  description:
    use +
    "List a directory or find filenames on a device as its normal T3 account. path is an absolute target directory. query is a case-insensitive filename substring; search is recursive by default when query is given. Symlinks are skipped. Results are bounded to 200 matches, 20,000 entries and 32 directory levels; truncated means narrow the directory or query. " +
    completion,
  parameters: Schema.Struct({
    requestId,
    device,
    path,
    query: Schema.optionalKey(Schema.String.check(Schema.isMaxLength(256))),
    recursive: Schema.optionalKey(Schema.Boolean),
    limit: Schema.optionalKey(Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 200 }))),
  }),
  success: FleetOperation,
  failure: FleetError,
  dependencies,
}).annotate(Tool.Readonly, true);
const Read = Tool.make("fleet_read_file", {
  description:
    use +
    "Read a text file (up to 64 KiB) at its absolute target path as its normal T3 account. Returns contents and SHA-256 for comparison or a guarded edit. Transfer larger or binary files to the current device and use the normal local tools. " +
    completion,
  parameters: Schema.Struct({ requestId, device, path }),
  success: FleetOperation,
  failure: FleetError,
  dependencies,
}).annotate(Tool.Readonly, true);
const Stat = Tool.make("fleet_stat_file", {
  description:
    use +
    "Get a file's size and SHA-256 without transferring it (up to 1 GiB). path is absolute and as its normal T3 account. Compare hashes across devices to compare their exact contents. " +
    completion,
  parameters: Schema.Struct({ requestId, device, path }),
  success: FleetOperation,
  failure: FleetError,
  dependencies,
}).annotate(Tool.Readonly, true);
const Write = Tool.make("fleet_write_file", {
  description:
    use +
    "Atomically write UTF-8 text up to 64 KiB at an absolute path as its normal T3 account. Parent directory must exist. Defaults to creating a new file and refuses collisions. For an edit, pass the expectedSha256 returned by reading the file; it replaces only if the current contents match. overwrite=true explicitly allows replacement without a hash check. Larger or binary files use fleet_transfer. " +
    completion,
  parameters: Schema.Struct({
    requestId,
    device,
    path,
    content: Schema.String.check(Schema.isMaxLength(64 * 1024)),
    expectedSha256: Schema.optionalKey(FleetHash),
    overwrite: Schema.optionalKey(Schema.Boolean),
  }),
  success: FleetOperation,
  failure: FleetError,
  dependencies,
});
const Transfer = Tool.make("fleet_transfer", {
  description:
    use +
    "Copy or move a file between devices over their authenticated T3 connections. fromPath/toPath are absolute native paths on the source and destination machines. Use move=true only when the user requests a move: source removal requires the source account's write permission and happens after hash-verified delivery, only if the source is unchanged. Defaults to copy; overwrite=true explicitly replaces the destination. Parent directories must exist; create them with fleet_run when authorized. Bytes are SHA-256 verified and limited to 1 GiB. Pass expectedSha256 to transfer the exact file/build inspected earlier. The current connected environment is included automatically for transfers to/from the local workspace. " +
    completion,
  parameters: Schema.Struct({
    requestId,
    from: FleetTransferRequest.fields.from,
    fromPath: path,
    to: FleetTransferRequest.fields.to,
    toPath: path,
    move: Schema.optionalKey(Schema.Boolean),
    overwrite: Schema.optionalKey(Schema.Boolean),
    expectedSha256: Schema.optionalKey(FleetHash),
  }),
  success: FleetOperation,
  failure: FleetError,
  dependencies,
});
export const FleetToolkit = Toolkit.make(Devices, Result, Run, Search, Read, Stat, Write, Transfer);
