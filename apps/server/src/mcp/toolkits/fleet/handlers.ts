import { FleetRequest } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { publishArtifact, readWorkerConfig } from "../../../fleet/Artifacts.ts";
import { Coordinator, fail, fleetError } from "../../../fleet/Coordinator.ts";
import { resolveRootPath } from "../../../fleet/Worker.ts";
import { McpInvocationContext } from "../../McpInvocationContext.ts";
import { FleetToolkit } from "./tools.ts";
const decodeRequest = Schema.decodeUnknownEffect(FleetRequest);

export const make = Effect.gen(function* () {
  const coordinator = yield* Coordinator;
  const request = (input: unknown) =>
    Effect.gen(function* () {
      const invocation = yield* McpInvocationContext;
      const decoded = yield* decodeRequest(input);
      return yield* coordinator.enqueue(invocation.threadId, decoded);
    }).pipe(Effect.mapError(fleetError));
  return FleetToolkit.of({
    fleet_devices: () =>
      Effect.flatMap(McpInvocationContext, (invocation) =>
        coordinator.devices(invocation.threadId),
      ),
    fleet_status: ({ jobId }) =>
      Effect.gen(function* () {
        const invocation = yield* McpInvocationContext;
        let jobs = jobId
          ? [yield* coordinator.job(jobId)]
          : (yield* coordinator.jobs(invocation.threadId)).slice(0, 20);
        if (jobs.some((job) => job.threadId !== invocation.threadId))
          return yield* fail("This job belongs to a different thread.");
        const limit = jobId ? 4096 : 128;
        const cut = (value: string) =>
          Buffer.byteLength(value) > limit
            ? `${Buffer.from(value)
                .subarray(0, limit - 4)
                .toString("utf8")}…`
            : value;
        jobs = jobs.map((job) =>
          job.result
            ? {
                ...job,
                result: {
                  ...job.result,
                  stdout: cut(job.result.stdout),
                  stderr: cut(job.result.stderr),
                  truncated:
                    job.result.truncated ||
                    Buffer.byteLength(job.result.stdout) > limit ||
                    Buffer.byteLength(job.result.stderr) > limit,
                },
              }
            : job,
        );
        while (jobs.length > 1 && Buffer.byteLength(JSON.stringify(jobs)) > 24 * 1024) jobs.pop();
        return jobs;
      }),
    fleet_run: ({ requestId, device, task }) =>
      request({ requestId, device, action: { kind: "run", task } }),
    fleet_read_file: ({ requestId, device, root, path }) =>
      request({ requestId, device, action: { kind: "read", root, path } }),
    fleet_transfer: (input) =>
      Effect.flatMap(McpInvocationContext, (invocation) =>
        coordinator.transfer(invocation.threadId, input),
      ),
    fleet_deploy: ({ requestId, device, application, sha256 }) =>
      request({ requestId, device, action: { kind: "deploy", application, sha256 } }),
    fleet_publish: ({ device, root, path, ...metadata }) =>
      Effect.gen(function* () {
        const invocation = yield* McpInvocationContext;
        if (
          !(yield* coordinator.authorized(invocation.threadId, device, {
            kind: "deploy",
            application: metadata.name,
            sha256: "0".repeat(64),
          }))
        )
          return yield* fail("This thread needs a current deployment grant for that device.");
        const config = yield* readWorkerConfig;
        if (!config)
          return yield* fail(
            "Configure the coordinator's approved read roots before publishing artifacts.",
          );
        const file = yield* resolveRootPath(config.readRoots, root, path);
        return yield* publishArtifact(file, metadata);
      }).pipe(Effect.mapError(fleetError)),
  });
});
export const FleetToolkitHandlersLive = FleetToolkit.toLayer(make);
