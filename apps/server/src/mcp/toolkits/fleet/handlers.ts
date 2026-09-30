import * as NodeCrypto from "node:crypto";
import { type FleetAction, type FleetJob } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import { Coordinator, fail, fleetError } from "../../../fleet/Coordinator.ts";
import { McpInvocationContext } from "../../McpInvocationContext.ts";
import { FleetToolkit } from "./tools.ts";
const operation = (job: FleetJob) => ({
  operationId: job.id,
  device: job.device,
  status: job.status,
  result: job.result,
});

export const make = Effect.gen(function* () {
  const coordinator = yield* Coordinator;
  const request = (
    device: string,
    action: FleetAction,
    requestId: string = NodeCrypto.randomUUID(),
  ) =>
    Effect.gen(function* () {
      const invocation = yield* McpInvocationContext;
      const job = yield* coordinator.enqueue(invocation.threadId, { requestId, device, action });
      return operation(yield* coordinator.wait(job.id));
    }).pipe(Effect.mapError(fleetError));
  return FleetToolkit.of({
    fleet_devices: () =>
      Effect.gen(function* () {
        const invocation = yield* McpInvocationContext;
        return (yield* coordinator.devices(invocation.threadId)).map((device) => ({
          id: device.id,
          online: device.online,
          permissions: device.permissions,
          os: device.metadata?.os ?? null,
          arch: device.metadata?.arch ?? null,
          execution: device.metadata?.execution ?? null,
          paths: device.metadata?.paths ?? null,
        }));
      }),
    fleet_result: ({ operationId }) =>
      Effect.gen(function* () {
        const invocation = yield* McpInvocationContext;
        const job = yield* coordinator.job(operationId);
        if (job.threadId !== invocation.threadId)
          return yield* fail("This operation belongs to a different thread.");
        return operation(yield* coordinator.wait(job.id));
      }),
    fleet_run: ({ requestId, device, command, afterOperationId }) =>
      request(
        device,
        { kind: "exec", command, ...(afterOperationId ? { afterOperationId } : {}) },
        requestId,
      ),
    fleet_search_files: ({ requestId, device, path, query, recursive, limit }) =>
      request(
        device,
        {
          kind: "search",
          path,
          ...(query !== undefined ? { query } : {}),
          recursive: recursive ?? query !== undefined,
          ...(limit !== undefined ? { limit } : {}),
        },
        requestId,
      ),
    fleet_read_file: ({ requestId, device, path }) =>
      request(device, { kind: "read", path }, requestId),
    fleet_stat_file: ({ requestId, device, path }) =>
      request(device, { kind: "stat", path }, requestId),
    fleet_write_file: ({ requestId, device, ...action }) =>
      request(device, { kind: "write", ...action }, requestId),
    fleet_transfer: (input) =>
      Effect.gen(function* () {
        const invocation = yield* McpInvocationContext;
        const transfer = yield* coordinator.transfer(invocation.threadId, {
          ...input,
          requestId: input.requestId ?? NodeCrypto.randomUUID(),
        });
        const job = transfer.cleanup ?? transfer.destination;
        return operation(yield* coordinator.wait(job.id));
      }),
  });
});
export const FleetToolkitHandlersLive = FleetToolkit.toLayer(make);
