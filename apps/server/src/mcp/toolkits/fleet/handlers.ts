import { ConnectPeers } from "../../../cloud/ConnectPeers.ts";
import { requestDeviceApproval } from "../../../fleet/Approvals.ts";
import * as Option from "effect/Option";
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
  const native = yield* Effect.serviceOption(ConnectPeers);
  const connected = Effect.gen(function* () {
    if (Option.isNone(native)) return [];
    const invocation = yield* McpInvocationContext;
    const peers = yield* native.value.list;
    yield* coordinator.connectDevices(
      peers.map((peer) => peer.environmentId),
      invocation.threadId,
    );
    return peers;
  });
  const complete = (id: string) =>
    Effect.gen(function* () {
      const job = yield* coordinator.wait(id);
      // Keep the normal tool call open while the user answers T3's approval card.
      // A slow/offline action still returns its durable operation ID for later inspection.
      const requested = yield* requestDeviceApproval(job);
      return operation(requested ? yield* coordinator.wait(id, true) : job);
    }).pipe(Effect.mapError(fleetError));
  const request = (
    device: string,
    action: FleetAction,
    requestId: string = NodeCrypto.randomUUID(),
  ) =>
    Effect.gen(function* () {
      const invocation = yield* McpInvocationContext;
      yield* connected;
      const job = yield* coordinator.enqueue(invocation.threadId, { requestId, device, action });
      return yield* complete(job.id);
    }).pipe(Effect.mapError(fleetError));
  return FleetToolkit.of({
    fleet_devices: () =>
      Effect.gen(function* () {
        const invocation = yield* McpInvocationContext;
        const peers = yield* connected;
        return (yield* coordinator.devices(invocation.threadId)).map((device) => ({
          label: peers.find((peer) => peer.environmentId === device.id)?.label ?? device.id,
          current: device.id === invocation.environmentId,
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
        yield* connected;
        const job = yield* coordinator.job(operationId);
        if (job.threadId !== invocation.threadId)
          return yield* fail("This operation belongs to a different thread.");
        return yield* complete(job.id);
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
        yield* connected;
        const transfer = yield* coordinator.transfer(invocation.threadId, {
          ...input,
          requestId: input.requestId ?? NodeCrypto.randomUUID(),
        });
        const job = transfer.cleanup ?? transfer.destination;
        return yield* complete(job.id);
      }),
  });
});
export const FleetToolkitHandlersLive = FleetToolkit.toLayer(make);
