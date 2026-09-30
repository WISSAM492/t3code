import { ConnectPeers } from "../cloud/ConnectPeers.ts";
import * as NodeCrypto from "node:crypto";
import {
  CommandId,
  EventId,
  ThreadId,
  type FleetJob,
  type ProviderApprovalDecision,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import { OrchestrationEngineService } from "../orchestration/Services/OrchestrationEngine.ts";
import { Coordinator, fail, fleetError } from "./Coordinator.ts";

const PREFIX = "connect-device:";
const requestId = (job: FleetJob) => `${PREFIX}${job.id}:${job.approvalRevision ?? 0}`;
export const isDeviceApproval = (id: string) => id.startsWith(PREFIX);

/** Device actions use the ordinary thread approval surface on every client. */
export const requestDeviceApproval = (job: FleetJob) =>
  Effect.gen(function* () {
    if (job.status !== "awaiting-approval") return false;
    const engine = yield* Effect.serviceOption(OrchestrationEngineService);
    if (Option.isNone(engine)) return false; // Standalone owner CLI retains its existing approval command.
    const id = requestId(job);
    const createdAt = new Date(yield* Clock.currentTimeMillis).toISOString();
    const peers = yield* Effect.serviceOption(ConnectPeers);
    const connected = Option.isSome(peers)
      ? yield* peers.value.list.pipe(Effect.orElseSucceed(() => []))
      : [];
    const label = connected.find((peer) => peer.environmentId === job.device)?.label ?? job.device;
    const action = job.action;
    const detail =
      action.kind === "exec"
        ? `${label}\n${action.command.executable} ${JSON.stringify(action.command.args)}\nWorking directory: ${action.command.cwd}${action.command.requiresElevation ? "\nAdministrator permission requested. OS elevation is still required." : ""}`
        : `${label}\n${JSON.stringify(action)}`;
    yield* engine.value.dispatch({
      type: "thread.activity.append",
      commandId: CommandId.make(id),
      threadId: ThreadId.make(job.threadId),
      createdAt,
      activity: {
        id: EventId.make(id),
        createdAt,
        tone: "approval",
        kind: "approval.requested",
        summary: "Device command needs approval",
        turnId: null,
        payload: {
          requestId: id,
          requestKind: "command",
          requestType: "command_execution_approval",
          detail,
          options: [
            { decision: "accept", label: "Allow once" },
            { decision: "decline", label: "Deny" },
          ],
        },
      },
    });
    return true;
  }).pipe(Effect.mapError(fleetError));

export const respondToDeviceApproval = (
  threadId: string,
  id: string,
  decision: ProviderApprovalDecision,
) =>
  Effect.gen(function* () {
    const coordinator = yield* Coordinator;
    const engine = yield* OrchestrationEngineService;
    const parts = id.slice(PREFIX.length).split(":");
    const job = yield* coordinator.job(parts[0] ?? "");
    if (job.threadId !== threadId || requestId(job) !== id)
      return yield* fail("This device approval is stale or belongs to another thread.");
    if (decision !== "accept" && decision !== "decline" && decision !== "cancel")
      return yield* fail("Device commands support approval for this action only.");
    const updated = yield* coordinator.approve(job.id, decision === "accept");
    const createdAt = new Date(yield* Clock.currentTimeMillis).toISOString();
    const eventId = NodeCrypto.randomUUID();
    yield* engine.dispatch({
      type: "thread.activity.append",
      commandId: CommandId.make(`connect-device:resolved:${eventId}`),
      threadId: ThreadId.make(threadId),
      createdAt,
      activity: {
        id: EventId.make(eventId),
        createdAt,
        tone: "approval",
        kind: "approval.resolved",
        summary: "Device approval resolved",
        turnId: null,
        payload: {
          requestId: id,
          requestKind: "command",
          requestType: "command_execution_approval",
          decision,
        },
      },
    });
    return updated;
  }).pipe(Effect.mapError(fleetError));
