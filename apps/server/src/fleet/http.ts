import {
  AuthFleetDeviceScope,
  FleetMetadata,
  FleetReceipt,
  EnvironmentAuthInvalidError,
  EnvironmentScopeRequiredError,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import {
  HttpRouter,
  HttpServerRequest,
  HttpServerResponse,
  HttpServerRespondable,
} from "effect/unstable/http";
import * as EnvironmentAuth from "../auth/EnvironmentAuth.ts";
import { failEnvironmentAuthInvalid, failEnvironmentScopeRequired } from "../auth/http.ts";
import { Coordinator, fail } from "./Coordinator.ts";
import { blobPath, uploadChunk, discardUpload } from "./Artifacts.ts";

const authenticated = Effect.gen(function* () {
  const request = yield* HttpServerRequest.HttpServerRequest;
  if (request.headers.origin !== undefined || !request.headers.authorization?.startsWith("Bearer "))
    return yield* failEnvironmentAuthInvalid("missing_credential");
  const auth = yield* EnvironmentAuth.EnvironmentAuth;
  const session = yield* auth
    .authenticateHttpRequest(request)
    .pipe(
      Effect.catchIf(EnvironmentAuth.isServerAuthCredentialError, (error) =>
        failEnvironmentAuthInvalid(EnvironmentAuth.serverAuthCredentialReason(error)),
      ),
    );
  if (!session.scopes.includes(AuthFleetDeviceScope))
    return yield* failEnvironmentScopeRequired(AuthFleetDeviceScope);
  return session.sessionId;
});
const body = <A>(schema: Schema.Decoder<A, never>) =>
  Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest;
    // Small control requests only. Binary artifacts use the bounded streaming endpoint.
    const length = request.headers["content-length"];
    if (!length || !/^\d+$/.test(length) || Number(length) > 1024 * 1024)
      return yield* fail("Fleet control request must have a Content-Length of at most 1 MiB.");
    return yield* Schema.decodeUnknownEffect(schema)(yield* request.json);
  });
const route = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(
    Effect.map((value) =>
      HttpServerResponse.jsonUnsafe(value, { headers: { "cache-control": "no-store" } }),
    ),
    Effect.catch((error) =>
      error instanceof EnvironmentAuthInvalidError || error instanceof EnvironmentScopeRequiredError
        ? HttpServerRespondable.toResponse(error)
        : Effect.succeed(
            HttpServerResponse.jsonUnsafe(
              { error: "fleet_request_failed" },
              { status: 400, headers: { "cache-control": "no-store" } },
            ),
          ),
    ),
  );

export const routes = Layer.mergeAll(
  HttpRouter.add(
    "POST",
    "/api/fleet/poll",
    route(
      Effect.gen(function* () {
        const session = yield* authenticated;
        const coordinator = yield* Coordinator;
        return yield* coordinator.poll(session, yield* body(FleetMetadata));
      }),
    ),
  ),
  HttpRouter.add(
    "POST",
    "/api/fleet/receipt",
    route(
      Effect.gen(function* () {
        const session = yield* authenticated;
        const coordinator = yield* Coordinator;
        const receipt = yield* body(FleetReceipt);
        const job = yield* coordinator.receipt(session, receipt);
        if (job.action.kind === "capture") yield* discardUpload(job.id);
        return job;
      }),
    ),
  ),
  HttpRouter.add(
    "POST",
    "/api/fleet/renew",
    route(
      Effect.gen(function* () {
        const session = yield* authenticated;
        const coordinator = yield* Coordinator;
        const input = yield* body(Schema.Struct({ jobId: Schema.String, lease: Schema.String }));
        yield* coordinator.renew(session, input.jobId, input.lease);
        return { renewed: true };
      }),
    ),
  ),
  HttpRouter.add(
    "PUT",
    "/api/fleet/artifact",
    route(
      Effect.gen(function* () {
        const session = yield* authenticated;
        const request = yield* HttpServerRequest.HttpServerRequest;
        const coordinator = yield* Coordinator;
        const job = yield* coordinator.checkLease(
          session,
          request.headers["x-fleet-job"] ?? "",
          request.headers["x-fleet-lease"] ?? "",
        );
        if (job.action.kind !== "capture" || job.status !== "running")
          return yield* fail("Only a running file capture can upload an artifact.");
        const offset = request.headers["x-fleet-offset"];
        const final = request.headers["x-fleet-final"];
        if (!offset || !/^\d{1,10}$/.test(offset) || (final !== "0" && final !== "1"))
          return yield* fail("Supply a canonical upload offset and final flag.");
        const stored = yield* uploadChunk(job.id, Number(offset), final === "1", request.stream);
        // Recheck revocation after the upload, before registering any transferable content.
        yield* coordinator.checkLease(session, job.id, job.lease!);
        if ("uploadedBytes" in stored) return stored;
        return yield* coordinator.captured(session, job.id, job.lease!, stored);
      }),
    ),
  ),
  HttpRouter.add(
    "GET",
    "/api/fleet/artifact",
    Effect.gen(function* () {
      const session = yield* authenticated;
      const request = yield* HttpServerRequest.HttpServerRequest;
      const coordinator = yield* Coordinator;
      const job = yield* coordinator.checkLease(
        session,
        request.headers["x-fleet-job"] ?? "",
        request.headers["x-fleet-lease"] ?? "",
      );
      let hash: string | undefined;
      if (job.action.kind === "deploy") hash = job.action.sha256;
      if (job.action.kind === "receive") {
        const source = yield* coordinator.job(job.action.sourceJobId);
        hash = source.capturedArtifact?.sha256;
      }
      if (!hash) return yield* fail("This execution has no downloadable artifact.");
      const fs = yield* FileSystem.FileSystem;
      return HttpServerResponse.stream(fs.stream(yield* blobPath(hash)), {
        headers: { "content-type": "application/octet-stream", "cache-control": "no-store" },
      });
    }).pipe(
      Effect.catchTag(
        ["EnvironmentAuthInvalidError", "EnvironmentScopeRequiredError"],
        HttpServerRespondable.toResponse,
      ),
      Effect.catch(() => Effect.succeed(HttpServerResponse.empty({ status: 403 }))),
    ),
  ),
);
