import { AuthPrivateForwardScope } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schedule from "effect/Schedule";
import {
  HttpRouter,
  HttpServerRequest,
  HttpServerResponse,
  HttpServerRespondable,
} from "effect/unstable/http";

import * as EnvironmentAuth from "../auth/EnvironmentAuth.ts";
import * as SessionStore from "../auth/SessionStore.ts";
import {
  failEnvironmentAuthInvalid,
  failEnvironmentInternal,
  failEnvironmentScopeRequired,
} from "../auth/http.ts";
import { FORWARD_PATH, FORWARD_PROTOCOL, bridge, connectLoopback } from "./bridge.ts";
import { readPolicy } from "./Policy.ts";

export const FORWARD_CONNECTION_LIMIT = 64;

export const privateForwardRouteLayer = Layer.unwrap(
  Effect.sync(() => {
    let activeConnections = 0;
    return HttpRouter.add(
      "GET",
      `${FORWARD_PATH}/:port`,
      Effect.gen(function* () {
        const request = yield* HttpServerRequest.HttpServerRequest;
        const auth = yield* EnvironmentAuth.EnvironmentAuth;
        const sessions = yield* SessionStore.SessionStore;
        // Native helper only: neither ambient browser cookies nor URL credentials grant access.
        if (request.headers.origin !== undefined || request.headers.authorization === undefined) {
          return yield* failEnvironmentAuthInvalid("missing_credential");
        }
        const session = yield* auth.authenticateHttpRequest(request).pipe(
          Effect.catchIf(EnvironmentAuth.isServerAuthCredentialError, (error) =>
            failEnvironmentAuthInvalid(EnvironmentAuth.serverAuthCredentialReason(error)),
          ),
          Effect.catchIf(EnvironmentAuth.isServerAuthInternalError, (error) =>
            failEnvironmentInternal("internal_error", error),
          ),
        );
        if (!session.scopes.includes(AuthPrivateForwardScope)) {
          return yield* failEnvironmentScopeRequired(AuthPrivateForwardScope);
        }
        const url = HttpServerRequest.toURL(request);
        const portString = Option.isSome(url)
          ? url.value.pathname.slice(`${FORWARD_PATH}/`.length)
          : "";
        // No host parameter, DNS lookup, arbitrary URL, or alternative numeric address syntax.
        if (
          !/^[1-9]\d{0,4}$/.test(portString) ||
          Number(portString) > 65535 ||
          (Option.isSome(url) && url.value.search !== "")
        ) {
          return HttpServerResponse.text("Invalid forward destination.", { status: 400 });
        }
        const port = Number(portString);
        const policy = yield* readPolicy.pipe(
          Effect.catch(() => failEnvironmentInternal("internal_error")),
        );
        if (!policy.ports.includes(port)) {
          return HttpServerResponse.text("Port has not been approved on this host.", {
            status: 403,
          });
        }
        if (
          request.headers.upgrade?.toLowerCase() !== "websocket" ||
          request.headers["sec-websocket-protocol"] !== FORWARD_PROTOCOL
        ) {
          return HttpServerResponse.text("A private-forward WebSocket is required.", {
            status: 400,
          });
        }
        const reserved = yield* Effect.acquireRelease(
          Effect.sync(() => {
            if (activeConnections >= FORWARD_CONNECTION_LIMIT) return false;
            activeConnections++;
            return true;
          }),
          (reserved) =>
            Effect.sync(() => {
              if (reserved) activeConnections--;
            }),
        );
        if (!reserved) {
          return HttpServerResponse.text("Forward connection limit reached.", { status: 429 });
        }
        const tcp = yield* connectLoopback(port);
        const socket = yield* request.upgrade;
        const recheck = Effect.gen(function* () {
          const current = yield* readPolicy;
          const active = yield* sessions.listActive();
          return (
            current.ports.includes(port) &&
            active.some((item) => item.sessionId === session.sessionId)
          );
        }).pipe(Effect.catch(() => Effect.succeed(false)));
        const untilRevoked = Effect.repeat(recheck, {
          schedule: Schedule.spaced("1 second"),
          while: (allowed) => allowed,
        });
        yield* bridge(tcp, socket).pipe(
          Effect.raceFirst(untilRevoked),
          Effect.catch(() => Effect.void),
        );
        return HttpServerResponse.empty();
      }).pipe(
        Effect.catchTag("PrivateForwardError", () =>
          Effect.succeed(HttpServerResponse.text("Local service is unavailable.", { status: 502 })),
        ),
        Effect.catchTag(
          [
            "EnvironmentAuthInvalidError",
            "EnvironmentInternalError",
            "EnvironmentScopeRequiredError",
          ],
          HttpServerRespondable.toResponse,
        ),
      ),
    );
  }),
);
