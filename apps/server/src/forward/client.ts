// @effect-diagnostics nodeBuiltinImport:off
// The listener is deliberately a Node adapter: it binds loopback and bridges native TCP streams.
import * as NodeNet from "node:net";
import * as NodeSocket from "@effect/platform-node/NodeSocket";
import * as Effect from "effect/Effect";
import * as FiberSet from "effect/FiberSet";
import * as Socket from "effect/unstable/socket/Socket";
import { type PrivateForwardMapping } from "@t3tools/contracts";

import {
  bridge,
  FORWARD_PATH,
  FORWARD_PROTOCOL,
  MAX_FRAME_BYTES,
  PrivateForwardError,
} from "./bridge.ts";

export function parseMapping(value: string): PrivateForwardMapping {
  const match = /^([1-9]\d{0,4}):([1-9]\d{0,4})$/.exec(value);
  if (!match || Number(match[1]) > 65535 || Number(match[2]) > 65535) {
    throw new PrivateForwardError({ message: "Use --map LOCAL_PORT:REMOTE_PORT (ports 1–65535)." });
  }
  return { localPort: Number(match[1]), remotePort: Number(match[2]) };
}

export function resolveForwardUrl(value: string): URL {
  let url: URL;
  try {
    url = new URL(value);
  } catch {
    throw new PrivateForwardError({ message: "Use an HTTPS T3 Connect origin for --remote." });
  }
  const isLoopback = ["127.0.0.1", "localhost", "[::1]"].includes(url.hostname);
  if (
    (url.protocol !== "https:" && !(url.protocol === "http:" && isLoopback)) ||
    url.username ||
    url.password ||
    url.pathname !== "/" ||
    url.search ||
    url.hash
  ) {
    throw new PrivateForwardError({
      message:
        "Use an HTTPS origin, or HTTP on loopback, without credentials, a path, or query parameters.",
    });
  }
  return url;
}

const openWebSocket = (url: URL, token: string, remotePort: number) =>
  Effect.acquireRelease(
    Effect.callback<NodeSocket.NodeWS.WebSocket, PrivateForwardError>((resume) => {
      const target = new URL(`${FORWARD_PATH}/${remotePort}`, url);
      target.protocol = target.protocol === "https:" ? "wss:" : "ws:";
      const ws = new NodeSocket.NodeWS.WebSocket(target, FORWARD_PROTOCOL, {
        headers: { authorization: `Bearer ${token}` },
        followRedirects: false,
        handshakeTimeout: 10_000,
        maxPayload: MAX_FRAME_BYTES,
        perMessageDeflate: false,
      });
      let alive = true;
      const heartbeat = setInterval(() => {
        if (ws.readyState !== NodeSocket.NodeWS.WebSocket.OPEN) return;
        if (!alive) {
          ws.terminate();
          return;
        }
        alive = false;
        ws.ping();
      }, 30_000);
      heartbeat.unref();
      ws.on("pong", () => {
        alive = true;
      });
      ws.once("close", () => clearInterval(heartbeat));
      ws.once("open", () => resume(Effect.succeed(ws)));
      ws.on("error", () =>
        resume(
          Effect.fail(
            new PrivateForwardError({
              message:
                "Could not open private forward. Check the credential, host approval, and remote service.",
            }),
          ),
        ),
      );
      return Effect.sync(() => ws.terminate());
    }),
    (ws) => Effect.sync(() => ws.terminate()),
  );

export const startForwardListener = Effect.fn("PrivateForward.startListener")(function* (input: {
  readonly url: URL;
  readonly token: string;
  readonly mapping: PrivateForwardMapping;
}) {
  const connections = yield* FiberSet.make<void, never>();
  const run = yield* FiberSet.runtime(connections)();
  const sockets = new Set<NodeNet.Socket>();
  const server = yield* Effect.acquireRelease(
    Effect.sync(() =>
      NodeNet.createServer({ allowHalfOpen: true, highWaterMark: MAX_FRAME_BYTES }, (tcp) => {
        tcp.pause();
        tcp.setNoDelay(true);
        // Install before opening the WebSocket so an early local disconnect cannot leak a socket.
        tcp.on("error", () => {});
        sockets.add(tcp);
        tcp.once("close", () => sockets.delete(tcp));
        run(
          Effect.scoped(
            Effect.gen(function* () {
              const disconnected = Effect.callback<void>((resume) => {
                const onClose = () => resume(Effect.void);
                tcp.once("close", onClose);
                if (tcp.destroyed) resume(Effect.void);
                return Effect.sync(() => tcp.off("close", onClose));
              });
              yield* Effect.scoped(
                Effect.gen(function* () {
                  const ws = yield* openWebSocket(input.url, input.token, input.mapping.remotePort);
                  if (ws.protocol !== FORWARD_PROTOCOL) {
                    return yield* new PrivateForwardError({
                      message: "Remote server does not support private forwarding.",
                    });
                  }
                  const socket = yield* Socket.fromWebSocket(Effect.succeed(ws));
                  yield* bridge(tcp, socket);
                }),
              ).pipe(Effect.raceFirst(disconnected));
            }),
          ).pipe(
            Effect.catch(() =>
              Effect.logWarning(
                `Private forward to port ${input.mapping.remotePort} failed. Check the credential, host approval, and service.`,
              ),
            ),
            Effect.ensuring(Effect.sync(() => tcp.destroy())),
          ),
        );
      }),
    ),
    (listener) =>
      Effect.callback<void>((resume) => {
        for (const tcp of sockets) tcp.destroy();
        listener.close(() => resume(Effect.void));
      }),
  );
  const failed = yield* Effect.acquireRelease(
    Effect.sync(() => new Promise<Error>((resolve) => server.on("error", resolve))),
    () => Effect.sync(() => server.removeAllListeners("error")),
  );
  yield* Effect.callback<void, PrivateForwardError>((resume) => {
    server.listen({ host: "127.0.0.1", port: input.mapping.localPort }, () => resume(Effect.void));
  }).pipe(
    Effect.raceFirst(
      Effect.promise(() => failed).pipe(
        Effect.flatMap(() =>
          Effect.fail(
            new PrivateForwardError({
              message: `Could not bind 127.0.0.1:${input.mapping.localPort}. Choose a free local port.`,
            }),
          ),
        ),
      ),
    ),
  );
  return server;
});
