// @effect-diagnostics nodeBuiltinImport:off - These tests exercise actual TCP and WebSocket boundaries.
import * as NodeHttp from "node:http";
import * as NodeNet from "node:net";
import * as NodeHttpServer from "@effect/platform-node/NodeHttpServer";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeSocket from "@effect/platform-node/NodeSocket";
import { assert, describe, expect, it } from "@effect/vitest";
import { AuthPrivateForwardScope, AuthStandardClientScopes } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Duration from "effect/Duration";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as TestClock from "effect/testing/TestClock";
import { HttpRouter, HttpServer } from "effect/unstable/http";

import * as EnvironmentAuth from "../auth/EnvironmentAuth.ts";
import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import * as ServerConfig from "../config.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import { FORWARD_PATH, FORWARD_PROTOCOL, MAX_FRAME_BYTES } from "./bridge.ts";
import { parseMapping, resolveForwardUrl, startForwardListener } from "./client.ts";
import { FORWARD_CONNECTION_LIMIT, privateForwardRouteLayer } from "./http.ts";
import { policyPath, readPolicy, writePolicy } from "./Policy.ts";

const authLayer = EnvironmentAuth.layer.pipe(
  Layer.provideMerge(SqlitePersistenceMemory),
  Layer.provideMerge(ServerSecretStore.layer),
  Layer.provide(ServerEnvironment.identityLayer),
);
const fixtureLayer = HttpRouter.serve(privateForwardRouteLayer, {
  disableListenLog: true,
  disableLogger: true,
}).pipe(
  Layer.provideMerge(authLayer),
  Layer.provideMerge(NodeHttpServer.layer(NodeHttp.createServer, { host: "127.0.0.1", port: 0 })),
  Layer.provideMerge(ServerConfig.layerTest(process.cwd(), { prefix: "t3-forward-test-" })),
  Layer.provideMerge(NodeServices.layer),
);

function portOf(server: NodeNet.Server | NodeHttp.Server): number {
  const address = server.address();
  assert(address !== null && typeof address !== "string");
  assert.equal(address.address, "127.0.0.1");
  return address.port;
}

const withFixture = <A, E, R>(run: (url: URL, token: string) => Effect.Effect<A, E, R>) =>
  Effect.scoped(
    Effect.gen(function* () {
      const server = yield* HttpServer.HttpServer;
      const address = server.address;
      assert("port" in address);
      const auth = yield* EnvironmentAuth.EnvironmentAuth;
      const issued = yield* auth.issueSession({ scopes: [AuthPrivateForwardScope] });
      return yield* run(new URL(`http://127.0.0.1:${address.port}`), issued.token);
    }),
  ).pipe(Effect.provide(fixtureLayer), TestClock.withLive);

const listen = <T extends NodeNet.Server | NodeHttp.Server>(server: T) =>
  Effect.acquireRelease(
    Effect.callback<T, Error>((resume) => {
      server.once("error", (error) => resume(Effect.fail(error)));
      server.listen(0, "127.0.0.1", () => resume(Effect.succeed(server)));
    }),
    (server) =>
      Effect.callback<void>((resume) => {
        server.close(() => resume(Effect.void));
      }),
  );

const forward = (url: URL, token: string, remotePort: number) =>
  Effect.gen(function* () {
    yield* writePolicy([remotePort]);
    return yield* startForwardListener({ url, token, mapping: { localPort: 0, remotePort } });
  });

const httpEcho = (port: number, body: Buffer) =>
  Effect.callback<Buffer, Error>((resume) => {
    const request = NodeHttp.request(
      { host: "127.0.0.1", port, method: "POST", path: "/nested?value=1", agent: false },
      (response) => {
        const chunks: Buffer[] = [];
        response.on("data", (chunk) => chunks.push(chunk));
        response.once("end", () => resume(Effect.succeed(Buffer.concat(chunks))));
        response.on("error", (error) => resume(Effect.fail(error)));
      },
    );
    request.setTimeout(10_000, () => request.destroy(new Error("request timed out")));
    request.on("error", (error) => resume(Effect.fail(error)));
    request.end(body);
    return Effect.sync(() => request.destroy());
  });

const openForwardSocket = (url: URL, token: string, port: number) =>
  Effect.acquireRelease(
    Effect.callback<NodeSocket.NodeWS.WebSocket, Error>((resume) => {
      const target = new URL(`${FORWARD_PATH}/${port}`, url);
      target.protocol = "ws:";
      const ws = new NodeSocket.NodeWS.WebSocket(target, FORWARD_PROTOCOL, {
        headers: { authorization: `Bearer ${token}` },
      });
      ws.once("open", () => resume(Effect.succeed(ws)));
      ws.on("error", (error) => resume(Effect.fail(error)));
      return Effect.sync(() => ws.terminate());
    }),
    (ws) => Effect.sync(() => ws.terminate()),
  );

const awaitClose = (ws: NodeSocket.NodeWS.WebSocket) =>
  Effect.callback<void>((resume) => {
    const onClose = () => resume(Effect.void);
    ws.once("close", onClose);
    return Effect.sync(() => ws.off("close", onClose));
  }).pipe(Effect.timeout("5 seconds"));

describe("private forwarding", () => {
  it.effect(
    "defaults to disabled and rejects unauthenticated, browser, and ordinary sessions",
    () =>
      withFixture((url, token) =>
        Effect.gen(function* () {
          assert.deepEqual((yield* readPolicy).ports, []);
          const auth = yield* EnvironmentAuth.EnvironmentAuth;
          const ordinary = yield* auth.issueSession({ scopes: AuthStandardClientScopes });
          const request = (headers: Record<string, string>) =>
            Effect.promise(() => fetch(new URL(`${FORWARD_PATH}/5173`, url), { headers }));
          assert.equal((yield* request({})).status, 401);
          assert.equal((yield* request({ authorization: "Bearer invalid" })).status, 401);
          assert.equal(
            (yield* request({
              authorization: `Bearer ${token}`,
              origin: "https://untrusted.example",
            })).status,
            401,
          );
          assert.equal((yield* request({ authorization: `Bearer ${ordinary.token}` })).status, 403);
          assert.equal((yield* request({ authorization: `Bearer ${token}` })).status, 403);
        }),
      ),
  );

  it.effect(
    "rejects arbitrary hosts, invalid ports, query credentials, and malformed approval files",
    () =>
      withFixture((url, token) =>
        Effect.gen(function* () {
          yield* writePolicy([5173]);
          const request = (path: string) =>
            Effect.promise(() =>
              fetch(new URL(path, url), { headers: { authorization: `Bearer ${token}` } }),
            );
          for (const path of [
            "0",
            "65536",
            "05173",
            "127.0.0.1:5173",
            "5173?host=192.168.1.10",
            "5173?wsTicket=secret",
          ]) {
            assert.equal((yield* request(`${FORWARD_PATH}/${path}`)).status, 400);
          }
          assert.equal((yield* request(`${FORWARD_PATH}/5173`)).status, 400);
          const fs = yield* FileSystem.FileSystem;
          yield* fs.writeFileString(yield* policyPath, '{"version":1,"ports":["8080"]}');
          assert.equal((yield* request(`${FORWARD_PATH}/5173`)).status, 500);
        }),
      ),
  );

  it.effect(
    "streams binary HTTP bodies, paths, and concurrent connections through a loopback listener",
    () =>
      withFixture((url, token) =>
        Effect.gen(function* () {
          const upstream = yield* listen(
            NodeHttp.createServer((request, response) => {
              expect(request.url).toBe("/nested?value=1");
              request.pipe(response);
            }),
          );
          const local = yield* forward(url, token, portOf(upstream));
          const payload = Buffer.alloc(2 * 1024 * 1024);
          for (let index = 0; index < payload.length; index++) payload[index] = index % 251;
          const received = yield* Effect.all(
            [
              httpEcho(portOf(local), payload),
              httpEcho(portOf(local), Buffer.from("second client")),
            ],
            { concurrency: "unbounded" },
          );
          assert(received[0]!.equals(payload));
          assert.equal(received[1]!.toString(), "second client");
        }),
      ),
  );

  it.effect("keeps the response direction alive after the client sends TCP FIN", () =>
    withFixture((url, token) =>
      Effect.gen(function* () {
        const upstream = yield* listen(
          NodeNet.createServer({ allowHalfOpen: true }, (tcp) => {
            const chunks: Buffer[] = [];
            tcp.on("data", (chunk) => chunks.push(chunk));
            tcp.on("end", () => tcp.end(Buffer.concat(chunks).toString().toUpperCase()));
          }),
        );
        const local = yield* forward(url, token, portOf(upstream));
        const received = yield* Effect.callback<string, Error>((resume) => {
          const tcp = NodeNet.createConnection({ host: "127.0.0.1", port: portOf(local) });
          const chunks: Buffer[] = [];
          tcp.on("data", (chunk) => chunks.push(chunk));
          tcp.on("end", () => resume(Effect.succeed(Buffer.concat(chunks).toString())));
          tcp.on("error", (error) => resume(Effect.fail(error)));
          tcp.on("connect", () => tcp.end("half closed request"));
          return Effect.sync(() => tcp.destroy());
        }).pipe(Effect.timeout("5 seconds"));
        assert.equal(received, "HALF CLOSED REQUEST");
      }),
    ),
  );

  it.effect("carries an inner WebSocket unchanged for HMR and Guacamole-style streams", () =>
    withFixture((url, token) =>
      Effect.gen(function* () {
        const upstream = yield* listen(NodeHttp.createServer());
        const wss = yield* Effect.acquireRelease(
          Effect.sync(() => new NodeSocket.NodeWS.WebSocketServer({ server: upstream })),
          (wss) => Effect.callback<void>((resume) => wss.close(() => resume(Effect.void))),
        );
        wss.on("connection", (ws) =>
          ws.on("message", (bytes, binary) => ws.send(bytes, { binary })),
        );
        const local = yield* forward(url, token, portOf(upstream));
        const result = yield* Effect.callback<Buffer, Error>((resume) => {
          const ws = new NodeSocket.NodeWS.WebSocket(
            `ws://127.0.0.1:${portOf(local)}/guacamole/websocket-tunnel`,
            "guacamole",
          );
          ws.on("open", () => ws.send(Buffer.from([0, 1, 128, 255])));
          ws.on("message", (data) => {
            ws.close();
            resume(Effect.succeed(Buffer.from(data as ArrayBuffer)));
          });
          ws.on("error", (error) => resume(Effect.fail(error)));
          return Effect.sync(() => ws.terminate());
        }).pipe(Effect.timeout("5 seconds"));
        assert.deepEqual([...result], [0, 1, 128, 255]);
      }),
    ),
  );

  it.effect("closes active forwards when host approval is removed", () =>
    withFixture((url, token) =>
      Effect.gen(function* () {
        const upstream = yield* listen(NodeNet.createServer((tcp) => tcp.on("error", () => {})));
        yield* writePolicy([portOf(upstream)]);
        const ws = yield* openForwardSocket(url, token, portOf(upstream));
        yield* Effect.all([awaitClose(ws), writePolicy([])], { concurrency: "unbounded" });
      }),
    ),
  );

  it.effect("closes active forwards when their credential is revoked", () =>
    withFixture((url) =>
      Effect.gen(function* () {
        const upstream = yield* listen(NodeNet.createServer((tcp) => tcp.on("error", () => {})));
        yield* writePolicy([portOf(upstream)]);
        const auth = yield* EnvironmentAuth.EnvironmentAuth;
        const issued = yield* auth.issueSession({ scopes: [AuthPrivateForwardScope] });
        const ws = yield* openForwardSocket(url, issued.token, portOf(upstream));
        yield* Effect.all([awaitClose(ws), auth.revokeSession(issued.sessionId)], {
          concurrency: "unbounded",
        });
      }),
    ),
  );

  it.effect("closes active forwards when a forwarding-only credential expires", () =>
    withFixture((url) =>
      Effect.gen(function* () {
        const upstream = yield* listen(NodeNet.createServer((tcp) => tcp.on("error", () => {})));
        yield* writePolicy([portOf(upstream)]);
        const auth = yield* EnvironmentAuth.EnvironmentAuth;
        const issued = yield* auth.issueSession({
          scopes: [AuthPrivateForwardScope],
          ttl: Duration.seconds(2),
        });
        const ws = yield* openForwardSocket(url, issued.token, portOf(upstream));
        yield* awaitClose(ws);
        const response = yield* Effect.promise(() =>
          fetch(new URL(`${FORWARD_PATH}/${portOf(upstream)}`, url), {
            headers: { authorization: `Bearer ${issued.token}` },
          }),
        );
        assert.equal(response.status, 401);
      }),
    ),
  );

  it.effect("reports an unavailable approved service before upgrading", () =>
    withFixture((url, token) =>
      Effect.gen(function* () {
        const reserved = yield* listen(NodeNet.createServer());
        const port = portOf(reserved);
        yield* Effect.callback<void>((resume) => {
          reserved.close(() => resume(Effect.void));
        });
        yield* writePolicy([port]);
        const status = yield* Effect.callback<number, Error>((resume) => {
          const target = new URL(`${FORWARD_PATH}/${port}`, url);
          target.protocol = "ws:";
          const ws = new NodeSocket.NodeWS.WebSocket(target, FORWARD_PROTOCOL, {
            headers: { authorization: `Bearer ${token}` },
          });
          ws.on("error", () => {});
          ws.on("unexpected-response", (_request, response) => {
            response.resume();
            ws.terminate();
            resume(Effect.succeed(response.statusCode!));
          });
          ws.on("open", () => resume(Effect.fail(new Error("Unexpected WebSocket upgrade"))));
          return Effect.sync(() => ws.terminate());
        }).pipe(Effect.timeout("5 seconds"));
        assert.equal(status, 502);
      }),
    ),
  );

  it.effect("limits active streams and releases capacity after disconnect", () =>
    withFixture((url, token) =>
      Effect.gen(function* () {
        const upstream = yield* listen(NodeNet.createServer((tcp) => tcp.on("error", () => {})));
        const port = portOf(upstream);
        yield* writePolicy([port]);
        const sockets = yield* Effect.all(
          Array.from({ length: FORWARD_CONNECTION_LIMIT }, () =>
            openForwardSocket(url, token, port),
          ),
          { concurrency: "unbounded" },
        );
        const status = yield* Effect.callback<number>((resume) => {
          const target = new URL(`${FORWARD_PATH}/${port}`, url);
          target.protocol = "ws:";
          const ws = new NodeSocket.NodeWS.WebSocket(target, FORWARD_PROTOCOL, {
            headers: { authorization: `Bearer ${token}` },
          });
          ws.on("error", () => {});
          ws.on("unexpected-response", (_request, response) => {
            response.resume();
            ws.terminate();
            resume(Effect.succeed(response.statusCode!));
          });
          return Effect.sync(() => ws.terminate());
        }).pipe(Effect.timeout("5 seconds"));
        assert.equal(status, 429);
        const first = sockets[0]!;
        yield* Effect.all([awaitClose(first), Effect.sync(() => first.close())], {
          concurrency: "unbounded",
        });
        // The close acknowledgement arrives after the handler releases its reserved slot.
        const next = yield* openForwardSocket(url, token, port);
        assert.equal(next.readyState, NodeSocket.NodeWS.WebSocket.OPEN);
      }),
    ),
  );

  it.effect("closes malformed and oversized streams instead of interpreting destinations", () =>
    withFixture((url, token) =>
      Effect.gen(function* () {
        const upstream = yield* listen(NodeNet.createServer((tcp) => tcp.on("error", () => {})));
        yield* writePolicy([portOf(upstream)]);
        for (const frame of ["connect 192.168.1.10:3389", Buffer.alloc(MAX_FRAME_BYTES + 1)]) {
          const ws = yield* openForwardSocket(url, token, portOf(upstream));
          yield* Effect.all([awaitClose(ws), Effect.sync(() => ws.send(frame))], {
            concurrency: "unbounded",
          });
        }
      }),
    ),
  );
});

describe("forward client inputs", () => {
  it("accepts explicit port mappings and TLS origins", () => {
    assert.deepEqual(parseMapping("15173:5173"), { localPort: 15173, remotePort: 5173 });
    assert.equal(resolveForwardUrl("https://home.example.com").origin, "https://home.example.com");
    assert.equal(resolveForwardUrl("http://127.0.0.1:3773").port, "3773");
  });
  it("rejects arbitrary destinations, unsafe credentials, and plaintext remote connections", () => {
    for (const value of ["0:5173", "1:65536", "1:localhost:80", "0x50:80", "80", "80:080"])
      expect(() => parseMapping(value)).toThrow();
    for (const value of [
      "http://192.168.1.1:3773",
      "http://home.example.com",
      "https://user:secret@example.com",
      "https://example.com/path",
      "https://example.com?token=secret",
      "ws://example.com",
    ])
      expect(() => resolveForwardUrl(value)).toThrow();
  });
});
