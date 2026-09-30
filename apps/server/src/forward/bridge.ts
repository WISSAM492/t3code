// @effect-diagnostics nodeBuiltinImport:off
// Node's TCP adapter preserves half-closes; Effect's socket writers apply native backpressure.
import * as NodeNet from "node:net";
import * as NodeSocket from "@effect/platform-node/NodeSocket";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Socket from "effect/unstable/socket/Socket";

export const FORWARD_PATH = "/api/private-forward";
export const FORWARD_PROTOCOL = "t3-private-forward-v1";
export const MAX_FRAME_BYTES = 64 * 1024;

export class PrivateForwardError extends Schema.TaggedError<PrivateForwardError>()(
  "PrivateForwardError",
  { message: Schema.String },
) {}

export const connectLoopback = (port: number) =>
  Effect.acquireRelease(
    Effect.callback<NodeNet.Socket, PrivateForwardError>((resume) => {
      const socket = new NodeNet.Socket({ allowHalfOpen: true });
      socket.pause();
      socket.setNoDelay(true);
      socket.setTimeout(10_000);
      const onError = () =>
        resume(Effect.fail(new PrivateForwardError({ message: "Local service is unavailable." })));
      socket.on("error", onError);
      socket.once("timeout", () => {
        socket.destroy();
        onError();
      });
      socket.once("connect", () => {
        socket.setTimeout(0);
        resume(Effect.succeed(socket));
      });
      socket.connect({ host: "127.0.0.1", port });
      return Effect.sync(() => socket.destroy());
    }),
    (socket) => Effect.sync(() => socket.destroy()),
  );

/** Binary frames carry bytes; the sole text control frame sends a TCP FIN in one direction. */
export const bridge = Effect.fn("PrivateForward.bridge")(function* (
  tcp: NodeNet.Socket,
  ws: Socket.Socket,
) {
  const remote = yield* ws.reader;
  const writer = yield* ws.writer;
  const localSocket = yield* NodeSocket.fromDuplex(Effect.succeed(tcp));
  const local = yield* localSocket.reader;
  const localWriter = yield* localSocket.writer;
  let localEnded = false;
  let remoteEnded = false;
  const finish = Effect.suspend(() =>
    localEnded && remoteEnded ? writer.write(new Socket.CloseEvent(1000)) : Effect.void,
  );

  const toRemote = Effect.forever(
    Effect.gen(function* () {
      const chunks = yield* local.pull;
      for (const chunk of chunks) {
        const bytes = typeof chunk === "string" ? new TextEncoder().encode(chunk) : chunk;
        for (let offset = 0; offset < bytes.length; offset += MAX_FRAME_BYTES) {
          yield* writer.write(bytes.subarray(offset, offset + MAX_FRAME_BYTES));
        }
      }
    }),
  ).pipe(
    Effect.catch((error) => {
      if (error.reason._tag !== "SocketCloseError" || error.reason.code !== 1000)
        return Effect.fail(error);
      return Effect.gen(function* () {
        yield* writer.write("eof");
        localEnded = true;
        yield* finish;
        return yield* Effect.never;
      });
    }),
  );
  const toLocal = Effect.forever(
    Effect.gen(function* () {
      const chunks = yield* remote.pull;
      for (const chunk of chunks) {
        if (typeof chunk === "string") {
          if (chunk !== "eof" || remoteEnded)
            return yield* new PrivateForwardError({ message: "Invalid forward control frame." });
          yield* Effect.callback<void, PrivateForwardError>((resume) => {
            tcp.end(() => resume(Effect.void));
          });
          remoteEnded = true;
          yield* finish;
        } else {
          if (remoteEnded || chunk.length > MAX_FRAME_BYTES)
            return yield* new PrivateForwardError({ message: "Invalid forward data frame." });
          yield* localWriter.write(chunk);
        }
      }
    }),
  );
  yield* toRemote.pipe(
    Effect.raceFirst(toLocal),
    Effect.catch((error) => {
      if (
        error._tag === "SocketError" &&
        error.reason._tag === "SocketCloseError" &&
        error.reason.code === 1000
      )
        return Effect.void;
      return Effect.fail(error);
    }),
  );
});
