import { PrivateForwardPolicy } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";

import { ServerConfig } from "../config.ts";

const decodePolicy = Schema.decodeUnknownEffect(Schema.fromJsonString(PrivateForwardPolicy));
const encodePolicy = Schema.encodeEffect(Schema.fromJsonString(PrivateForwardPolicy));

export const policyPath = Effect.gen(function* () {
  const config = yield* ServerConfig;
  const path = yield* Path.Path;
  return path.join(config.stateDir, "private-forward.json");
});

export const readPolicy = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const filePath = yield* policyPath;
  const json = yield* fs
    .readFileString(filePath)
    .pipe(
      Effect.catch((error) =>
        error.reason._tag === "NotFound"
          ? Effect.succeed('{"version":1,"ports":[]}')
          : Effect.fail(error),
      ),
    );
  return yield* decodePolicy(json);
});

// Replace the complete policy atomically. Approval is a host-local action, never a remote API.
export const writePolicy = Effect.fn("PrivateForward.writePolicy")(function* (
  ports: ReadonlyArray<number>,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const filePath = yield* policyPath;
  const json = yield* encodePolicy({
    version: 1,
    ports: [...new Set(ports)].sort((a, b) => a - b),
  });
  yield* Effect.scoped(
    Effect.gen(function* () {
      const temporaryPath = yield* fs.makeTempFileScoped({
        directory: path.dirname(filePath),
        prefix: ".private-forward-",
      });
      yield* fs.writeFileString(temporaryPath, `${json}\n`);
      yield* fs.chmod(temporaryPath, 0o600);
      yield* fs.rename(temporaryPath, filePath);
    }),
  );
});
