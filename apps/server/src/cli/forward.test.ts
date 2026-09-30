import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NetService from "@t3tools/shared/Net";
import { AuthPrivateForwardScope } from "@t3tools/contracts";
import { assert, describe, it } from "@effect/vitest";
import * as ConfigProvider from "effect/ConfigProvider";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as TestConsole from "effect/testing/TestConsole";
import { Command } from "effect/unstable/cli";

import * as EnvironmentAuth from "../auth/EnvironmentAuth.ts";
import { ServerConfig, deriveServerPaths } from "../config.ts";
import { forwardCommand } from "./forward.ts";
import { resolveCliAuthConfig } from "./config.ts";
import * as Option from "effect/Option";

const runtime = Layer.mergeAll(
  NodeServices.layer,
  NetService.layer,
  TestConsole.layer,
  ConfigProvider.layer(ConfigProvider.fromEnv({ env: {} })),
);
const runCli = (args: ReadonlyArray<string>) =>
  Command.runWith(forwardCommand, { version: "test" })(args);

describe("t3 forward CLI", () => {
  it.effect("persists a replacement allowlist, lists it, and disables it in isolated state", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-forward-cli-" });
        const location = ["--base-dir", baseDir];
        yield* runCli(["list", ...location]);
        assert.equal((yield* TestConsole.logLines).at(-1), "Private forwarding is disabled.");
        yield* runCli(["allow", "8080", "5173", "5173", ...location]);
        const paths = yield* deriveServerPaths(baseDir, undefined);
        assert.deepEqual(
          JSON.parse(yield* fs.readFileString(`${paths.stateDir}/private-forward.json`)),
          { version: 1, ports: [5173, 8080] },
        );
        yield* runCli(["allow", "8080", ...location]);
        yield* runCli(["list", ...location]);
        assert.equal((yield* TestConsole.logLines).at(-1), "Approved localhost ports: 8080");
        yield* runCli(["disable", ...location]);
        assert.deepEqual(
          JSON.parse(yield* fs.readFileString(`${paths.stateDir}/private-forward.json`)).ports,
          [],
        );
      }),
    ).pipe(Effect.provide(runtime)),
  );

  it.effect("writes a private, forwarding-only credential and refuses to overwrite it", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-forward-token-" });
        const tokenPath = `${baseDir}/token`;
        const args = ["token", "--base-dir", baseDir, "--out", tokenPath, "--ttl", "1h"];
        yield* runCli(args);
        const token = (yield* fs.readFileString(tokenPath)).trim();
        assert(token.length > 0);
        const output = (yield* TestConsole.logLines).join("\n");
        assert(!output.includes(token));
        const config = yield* resolveCliAuthConfig(
          { baseDir: Option.some(baseDir) },
          Option.none(),
        );
        yield* Effect.gen(function* () {
          const auth = yield* EnvironmentAuth.EnvironmentAuth;
          const sessions = yield* auth.listSessions();
          assert.equal(sessions.length, 1);
          assert.deepEqual(sessions[0]!.scopes, [AuthPrivateForwardScope]);
        }).pipe(
          Effect.provide(
            EnvironmentAuth.runtimeLayer.pipe(Layer.provide(Layer.succeed(ServerConfig, config))),
          ),
        );
        assert.equal((yield* runCli(args).pipe(Effect.result))._tag, "Failure");
        assert.equal((yield* fs.readFileString(tokenPath)).trim(), token);
      }),
    ).pipe(Effect.provide(runtime)),
  );

  it.effect("rejects invalid ports before creating any host policy", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-forward-invalid-" });
        for (const port of ["0", "65536", "127.0.0.1:5173"]) {
          assert.equal(
            (yield* runCli(["allow", port, "--base-dir", baseDir]).pipe(Effect.result))._tag,
            "Failure",
          );
        }
        assert.equal(yield* fs.exists(`${baseDir}/userdata/private-forward.json`), false);
      }),
    ).pipe(Effect.provide(runtime)),
  );

  it.effect("rejects duplicate local ports and insecure remote origins before binding", () =>
    Effect.scoped(
      Effect.gen(function* () {
        assert.equal(
          (yield* runCli([
            "--remote",
            "http://home.example.com",
            "--token-file",
            "/missing",
            "--map",
            "5173:5173",
          ]).pipe(Effect.result))._tag,
          "Failure",
        );
        assert.equal(
          (yield* runCli([
            "--remote",
            "https://home.example.com",
            "--token-file",
            "/missing",
            "--map",
            "5173:5173",
            "--map",
            "5173:8080",
          ]).pipe(Effect.result))._tag,
          "Failure",
        );
      }),
    ).pipe(Effect.provide(runtime)),
  );
});
