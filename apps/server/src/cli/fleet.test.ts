import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NetService from "@t3tools/shared/Net";
import { AuthFleetDeviceScope } from "@t3tools/contracts";
import { assert, describe, it } from "@effect/vitest";
import * as ConfigProvider from "effect/ConfigProvider";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as TestConsole from "effect/testing/TestConsole";
import { Command } from "effect/unstable/cli";
import * as EnvironmentAuth from "../auth/EnvironmentAuth.ts";
import { ServerConfig, deriveServerPaths } from "../config.ts";
import * as Fleet from "../fleet/Coordinator.ts";
import { resolveCliAuthConfig } from "./config.ts";
import { fleetCommand } from "./fleet.ts";

const runtime = Layer.mergeAll(
  NodeServices.layer,
  NetService.layer,
  TestConsole.layer,
  ConfigProvider.layer(ConfigProvider.fromEnv({ env: {} })),
);
const runCli = (args: ReadonlyArray<string>) =>
  Command.runWith(fleetCommand, { version: "test" })(args);
const withStore = <A, E, R>(baseDir: string, effect: Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    const config = yield* resolveCliAuthConfig({ baseDir: Option.some(baseDir) }, Option.none());
    return yield* effect.pipe(
      Effect.provide(Fleet.layer.pipe(Layer.provideMerge(EnvironmentAuth.runtimeLayer))),
      Effect.provideService(ServerConfig, config),
    );
  });

describe("t3 fleet owner commands", () => {
  it.effect(
    "enrolls with a private device-only credential and rotates it without losing identity or grants",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem;
          const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-fleet-cli-" });
          const location = ["--base-dir", baseDir];
          const tokenFile = `${baseDir}/token`;
          yield* runCli(["enroll", "macbook", "--out", tokenFile, "--ttl", "1h", ...location]);
          const token = (yield* fs.readFileString(tokenFile)).trim();
          assert(!(yield* TestConsole.logLines).join("\n").includes(token));
          yield* runCli([
            "grant",
            "--device",
            "macbook",
            "--thread",
            "thread-one",
            "--capability",
            "read",
            ...location,
          ]);
          yield* withStore(
            baseDir,
            Effect.gen(function* () {
              const auth = yield* EnvironmentAuth.EnvironmentAuth;
              assert.deepEqual((yield* auth.listSessions())[0]!.scopes, [AuthFleetDeviceScope]);
              const coordinator = yield* Fleet.Coordinator;
              assert.equal((yield* coordinator.devices("thread-one"))[0]!.permissions[0], "read");
              assert.deepEqual(yield* coordinator.devices("ungranted-thread"), []);
            }),
          );
          assert.equal(
            (yield* runCli(["enroll", "macbook", "--out", tokenFile, ...location]).pipe(
              Effect.result,
            ))._tag,
            "Failure",
          );
          yield* runCli([
            "enroll",
            "macbook",
            "--out",
            `${baseDir}/rotated`,
            "--rotate",
            ...location,
          ]);
          assert.notEqual((yield* fs.readFileString(`${baseDir}/rotated`)).trim(), token);
          yield* withStore(
            baseDir,
            Effect.gen(function* () {
              const auth = yield* EnvironmentAuth.EnvironmentAuth;
              assert.equal((yield* auth.listSessions()).length, 1);
              const coordinator = yield* Fleet.Coordinator;
              assert.equal((yield* coordinator.devices("thread-one"))[0]!.id, "macbook");
            }),
          );
          yield* runCli(["revoke", "--device", "macbook", "--thread", "thread-one", ...location]);
          yield* withStore(
            baseDir,
            Effect.flatMap(Fleet.Coordinator, (coordinator) =>
              coordinator
                .devices("thread-one")
                .pipe(Effect.map((devices) => assert.deepEqual(devices, []))),
            ),
          );
        }),
      ).pipe(Effect.provide(runtime)),
  );
  it.effect("validates and atomically replaces worker configuration, then leaves", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-fleet-join-" });
        const root = yield* fs.makeTempDirectoryScoped();
        const location = ["--base-dir", baseDir];
        yield* fs.writeFileString(`${root}/token`, "example-token");
        const config = {
          version: 1,
          coordinator: "https://my-existing-connect.example",
          tokenFile: `${root}/token`,
          readRoots: { project: root },
          writeRoots: { project: root },
          tasks: {
            test: {
              approval: "once",
              command: { executable: "node", args: ["--version"], cwd: root, timeoutSeconds: 10 },
            },
          },
          applications: {},
        };
        const configFile = `${root}/worker.json`;
        const paths = yield* deriveServerPaths(baseDir, undefined);
        const target = `${paths.stateDir}/fleet-worker.json`;
        yield* fs.writeFileString(configFile, JSON.stringify(config));
        yield* runCli(["join", "--config", configFile, ...location]);
        const installed = yield* fs.readFileString(target);
        assert.deepEqual(JSON.parse(installed).tasks, config.tasks);
        for (const invalid of [
          { ...config, coordinator: "http://public.example" },
          { ...config, readRoots: { project: "/missing-absolute-folder" } },
          { ...config, tokenFile: "relative-token" },
          {
            ...config,
            tasks: {
              bad: {
                approval: "always",
                command: {
                  executable: "sh",
                  args: ["-c", "install {artifact}"],
                  cwd: root,
                  timeoutSeconds: 10,
                },
              },
            },
          },
        ]) {
          yield* fs.writeFileString(configFile, JSON.stringify(invalid));
          assert.equal(
            (yield* runCli(["join", "--config", configFile, ...location]).pipe(Effect.result))._tag,
            "Failure",
          );
          assert.equal(yield* fs.readFileString(target), installed);
        }
        const { tasks: _tasks, applications: _applications, ...deviceConfig } = config;
        yield* fs.writeFileString(
          configFile,
          JSON.stringify({ ...deviceConfig, execution: "allow" }),
        );
        yield* runCli(["join", "--config", configFile, ...location]);
        const minimal = JSON.parse(yield* fs.readFileString(target));
        assert.equal(minimal.execution, "allow");
        assert.deepEqual(minimal.tasks, {});
        assert.deepEqual(minimal.applications, {});
        yield* runCli(["leave", ...location]);
        assert.equal(yield* fs.exists(target), false);
      }),
    ).pipe(Effect.provide(runtime)),
  );
  it.effect("publishes a hashed platform build and refuses replacing that exact version", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-fleet-artifact-" });
        const file = `${baseDir}/build`;
        const args = [
          "artifact",
          "--file",
          file,
          "--name",
          "demo",
          "--version",
          "abc123",
          "--os",
          "linux",
          "--arch",
          "x64",
          "--base-dir",
          baseDir,
        ];
        yield* fs.writeFileString(file, "build-one");
        yield* runCli(args);
        const output = (yield* TestConsole.logLines).at(-1);
        assert.equal(typeof output, "string");
        assert(typeof output === "string");
        const result = JSON.parse(output);
        assert.equal(result.version, "abc123");
        assert.match(result.sha256, /^[a-f0-9]{64}$/);
        yield* runCli(args);
        yield* fs.writeFileString(file, "build-two");
        assert.equal((yield* runCli(args).pipe(Effect.result))._tag, "Failure");
      }),
    ).pipe(Effect.provide(runtime)),
  );
});
