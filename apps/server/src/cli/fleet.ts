import {
  AuthFleetDeviceScope,
  AuthSessionId,
  FleetArtifact,
  FleetCapability,
  FleetRequest,
  FleetWorkerConfig,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Console from "effect/Console";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import { Argument, Command, Flag, GlobalFlag } from "effect/unstable/cli";
import * as EnvironmentAuth from "../auth/EnvironmentAuth.ts";
import { ServerConfig } from "../config.ts";
import { publishArtifact } from "../fleet/Artifacts.ts";
import * as Fleet from "../fleet/Coordinator.ts";
import { resolveForwardUrl } from "../forward/client.ts";
import { expandHomePath } from "../os-jank.ts";
import {
  authLocationFlags,
  type CliAuthLocationFlags,
  DurationFromString,
  resolveCliAuthConfig,
} from "./config.ts";

const runtime = Fleet.layer.pipe(Layer.provideMerge(EnvironmentAuth.runtimeLayer));
const withHost = <A, E, R>(flags: CliAuthLocationFlags, effect: Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    const level = yield* GlobalFlag.LogLevel;
    const config = yield* resolveCliAuthConfig(flags, level);
    return yield* effect.pipe(Effect.provide(runtime), Effect.provideService(ServerConfig, config));
  });
const print = (value: unknown) => Console.log(JSON.stringify(value, null, 2));
const deviceFlag = Flag.String("device").pipe(Flag.withSchema(FleetRequest.fields.device));
const threadFlag = Flag.String("thread");
const ttlFlag = Flag.String("ttl").pipe(
  Flag.withSchema(DurationFromString),
  Flag.withDefault(Duration.days(30)),
);
const jobArgument = Argument.String("job");
const decodeConfig = Schema.decodeUnknownEffect(Schema.fromJsonString(FleetWorkerConfig));

const enroll = Command.make("enroll", {
  ...authLocationFlags,
  device: Argument.String("device").pipe(Argument.withSchema(FleetRequest.fields.device)),
  out: Flag.String("out"),
  ttl: ttlFlag,
  rotate: Flag.Boolean("rotate").pipe(Flag.withDefault(false)),
}).pipe(
  Command.withDescription(
    "Enroll a device on this coordinator and save its dedicated credential privately.",
  ),
  Command.withHandler((flags) =>
    withHost(
      flags,
      Effect.scoped(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem;
          const auth = yield* EnvironmentAuth.EnvironmentAuth;
          const coordinator = yield* Fleet.Coordinator;
          const file = yield* fs.open(yield* expandHomePath(flags.out), {
            flag: "wx",
            mode: 0o600,
          });
          const issued = yield* auth.issueSession({
            scopes: [AuthFleetDeviceScope],
            ttl: flags.ttl,
            label: `Fleet: ${flags.device}`,
          });
          yield* Effect.gen(function* () {
            yield* file.writeAll(new TextEncoder().encode(`${issued.token}\n`));
            yield* file.sync;
            if (flags.rotate) {
              const previous = yield* coordinator.rotateCredential(flags.device, issued.sessionId);
              yield* auth.revokeSession(AuthSessionId.make(previous));
            } else yield* coordinator.enroll(flags.device, issued.sessionId);
          }).pipe(Effect.onError(() => auth.revokeSession(issued.sessionId).pipe(Effect.ignore)));
          yield* Console.log(
            `Enrolled ${flags.device}. Credential saved to ${flags.out}.\nSession: ${issued.sessionId}\nTransfer this file privately to that device. Revoke with t3 auth session revoke ${issued.sessionId}.`,
          );
        }),
      ),
    ),
  ),
);

const join = Command.make("join", { ...authLocationFlags, config: Flag.String("config") }).pipe(
  Command.withDescription(
    "Configure this environment's outbound worker; its running server picks it up automatically.",
  ),
  Command.withHandler((flags) =>
    withHost(
      flags,
      Effect.scoped(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem;
          const path = yield* Path.Path;
          const server = yield* ServerConfig;
          const configuration = yield* decodeConfig(
            yield* fs.readFileString(yield* expandHomePath(flags.config)),
          );
          yield* Effect.try({
            try: () => resolveForwardUrl(configuration.coordinator),
            catch: Fleet.fleetError,
          });
          if (!path.isAbsolute(configuration.tokenFile))
            return yield* Fleet.fail("tokenFile must be an absolute path on this device.");
          const token = (yield* fs.readFileString(configuration.tokenFile)).trim();
          if (!token || /\s/.test(token)) return yield* Fleet.fail("Invalid Fleet token file.");
          for (const directory of [
            ...Object.values(configuration.readRoots),
            ...Object.values(configuration.writeRoots),
          ]) {
            if (!path.isAbsolute(directory) || (yield* fs.stat(directory)).type !== "Directory")
              return yield* Fleet.fail("Approved roots must be existing absolute directories.");
          }
          for (const input of [
            ...Object.values(configuration.tasks).map((task) => task.command),
            ...Object.values(configuration.applications).flatMap((app) => [
              app.install,
              app.verify,
              ...(app.start ? [app.start] : []),
              ...(app.health ? [app.health] : []),
            ]),
          ]) {
            if (
              !input.executable ||
              !path.isAbsolute(input.cwd) ||
              input.args.some((arg) => arg.includes("{artifact}") && arg !== "{artifact}")
            )
              return yield* Fleet.fail(
                "Commands need an executable, absolute cwd, and standalone {artifact} arguments.",
              );
          }
          yield* fs.makeDirectory(server.stateDir, { recursive: true });
          const temporary = yield* fs.makeTempFileScoped({ directory: server.stateDir });
          yield* fs.writeFileString(temporary, JSON.stringify(configuration, null, 2));
          yield* fs.chmod(temporary, 0o600);
          yield* fs.rename(temporary, path.join(server.stateDir, "fleet-worker.json"));
          yield* Console.log(
            "Fleet worker configured. Start this environment's T3 server if it is not running.",
          );
        }),
      ),
    ),
  ),
);
const leave = Command.make("leave", { ...authLocationFlags }).pipe(
  Command.withDescription(
    "Disable this environment's worker and stop active work at the next lease check.",
  ),
  Command.withHandler((flags) =>
    withHost(
      flags,
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const server = yield* ServerConfig;
        yield* fs.remove(path.join(server.stateDir, "fleet-worker.json"), { force: true });
        yield* Console.log("Fleet worker disabled.");
      }),
    ),
  ),
);
const grant = Command.make("grant", {
  ...authLocationFlags,
  device: deviceFlag,
  thread: threadFlag,
  capabilities: Flag.Literals("capability", FleetCapability.literals).pipe(Flag.atLeast(1)),
  ttl: ttlFlag,
}).pipe(
  Command.withDescription("Replace one thread's capabilities for one device, with an expiry."),
  Command.withHandler((flags) =>
    withHost(
      flags,
      Effect.gen(function* () {
        const coordinator = yield* Fleet.Coordinator;
        yield* coordinator.grant(
          flags.thread,
          flags.device,
          flags.capabilities,
          (yield* Clock.currentTimeMillis) + Duration.toMillis(flags.ttl),
        );
        yield* Console.log(
          `Granted ${flags.capabilities.join(", ")} on ${flags.device} to thread ${flags.thread}.`,
        );
      }),
    ),
  ),
);
const revoke = Command.make("revoke", {
  ...authLocationFlags,
  device: deviceFlag,
  thread: threadFlag,
}).pipe(
  Command.withDescription(
    "Remove a thread's device grant, cancel queued work, and stop leased work.",
  ),
  Command.withHandler((flags) =>
    withHost(
      flags,
      Effect.gen(function* () {
        const coordinator = yield* Fleet.Coordinator;
        yield* coordinator.revoke(flags.thread, flags.device);
        yield* Console.log("Grant revoked.");
      }),
    ),
  ),
);
const approve = Command.make("approve", { ...authLocationFlags, job: jobArgument }).pipe(
  Command.withDescription("Allow this exact queued action once under the device's current policy."),
  Command.withHandler((flags) =>
    withHost(
      flags,
      Effect.flatMap(Fleet.Coordinator, (coordinator) =>
        coordinator.approve(flags.job, true).pipe(Effect.flatMap(print)),
      ),
    ),
  ),
);
const deny = Command.make("deny", { ...authLocationFlags, job: jobArgument }).pipe(
  Command.withDescription("Deny a pending action."),
  Command.withHandler((flags) =>
    withHost(
      flags,
      Effect.flatMap(Fleet.Coordinator, (coordinator) =>
        coordinator.approve(flags.job, false).pipe(Effect.flatMap(print)),
      ),
    ),
  ),
);
const cancel = Command.make("cancel", { ...authLocationFlags, job: jobArgument }).pipe(
  Command.withDescription("Cancel an action that has not started."),
  Command.withHandler((flags) =>
    withHost(
      flags,
      Effect.flatMap(Fleet.Coordinator, (coordinator) => coordinator.cancel(flags.job)),
    ),
  ),
);
const devices = Command.make("devices", { ...authLocationFlags }).pipe(
  Command.withDescription("List enrolled devices, platform, status, and last verified versions."),
  Command.withHandler((flags) =>
    withHost(
      flags,
      Effect.flatMap(Fleet.Coordinator, (coordinator) =>
        coordinator.devices().pipe(Effect.flatMap(print)),
      ),
    ),
  ),
);
const jobs = Command.make("jobs", { ...authLocationFlags }).pipe(
  Command.withDescription(
    "Review action IDs, exact targets, pending approvals, outputs, and receipts.",
  ),
  Command.withHandler((flags) =>
    withHost(
      flags,
      Effect.flatMap(Fleet.Coordinator, (coordinator) =>
        coordinator.jobs().pipe(Effect.flatMap(print)),
      ),
    ),
  ),
);
const artifact = Command.make("artifact", {
  ...authLocationFlags,
  file: Flag.String("file"),
  name: Flag.String("name").pipe(Flag.withSchema(FleetArtifact.fields.name)),
  version: Flag.String("version").pipe(Flag.withSchema(FleetArtifact.fields.version)),
  os: Flag.Literals("os", FleetArtifact.fields.os.literals),
  arch: Flag.Literals("arch", FleetArtifact.fields.arch.literals),
}).pipe(
  Command.withDescription("Publish an immutable platform build and return its SHA-256."),
  Command.withHandler((flags) =>
    withHost(
      flags,
      Effect.gen(function* () {
        yield* publishArtifact(yield* expandHomePath(flags.file), {
          name: flags.name,
          version: flags.version,
          os: flags.os,
          arch: flags.arch,
        }).pipe(Effect.flatMap(print));
      }),
    ),
  ),
);
const retry = Command.make("retry", {
  ...authLocationFlags,
  job: jobArgument,
  requestId: Flag.String("request-id").pipe(Flag.withSchema(FleetRequest.fields.requestId)),
  acknowledge: Flag.Boolean("acknowledge-duplicate-risk"),
}).pipe(
  Command.withDescription(
    "Create a new action after inspecting a failed or uncertain execution; never automatic.",
  ),
  Command.withHandler((flags) =>
    withHost(
      flags,
      Effect.gen(function* () {
        const coordinator = yield* Fleet.Coordinator;
        const job = yield* coordinator.job(flags.job);
        if (!["failed", "uncertain"].includes(job.status) || !flags.acknowledge)
          return yield* Fleet.fail(
            "Inspect the target, then use --acknowledge-duplicate-risk with a new request ID.",
          );
        if (job.requestId === flags.requestId)
          return yield* Fleet.fail("Retry requires a new request ID.");
        yield* coordinator
          .enqueue(job.threadId, {
            requestId: flags.requestId,
            device: job.device,
            action: job.action,
          })
          .pipe(Effect.flatMap(print));
      }),
    ),
  ),
);
export const fleetCommand = Command.make("fleet").pipe(
  Command.withDescription(
    "Grant normal T3 agents private access to other devices through existing environments.",
  ),
  Command.withSubcommands([
    enroll,
    join,
    leave,
    grant,
    revoke,
    approve,
    deny,
    cancel,
    devices,
    jobs,
    artifact,
    retry,
  ]),
);
