import { AuthPrivateForwardScope, PortSchema } from "@t3tools/contracts";
import * as Console from "effect/Console";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import { Argument, Command, Flag, GlobalFlag } from "effect/unstable/cli";

import * as EnvironmentAuth from "../auth/EnvironmentAuth.ts";
import { ServerConfig } from "../config.ts";
import { expandHomePath } from "../os-jank.ts";
import { PrivateForwardError } from "../forward/bridge.ts";
import { parseMapping, resolveForwardUrl, startForwardListener } from "../forward/client.ts";
import { readPolicy, writePolicy } from "../forward/Policy.ts";
import {
  authLocationFlags,
  type CliAuthLocationFlags,
  DurationFromString,
  resolveCliAuthConfig,
} from "./config.ts";

const withHostConfig = <A, E, R>(flags: CliAuthLocationFlags, effect: Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    const logLevel = yield* GlobalFlag.LogLevel;
    const config = yield* resolveCliAuthConfig(flags, logLevel);
    return yield* effect.pipe(Effect.provideService(ServerConfig, config));
  });

const allowCommand = Command.make("allow", {
  ...authLocationFlags,
  ports: Argument.Int("ports").pipe(Argument.withSchema(PortSchema), Argument.variadic({ min: 1 })),
}).pipe(
  Command.withDescription(
    "Replace this host's approved localhost ports. Removing a port closes its forwards.",
  ),
  Command.withHandler((flags) =>
    withHostConfig(
      flags,
      Effect.gen(function* () {
        yield* writePolicy(flags.ports);
        yield* Console.log(`Approved localhost ports: ${(yield* readPolicy).ports.join(", ")}`);
      }),
    ),
  ),
);

const listCommand = Command.make("list", { ...authLocationFlags }).pipe(
  Command.withDescription("Show this host's approved localhost ports."),
  Command.withHandler((flags) =>
    withHostConfig(
      flags,
      Effect.gen(function* () {
        const policy = yield* readPolicy;
        yield* Console.log(
          policy.ports.length
            ? `Approved localhost ports: ${policy.ports.join(", ")}`
            : "Private forwarding is disabled.",
        );
      }),
    ),
  ),
);

const disableCommand = Command.make("disable", { ...authLocationFlags }).pipe(
  Command.withDescription("Remove all approvals and close this host's active private forwards."),
  Command.withHandler((flags) =>
    withHostConfig(
      flags,
      Effect.gen(function* () {
        yield* writePolicy([]);
        yield* Console.log(
          "Private forwarding is disabled. Active forwards close within one second.",
        );
      }),
    ),
  ),
);

const tokenCommand = Command.make("token", {
  ...authLocationFlags,
  out: Flag.String("out").pipe(
    Flag.withDescription("New file for the forwarding-only credential (mode 0600)."),
  ),
  ttl: Flag.String("ttl").pipe(Flag.withSchema(DurationFromString), Flag.optional),
}).pipe(
  Command.withDescription(
    "Issue a forwarding-only token on the host. Transfer its file privately to the client.",
  ),
  Command.withHandler((flags) =>
    withHostConfig(
      flags,
      Effect.gen(function* () {
        const config = yield* ServerConfig;
        return yield* Effect.scoped(
          Effect.gen(function* () {
            const fs = yield* FileSystem.FileSystem;
            const filePath = yield* expandHomePath(flags.out);
            const file = yield* fs.open(filePath, { flag: "wx", mode: 0o600 });
            const auth = yield* EnvironmentAuth.EnvironmentAuth;
            const issued = yield* auth.issueSession({
              scopes: [AuthPrivateForwardScope],
              ttl: Option.getOrElse(flags.ttl, () => Duration.days(30)),
              label: "Private port forwarding",
            });
            yield* file.writeAll(new TextEncoder().encode(`${issued.token}\n`)).pipe(
              Effect.andThen(file.sync),
              Effect.onError(() => auth.revokeSession(issued.sessionId).pipe(Effect.ignore)),
            );
            yield* Console.log(
              `Saved forwarding credential to ${filePath}\nSession: ${issued.sessionId}\nRevoke with: t3 auth session revoke ${issued.sessionId}`,
            );
          }),
        ).pipe(
          Effect.provide(
            EnvironmentAuth.runtimeLayer.pipe(Layer.provide(Layer.succeed(ServerConfig, config))),
          ),
        );
      }),
    ),
  ),
);

export const forwardCommand = Command.make("forward", {
  remote: Flag.String("remote").pipe(
    Flag.withDescription("Existing HTTPS T3 Connect origin."),
    Flag.optional,
  ),
  tokenFile: Flag.String("token-file").pipe(
    Flag.withDescription("Forwarding credential file; never put the token in the URL."),
    Flag.optional,
  ),
  mappings: Flag.String("map").pipe(
    Flag.withDescription("LOCAL_PORT:REMOTE_PORT. Repeat for multiple forwards."),
    Flag.atLeast(0),
  ),
}).pipe(
  Command.withDescription(
    "Forward approved remote localhost ports to this machine's 127.0.0.1 through T3 Connect.",
  ),
  Command.withHandler((flags) =>
    Effect.scoped(
      Effect.gen(function* () {
        if (
          Option.isNone(flags.remote) ||
          Option.isNone(flags.tokenFile) ||
          flags.mappings.length === 0
        ) {
          return yield* new PrivateForwardError({
            message:
              "Use t3 forward --remote https://YOUR-CONNECT-HOST --token-file FILE --map 5173:5173.",
          });
        }
        const remote = flags.remote.value;
        const { url, mappings } = yield* Effect.try({
          try: () => ({
            url: resolveForwardUrl(remote),
            mappings: flags.mappings.map(parseMapping),
          }),
          catch: () =>
            new PrivateForwardError({
              message:
                "Invalid --remote origin or --map. Use HTTPS and LOCAL_PORT:REMOTE_PORT (1–65535).",
            }),
        });
        if (new Set(mappings.map((item) => item.localPort)).size !== mappings.length) {
          return yield* new PrivateForwardError({
            message: "Each mapping must have a distinct local port.",
          });
        }
        const fs = yield* FileSystem.FileSystem;
        const filePath = yield* expandHomePath(flags.tokenFile.value);
        const token = (yield* fs.readFileString(filePath)).trim();
        if (!token || /\s/.test(token)) {
          return yield* new PrivateForwardError({
            message: "Credential file must contain one bearer token.",
          });
        }
        for (const mapping of mappings) {
          yield* startForwardListener({ url, token, mapping });
          yield* Console.log(
            `127.0.0.1:${mapping.localPort} ⇄ remote 127.0.0.1:${mapping.remotePort}`,
          );
        }
        yield* Console.log("Private forwarding is running. Press Ctrl+C to stop.");
        return yield* Effect.never;
      }),
    ),
  ),
  Command.withSubcommands([allowCommand, listCommand, disableCommand, tokenCommand]),
);
