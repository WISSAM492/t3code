# Fleet

Fleet lets an authorized agent thread run approved tasks, move files, and deploy
exact builds across your T3 environments. Linux can coordinate your fleet while
your Mac and Windows environments connect outward through its existing T3
Connect address. Every participating computer runs a build with Fleet support.
Private localhost forwarding remains useful for Vite and Guacamole; Fleet runs
independently of those forwards.

## Enroll your devices

On the coordinator, create a separate credential for each device:

```bash
t3 fleet enroll linux-main --out ~/fleet-linux-token --ttl 30d
t3 fleet enroll macbook --out ~/fleet-mac-token --ttl 30d
t3 fleet enroll windows-main --out ~/fleet-windows-token --ttl 30d
```

Transfer each credential file privately to its device. These credentials can
only participate as their enrolled device. They cannot access normal T3
terminals, threads, or private port forwards. Enrolling alone gives agents no
access. Use the same T3 home as the running environment; pass `--base-dir` to any
Fleet command when using a custom home.

On each device, prepare a worker configuration. This Linux example allows
project reads, transfers into an existing inbox, and one named test command:

```json
{
  "version": 1,
  "coordinator": "https://YOUR-EXISTING-T3-CONNECT-HOST",
  "tokenFile": "/home/me/fleet-linux-token",
  "readRoots": { "project": "/home/me/project" },
  "writeRoots": { "inbox": "/home/me/project/inbox" },
  "tasks": {
    "tests": {
      "approval": "always",
      "command": {
        "executable": "npm",
        "args": ["test"],
        "cwd": "/home/me/project",
        "timeoutSeconds": 600
      }
    }
  },
  "applications": {}
}
```

Replace the paths and task commands with those for that device. On macOS use
`/Users/...` paths. On Windows use JSON paths such as `C:\\Users\\me\\project`
and named PowerShell tasks such as `powershell.exe` with arguments
`["-NoProfile", "-NonInteractive", "-File", "C:\\scripts\\run-tests.ps1"]`.
The coordinator's own worker can use `http://127.0.0.1:3773` with its actual T3
port; other devices use the existing HTTPS environment origin.

```bash
t3 fleet join --config ./worker.json
```

Keep that device's T3 server running. A running server picks up configuration
changes within about five seconds when idle. A laptop can sleep and reconnect
later. `t3 fleet leave` disables its worker and interrupts active work at the
next lease check.

## Grant one thread access

Copy the thread ID from its T3 URL and grant only the needed capabilities on the
coordinator:

```bash
t3 fleet grant --thread THREAD-ID --device linux-main \
  --capability run --capability read --capability transfer --capability deploy --ttl 30d
t3 fleet grant --thread THREAD-ID --device macbook \
  --capability transfer --capability deploy --ttl 30d
t3 fleet grant --thread THREAD-ID --device windows-main \
  --capability run --capability transfer --capability deploy --ttl 30d
```

Each grant replaces that thread's capabilities for that device. Your agent can
then use `fleet_devices`, `fleet_status`, `fleet_run`, `fleet_read_file`,
`fleet_transfer`, `fleet_publish`, and `fleet_deploy`. Ask it to list devices
first. Tasks can include tests, log collection, application launches, and
restarts; the device owner chooses their executable and fixed arguments.
Files use named roots and relative paths, with symlink escapes rejected.
Transfers preserve file contents, have a 1 GiB limit, and refuse existing
destination files. Destination folders must already exist.

Fleet tools are available to agent sessions through T3's existing MCP setup,
including sessions controlled from web, desktop, and mobile. Restart an existing
agent session after installing this build if it has an older tool list.

## Deploy an exact build

Configure an application recipe on each target. For example, add this entry to
`applications` on Linux:

```json
{
  "demo": {
    "approval": "once",
    "install": {
      "executable": "/home/me/scripts/install-demo",
      "args": ["{artifact}"],
      "cwd": "/home/me/project",
      "timeoutSeconds": 300
    },
    "verify": {
      "executable": "/home/me/apps/demo/bin/demo",
      "args": ["--build-id"],
      "cwd": "/home/me/apps/demo",
      "timeoutSeconds": 15
    },
    "start": {
      "executable": "/home/me/scripts/start-demo",
      "args": [],
      "cwd": "/home/me/apps/demo",
      "timeoutSeconds": 30
    },
    "health": {
      "executable": "/home/me/scripts/check-demo-startup",
      "args": [],
      "cwd": "/home/me/apps/demo",
      "timeoutSeconds": 30
    }
  }
}
```

Supply actual installer and startup scripts for your application and each
platform. `{artifact}` becomes one argument pointing to the downloaded,
hash-verified file. The file has a content-addressed name; installers should
unpack or copy it to the extension/path their platform requires. Keep the
placeholder as a standalone argument rather than inserting it into shell source.
Commands execute as the account running T3.

Publish a build with a full Git commit or another immutable build ID:

```bash
t3 fleet artifact --file ./dist/demo-linux-x64.tar.gz \
  --name demo --version FULL-GIT-COMMIT --os linux --arch x64
```

Publication returns its SHA-256. Publishing different bytes under the same
application/version/platform is rejected. Publish separate `darwin/arm64` and
`windows/x64` artifacts for those devices. The agent can also publish files from
the coordinator's approved read roots with `fleet_publish`.

Ask the agent to deploy those hashes to the corresponding devices. An
installation counts as verified only when `verify` exits successfully and its
stdout, trimmed, equals the manifest's version. That command must probe the
installed application itself; writing the requested version into a receipt file
does not establish the installed version. `start` should return after launching
the application. A configured `health` command must exit successfully to report
healthy startup. Without a health probe, Fleet reports the verified version
without claiming health.

## Review permissions and results

On the coordinator:

```bash
t3 fleet devices
t3 fleet jobs
t3 fleet approve JOB-ID
t3 fleet deny JOB-ID
t3 fleet cancel JOB-ID
```

`approval: "once"` requires owner approval for each action. `"always"` permits
that exact named task or recipe whenever a current thread grant allows it.
Changing a device policy invalidates pending once approvals. Add
`"requiresElevation": true` to any command needing administrator privileges;
it always requires once approval. Fleet approval does not bypass the operating
system's elevation prompt or grant administrator privileges. Run T3 as your
normal account. Fleet permissions constrain Fleet requests; they do not sandbox
code already running with host access.

Pending work survives restarts and resumes when the device reconnects, provided
its credential and thread grant are still valid. Queued jobs expire after seven
days. Completed receipts include bounded stdout/stderr and truncation flags.
Installed versions are the last verified results, and online status is based on
recent contact. Fleet clears an application's old verification when a new
deployment begins.

Work interrupted after dispatch becomes `uncertain`, because the target may have
already performed it. Fleet accepts a recovered completion receipt and avoids
automatically repeating that action. After inspecting the target, an owner can
create a new attempt:

```bash
t3 fleet retry JOB-ID --request-id NEW-REQUEST-ID --acknowledge-duplicate-risk
```

To revoke a thread's access and cancel its pending work:

```bash
t3 fleet revoke --thread THREAD-ID --device windows-main
```

To replace an expired or revoked device credential while retaining its identity
and grants, use `t3 fleet enroll DEVICE --rotate --out NEW-FILE --ttl 30d`, transfer
the new file privately, and update its worker's `tokenFile`. The previous
credential is revoked. Credentials can also be revoked by session ID with
`t3 auth session revoke SESSION-ID`.
