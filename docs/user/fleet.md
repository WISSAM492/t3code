# Device access with Fleet

Fleet gives your normal T3 agent access to your other computers when you ask it
to use them. Keep working in the same thread:

- “Find report.pdf on Windows, move it here, and continue working on it.”
- “Compare this file with the one on my Mac.”
- “Run the tests on Windows and bring back the output.”
- “Install this exact build on my Mac and Windows, then verify the versions.”

The agent discovers the devices, searches their approved folders, reads or
writes files, transfers bytes, and runs the appropriate platform commands.
You do not need to define tasks or application recipes. Installations use the
same file and command capabilities as other work; the agent should transfer
the exact build, use its platform's installer, and check the installed version.

Each computer runs a T3 environment with Fleet support. Its worker connects
outward to your coordinating environment's existing T3 Connect address. No
additional VPN, daemon, public service port, or public artifact URL is needed.
Private localhost forwarding works independently for services such as Vite and
Guacamole. Fleet does not change T3's chat, threads, or coding workflow.

## Connect your devices once

On the coordinating environment, enroll each computer, including that computer
itself if you want transfers to and from its local workspace:

```bash
t3 fleet enroll linux-main --out ~/fleet-linux-token --ttl 30d
t3 fleet enroll macbook --out ~/fleet-mac-token --ttl 30d
t3 fleet enroll windows-main --out ~/fleet-windows-token --ttl 30d
```

Transfer each credential privately to its device. It can only act as its enrolled
device and cannot access ordinary T3 threads, terminals, or private forwards.
Enrollment alone gives agents no access. Use the same T3 home as the running
server; specify `--base-dir` when you use a custom home.

On each device, create a configuration like this Linux example:

```json
{
  "version": 1,
  "coordinator": "https://YOUR-EXISTING-T3-CONNECT-HOST",
  "tokenFile": "/home/me/fleet-linux-token",
  "readRoots": { "home": "/home/me" },
  "writeRoots": { "project": "/home/me/project", "downloads": "/home/me/Downloads" },
  "execution": "allow"
}
```

Choose the folders you want the agent to access. Use `/Users/...` on macOS and
JSON paths such as `C:\\Users\\me` on Windows. Folders must already exist.
The coordinator's own worker may use `http://127.0.0.1:3773` with its actual
T3 port. Other computers use its existing HTTPS origin.

`execution: "allow"` permits commands as the normal account running T3, when
a thread also has a `run` grant. Commands have that account's filesystem access;
the approved folders constrain the file tools, not a command's code. Use
`"ask"` to require owner approval for every command or `"disabled"` to disable
commands. Omitting `execution` disables arbitrary commands, including in older
configurations with named tasks.

```bash
t3 fleet join --config ./worker.json
```

Keep each device's T3 server running. It picks up configuration changes within
about five seconds when idle. `t3 fleet leave` disables its worker and stops
active work at the next lease check.

## Allow your agent thread to use devices

On the coordinator, copy the thread ID from its T3 URL and grant the capabilities
you want for each device:

```bash
t3 fleet grant --thread THREAD-ID --device windows-main \
  --capability read --capability write --capability transfer --capability run --ttl 30d
```

Repeat for your Mac and Linux as needed. A grant replaces that thread's previous
capabilities for the device. `read` allows finding, reading and hashing files;
`write` allows text edits; `transfer` allows copies and verified moves; `run`
allows commands under the device's execution policy. Other threads receive no
access automatically.

Now ask normally. These capabilities use T3's existing agent tool connection
across web, desktop and mobile, and its existing provider adapters. Restart an
older agent session after installing this build if it still has the old tool list.

File paths are native absolute paths on the target computer. File tools reject
paths and symlinks escaping approved folders. Text reads and writes are limited
to 64 KiB; transfers and file hashing support regular files up to 1 GiB. Searches
skip symlinks and have bounded results; the agent can narrow a search. Destination
folders must exist, or the agent can create them with an authorized command.

Copies refuse existing destinations unless replacement is explicitly requested.
A move verifies the destination's SHA-256 before removing the source, and requires
the source to be in an approved write folder too. If the source changes, delivery
fails, or access is revoked, it retains the source. Text edits can use the previous
file hash to reject stale replacements. Transfers can also require a specific
source hash to send the exact build inspected earlier.

## Permissions, sleeping devices and interrupted work

Ordinary operations return their result directly to the agent. A long operation
or sleeping device returns a pending identifier the agent can check. Pending
work survives restarts and resumes when the device reconnects while its credential
and thread grant remain valid. Commands that depend on a transfer wait for its
successful completion; a failed transfer cancels those commands. Pending work
expires after seven days. Fleet never silently
repeats an operation whose outcome is uncertain after interruption.

Commands requesting elevation always need approval for that exact action.
Approval does not grant administrator rights or bypass OS prompts. Run T3 as your
normal account. With `execution: "ask"`, normal commands also need approval.
The owner can inspect and approve or deny a request on the coordinator:

```bash
t3 fleet jobs
t3 fleet approve OPERATION-ID
t3 fleet deny OPERATION-ID
```

Changing a device policy invalidates pending approvals. To revoke a thread's
access and cancel its pending work:

```bash
t3 fleet revoke --thread THREAD-ID --device windows-main
```

Use `t3 fleet devices` to inspect enrolled devices. To replace a credential while
preserving its identity, use `t3 fleet enroll DEVICE --rotate --out NEW-FILE --ttl 30d`,
transfer the new credential privately, and update the worker's `tokenFile`.
The previous credential is revoked. After inspecting an uncertain operation,
`t3 fleet retry OPERATION-ID --request-id NEW-ID --acknowledge-duplicate-risk`
creates an explicit new attempt.
