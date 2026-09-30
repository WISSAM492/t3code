# Device access through T3 Connect

Connect each computer's T3 environment to the same T3 Connect account once.
Your normal agent can then discover and use those computers when your request
needs another device. There is no separate Fleet enrollment, joining, token
file, or thread grant to configure.

For example:

- “Find the PDF on Windows, move it to this Linux project, and continue working.”
- “Check the file on my Mac and compare it with this one.”
- “Run these tests on Windows and bring back the output.”
- “Install this exact build on my Mac and Windows, then verify their versions.”

Keep working in the same T3 thread. Device tools discover each environment's
label, operating system, architecture, starting directories, and availability.
They can search, read, write, transfer, and execute commands with the target's
normal operating-system account permissions. Administrator actions require
**Allow once** in the usual thread approval UI; approval does not bypass the
operating system's own elevation requirements.

Transfers verify SHA-256 and refuse destination collisions by default. A move
removes its source only after verified delivery, and only if the source has not
changed. Text reads and writes support 64 KiB; transfers support files up to
1 GiB. To install an exact build, the agent transfers the inspected artifact,
runs the platform's installer, and checks the resulting version.

Offline actions wait for the environment to reconnect, for up to seven days.
An interrupted action with an uncertain outcome requires inspection before
another attempt. Removing an environment from your Connect account removes its
device access; running actions stop at the next authorization/lease check.

All environments and the T3 Connect relay must run a build with device access
support. An older hosted relay reports that it needs an update. A connected
browser alone is a client: to execute commands on a Mac or Windows computer,
its own T3 environment must be running and connected.

Device access uses the existing authenticated Connect addresses. It adds no
public ports, VPN, separate daemon, or dependency on private port forwarding.
Private forwarding remains available for accessing local services such as Vite
and Guacamole.

Existing manually configured Fleet workers remain compatible. Their folder and
command policies still apply; those legacy settings are optional and are not
part of the normal Connect setup.
