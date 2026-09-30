import * as NodeCrypto from "node:crypto";
import { type FleetAction, type FleetFile, type FleetResult } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Stream from "effect/Stream";
import { fail, fleetError, MAX_ARTIFACT_BYTES, MAX_OUTPUT_BYTES } from "./Coordinator.ts";

/** Absolute paths are selected against approved roots on the target, using its native path rules. */
export const resolveRootPath = Effect.fnUntraced(function* (
  roots: Readonly<Record<string, string>>,
  root: string | undefined,
  input: string,
  writing = false,
  directory = false,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const inside = (base: string, value: string) => {
    const rel = path.relative(base, value);
    return rel !== ".." && !rel.startsWith(`..${path.sep}`) && !path.isAbsolute(rel);
  };
  if (!input || input.includes("\0")) return yield* fail("Invalid device path.");
  let configured = root ? roots[root] : undefined;
  if (root && (!configured || path.isAbsolute(input)))
    return yield* fail("Use a configured root with a relative path, or an absolute device path.");
  if (!root) {
    if (!path.isAbsolute(input)) return yield* fail("Use an absolute path on the target device.");
    configured = Object.values(roots)
      .filter((base) => inside(path.resolve(base), path.resolve(input)))
      .sort((a, b) => b.length - a.length)[0];
  }
  if (!configured || !path.isAbsolute(configured))
    return yield* fail("Path is outside this device's approved folders.");
  const base = yield* fs.realPath(configured);
  const candidate = root
    ? path.resolve(base, input)
    : path.resolve(base, path.relative(configured, input));
  if (!inside(base, candidate) || (!directory && candidate === base))
    return yield* fail("File escapes its approved root.");
  if (writing) {
    const parent = yield* fs.realPath(path.dirname(candidate));
    if (!inside(base, parent)) return yield* fail("Destination folder escapes its approved root.");
    return path.join(parent, path.basename(candidate));
  }
  const real = yield* fs.realPath(candidate);
  if (!inside(base, real)) return yield* fail("File symlink escapes its approved root.");
  const info = yield* fs.stat(real);
  if (directory ? info.type !== "Directory" : info.type !== "File")
    return yield* fail(directory ? "Path must be a directory." : "Path must be a regular file.");
  return real;
}, Effect.mapError(fleetError));

export const hashFile = Effect.fnUntraced(function* (file: string) {
  const fs = yield* FileSystem.FileSystem;
  const before = yield* fs.stat(file);
  if (before.type !== "File" || before.size > BigInt(MAX_ARTIFACT_BYTES))
    return yield* fail("Hashing supports regular files up to 1 GiB.");
  const hash = NodeCrypto.createHash("sha256");
  let size = 0;
  yield* Stream.runForEach(fs.stream(file), (bytes) =>
    Effect.gen(function* () {
      size += bytes.byteLength;
      if (size > MAX_ARTIFACT_BYTES) return yield* fail("File grew beyond the 1 GiB limit.");
      hash.update(bytes);
    }),
  );
  const after = yield* fs.stat(file);
  if (
    BigInt(size) !== after.size ||
    before.size !== after.size ||
    Option.getOrNull(before.ino) !== Option.getOrNull(after.ino) ||
    Option.getOrNull(before.mtime)?.getTime() !== Option.getOrNull(after.mtime)?.getTime()
  )
    return yield* fail("File changed while being read; inspect it before retrying.");
  return { path: file, type: "file" as const, size, sha256: hash.digest("hex") };
}, Effect.mapError(fleetError));

/** Stage writes beside their destination; create refuses collisions, replace preserves file permissions. */
export const writeFile = Effect.fnUntraced(function* (
  destination: string,
  bytes: Uint8Array,
  overwrite = false,
  expectedSha256?: string,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const exists = yield* fs.exists(destination);
  let mode = 0o600;
  if (exists) {
    if (Option.isSome(yield* fs.readLink(destination).pipe(Effect.option)))
      return yield* fail("Refusing to replace a destination symlink.");
    const info = yield* fs.stat(destination);
    if (info.type !== "File") return yield* fail("Destination is not a regular file.");
    mode = info.mode & 0o777;
  }
  if (expectedSha256) {
    if (!exists || (yield* hashFile(destination)).sha256 !== expectedSha256)
      return yield* fail("Destination changed; expected SHA-256 did not match.");
  }
  const temporary = yield* fs.makeTempFileScoped({
    directory: path.dirname(destination),
    prefix: ".fleet-",
  });
  yield* fs.writeFile(temporary, bytes);
  yield* fs.chmod(temporary, mode);
  if (overwrite || expectedSha256) yield* fs.rename(temporary, destination);
  else yield* fs.link(temporary, destination);
  return yield* hashFile(destination);
}, Effect.mapError(fleetError));

export const searchFiles = Effect.fnUntraced(function* (
  base: string,
  action: Extract<FleetAction, { kind: "search" }>,
): Effect.fn.Return<
  Pick<FleetResult, "files" | "truncated">,
  ReturnType<typeof fleetError>,
  FileSystem.FileSystem | Path.Path
> {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const files: Array<typeof FleetFile.Type> = [];
  const pending = [{ directory: base, depth: 0 }];
  const query = (action.query ?? "").toLocaleLowerCase();
  const limit = action.limit ?? 100;
  let visited = 0;
  let bytes = 0;
  let truncated = false;
  let cursor = 0;
  while (cursor < pending.length) {
    const next = pending[cursor++]!;
    const names = yield* fs.readDirectory(next.directory).pipe(Effect.option);
    if (Option.isNone(names)) {
      truncated = true;
      continue;
    }
    for (const name of names.value) {
      if (++visited > 20_000) return { files, truncated: true };
      const file = path.join(next.directory, name);
      if (Option.isSome(yield* fs.readLink(file).pipe(Effect.option))) continue;
      const info = yield* fs.stat(file).pipe(Effect.option);
      if (Option.isNone(info) || info.value.type === "SymbolicLink") continue;
      const type =
        info.value.type === "File"
          ? "file"
          : info.value.type === "Directory"
            ? "directory"
            : "other";
      if (!query || name.toLocaleLowerCase().includes(query)) {
        bytes += Buffer.byteLength(file) + 128;
        if (files.length === limit || bytes > MAX_OUTPUT_BYTES / 2)
          return { files, truncated: true };
        files.push({ path: file, type, size: Number(info.value.size) });
      }
      if (action.recursive && type === "directory") {
        if (next.depth < 32) pending.push({ directory: file, depth: next.depth + 1 });
        else truncated = true;
      }
    }
  }
  return { files, truncated };
}, Effect.mapError(fleetError));
