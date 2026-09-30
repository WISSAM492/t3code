import * as NodeCrypto from "node:crypto";
import { FleetArtifact, FleetError, FleetWorkerConfig } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { ServerConfig } from "../config.ts";
import { Coordinator, MAX_ARTIFACT_BYTES, fail, fleetError } from "./Coordinator.ts";
const decodeArtifact = Schema.decodeUnknownEffect(FleetArtifact);
const decodeConfig = Schema.decodeUnknownEffect(Schema.fromJsonString(FleetWorkerConfig));

export const blobPath = (hash: string) =>
  Effect.gen(function* () {
    if (!/^[a-f0-9]{64}$/.test(hash)) return yield* fail("Invalid artifact hash.");
    const config = yield* ServerConfig;
    const path = yield* Path.Path;
    return path.join(config.stateDir, "fleet-artifacts", hash);
  });

/** Stream into a private temporary file; neither failed uploads nor mutable source files become builds. */
export const storeBlob = <E, R>(stream: Stream.Stream<Uint8Array, E, R>) =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = path.dirname(yield* blobPath("0".repeat(64)));
      yield* fs.makeDirectory(directory, { recursive: true, mode: 0o700 });
      const temporary = yield* fs.makeTempFileScoped({ directory });
      const hash = NodeCrypto.createHash("sha256");
      let size = 0;
      yield* stream.pipe(
        Stream.mapEffect((chunk) =>
          Effect.gen(function* () {
            size += chunk.byteLength;
            if (size > MAX_ARTIFACT_BYTES) return yield* fail("Artifact exceeds the 1 GiB limit.");
            hash.update(chunk);
            return chunk;
          }),
        ),
        Stream.run(fs.sink(temporary)),
      );
      yield* fs.chmod(temporary, 0o600);
      const sha256 = hash.digest("hex");
      const destination = yield* blobPath(sha256);
      // The destination is content addressed. Concurrent identical uploads may replace identical bytes.
      yield* fs.rename(temporary, destination);
      return { sha256, size };
    }),
  ).pipe(Effect.mapError(fleetError));

export const publishArtifact = (file: string, metadata: Omit<FleetArtifact, "sha256" | "size">) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const coordinator = yield* Coordinator;
    const stored = yield* storeBlob(fs.stream(file));
    const build = yield* decodeArtifact({ ...metadata, ...stored });
    return yield* coordinator.registerArtifact(build);
  }).pipe(Effect.mapError(fleetError));

export const UPLOAD_CHUNK_BYTES = 4 * 1024 * 1024;
const uploading = new Set<string>();
const uploadPath = (id: string) =>
  Effect.gen(function* () {
    if (!/^[a-f0-9-]{36}$/.test(id)) return yield* fail("Invalid upload job.");
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const server = yield* ServerConfig;
    const directory = path.join(server.stateDir, "fleet-uploads");
    yield* fs.makeDirectory(directory, { recursive: true, mode: 0o700 });
    return path.join(directory, id);
  });
export const discardUpload = (id: string) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    yield* fs.remove(yield* uploadPath(id), { force: true });
  });
export const uploadChunk = <E, R>(
  id: string,
  offset: number,
  final: boolean,
  stream: Stream.Stream<Uint8Array, E, R>,
) =>
  Effect.scoped(
    Effect.gen(function* () {
      const locked = yield* Effect.acquireRelease(
        Effect.sync(() => {
          if (uploading.has(id)) return false;
          uploading.add(id);
          return true;
        }),
        (held) =>
          Effect.sync(() => {
            if (held) uploading.delete(id);
          }),
      );
      if (!locked) return yield* fail("Another chunk is already uploading for this job.");
      const fs = yield* FileSystem.FileSystem;
      const file = yield* uploadPath(id);
      const exists = yield* fs.exists(file);
      const size = exists ? Number((yield* fs.stat(file)).size) : 0;
      if (!Number.isSafeInteger(offset) || offset < 0 || offset !== size)
        return yield* fail("Upload offset does not match the saved bytes.");
      let bytes = 0;
      yield* Effect.scoped(
        Effect.gen(function* () {
          const output = yield* fs.open(file, { flag: "a", mode: 0o600 });
          yield* stream.pipe(
            Stream.runForEach((chunk) =>
              Effect.gen(function* () {
                bytes += chunk.byteLength;
                if (bytes > UPLOAD_CHUNK_BYTES || offset + bytes > MAX_ARTIFACT_BYTES)
                  return yield* fail("Upload chunk or artifact is too large.");
                yield* output.writeAll(chunk);
              }),
            ),
          );
          yield* output.sync;
        }),
      );
      if (!final) return { uploadedBytes: offset + bytes };
      const stored = yield* storeBlob(fs.stream(file));
      yield* fs.remove(file, { force: true });
      return {
        ...stored,
        name: "file",
        version: stored.sha256,
        os: "unknown" as const,
        arch: "other" as const,
      };
    }),
  ).pipe(Effect.mapError(fleetError));

export const readWorkerConfig = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const config = yield* ServerConfig;
  const file = path.join(config.stateDir, "fleet-worker.json");
  if (!(yield* fs.exists(file))) return null;
  return yield* decodeConfig(yield* fs.readFileString(file));
}).pipe(
  Effect.mapError(
    () =>
      new FleetError({
        message: "Invalid fleet-worker.json; Fleet worker is disabled until repaired.",
      }),
  ),
);
