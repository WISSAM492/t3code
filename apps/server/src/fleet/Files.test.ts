import * as NodePath from "@effect/platform-node/NodePath";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import { resolveRootPath } from "./Files.ts";

describe("Fleet native device paths", () => {
  it.effect(
    "uses Windows drive and case rules while rejecting other drives and symlink escapes",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const realFs = yield* FileSystem.FileSystem;
          const fixture = yield* realFs.makeTempFileScoped();
          const info = yield* realFs.stat(fixture);
          const fs = FileSystem.makeNoop({
            realPath: (path) =>
              Effect.succeed(path.endsWith("link.txt") ? "D:\\private\\secret.txt" : path),
            stat: () => Effect.succeed(info),
          });
          const roots = { home: "C:\\Users\\Me" };
          assert.equal(
            yield* resolveRootPath(roots, undefined, "c:/users/me/Downloads/report.txt").pipe(
              Effect.provideService(FileSystem.FileSystem, fs),
              Effect.provide(NodePath.layerWin32),
            ),
            "C:\\Users\\Me\\Downloads\\report.txt",
          );
          for (const path of [
            "D:\\private\\secret.txt",
            "C:\\Users\\Me2\\secret.txt",
            "C:\\Users\\Me\\..\\secret.txt",
            "C:\\Users\\Me\\link.txt",
            "C:relative.txt",
            "\\\\other-host\\share\\file",
          ]) {
            assert.equal(
              (yield* resolveRootPath(roots, undefined, path).pipe(
                Effect.provideService(FileSystem.FileSystem, fs),
                Effect.provide(NodePath.layerWin32),
                Effect.result,
              ))._tag,
              "Failure",
            );
          }
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
  );
});
