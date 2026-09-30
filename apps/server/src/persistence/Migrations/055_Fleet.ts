import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE fleet_devices (id TEXT PRIMARY KEY, session_id TEXT NOT NULL UNIQUE, metadata TEXT, last_seen REAL, installed TEXT NOT NULL DEFAULT '{}')`;
  yield* sql`CREATE TABLE fleet_grants (thread_id TEXT NOT NULL, device TEXT NOT NULL, capabilities TEXT NOT NULL, expires_at REAL NOT NULL, PRIMARY KEY(thread_id, device))`;
  yield* sql`CREATE TABLE fleet_jobs (id TEXT PRIMARY KEY, thread_id TEXT NOT NULL, request_id TEXT NOT NULL, device TEXT NOT NULL, status TEXT NOT NULL, payload TEXT NOT NULL, UNIQUE(thread_id, request_id))`;
  yield* sql`CREATE INDEX fleet_jobs_pending ON fleet_jobs(device, status)`;
  yield* sql`CREATE TABLE fleet_artifacts (sha256 TEXT PRIMARY KEY, name TEXT NOT NULL, version TEXT NOT NULL, os TEXT NOT NULL, arch TEXT NOT NULL, payload TEXT NOT NULL, UNIQUE(name, version, os, arch))`;
  // Target-side receipts survive coordinator disconnects and target restarts.
  yield* sql`CREATE TABLE fleet_execution (job_id TEXT PRIMARY KEY, lease TEXT NOT NULL, result TEXT)`;
});
