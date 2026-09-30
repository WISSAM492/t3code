import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  // A device can execute for several connected environments. Never deliver a receipt to another coordinator.
  yield* sql`ALTER TABLE fleet_execution ADD COLUMN coordinator_id TEXT NOT NULL DEFAULT ''`;
});
