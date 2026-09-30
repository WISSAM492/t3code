import * as Schema from "effect/Schema";
import { PortSchema } from "./baseSchemas.ts";

export const PrivateForwardPolicy = Schema.Struct({
  version: Schema.Literal(1),
  ports: Schema.Array(PortSchema),
});
export type PrivateForwardPolicy = typeof PrivateForwardPolicy.Type;

export const PrivateForwardMapping = Schema.Struct({
  localPort: PortSchema,
  remotePort: PortSchema,
});
export type PrivateForwardMapping = typeof PrivateForwardMapping.Type;
