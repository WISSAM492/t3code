import {
  RelayEnvironmentConnectResponse,
  RelayListEnvironmentsResponse,
  type RelayClientEnvironmentRecord,
} from "@t3tools/contracts/relay";
import * as NodeCrypto from "node:crypto";
import {
  AuthAccessTokenResult,
  AuthAccessTokenType,
  AuthEnvironmentBootstrapTokenType,
  AuthFleetDeviceScope,
  AuthTokenExchangeGrantType,
  FleetError,
} from "@t3tools/contracts";
import {
  computeDpopAccessTokenHash,
  computeDpopJwkThumbprint,
  normalizeDpopHtu,
} from "@t3tools/shared/dpop";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { ServerSecretStore } from "../auth/ServerSecretStore.ts";
import { fail, fleetError } from "../fleet/Coordinator.ts";
import { parseConnectOrigin } from "./origin.ts";
import {
  CLOUD_LINKED_USER_ID,
  RELAY_ENVIRONMENT_CREDENTIAL_SECRET,
  RELAY_URL_SECRET,
} from "./config.ts";

const decodePublicJwk = Schema.decodeUnknownSync(
  Schema.Struct({
    kty: Schema.Literal("EC"),
    crv: Schema.Literal("P-256"),
    x: Schema.String,
    y: Schema.String,
  }),
);

// The subject comes from the relay-signed mint proof, never from a worker's request body.
export const connectPeerSubject = (userId: string, environmentId: string) =>
  `connect-peer:${encodeURIComponent(userId)}:${encodeURIComponent(environmentId)}`;
export const parseConnectPeerSubject = (subject: string) => {
  const match = /^connect-peer:([^:]+):([^:]+)$/.exec(subject);
  if (!match) return undefined;
  try {
    return { userId: decodeURIComponent(match[1]!), environmentId: decodeURIComponent(match[2]!) };
  } catch {
    return undefined;
  }
};

/** Connect's installed environment identity discovers and authenticates its account's peers. */
export const make = Effect.gen(function* () {
  const secrets = yield* ServerSecretStore;
  const read = (name: string) =>
    secrets
      .get(name)
      .pipe(
        Effect.map((bytes) =>
          Option.isSome(bytes) ? new TextDecoder().decode(bytes.value) : null,
        ),
      );
  const link = Effect.gen(function* () {
    const [userId, relay, credential] = yield* Effect.all([
      read(CLOUD_LINKED_USER_ID),
      read(RELAY_URL_SECRET),
      read(RELAY_ENVIRONMENT_CREDENTIAL_SECRET),
    ]);
    return userId && relay && credential ? { userId, relay, credential } : null;
  });
  const fetchResponse = (url: URL, init: RequestInit) =>
    Effect.tryPromise({
      try: (signal) => fetch(url, { ...init, signal, redirect: "error" }),
      catch: () => new FleetError({ message: "T3 Connect device connection is unavailable." }),
    }).pipe(
      Effect.flatMap((response) =>
        response.ok
          ? Effect.succeed(response)
          : fail(
              response.status === 404
                ? "This T3 Connect relay needs the device-access update."
                : `T3 Connect rejected device access (${response.status}).`,
            ),
      ),
      Effect.timeout("15 seconds"),
      Effect.mapError(fleetError),
    );
  const decode = <A>(response: Response, schema: Schema.Decoder<A, never>) =>
    Effect.tryPromise({ try: () => response.json(), catch: fleetError }).pipe(
      Effect.flatMap(Schema.decodeUnknownEffect(schema)),
      Effect.mapError(fleetError),
    );
  const relayRequest = <A>(
    current: NonNullable<Effect.Success<typeof link>>,
    endpoint: string,
    payload: unknown,
    schema: Schema.Decoder<A, never>,
  ) =>
    Effect.gen(function* () {
      const origin = parseConnectOrigin(current.relay);
      if (!origin) return yield* fail("Use an HTTPS T3 Connect relay origin.");
      const response = yield* fetchResponse(new URL(endpoint, origin), {
        method: "POST",
        headers: {
          authorization: `Bearer ${current.credential}`,
          "content-type": "application/json",
        },
        body: JSON.stringify(payload),
      });
      return yield* decode(response, schema);
    });
  let directory:
    | { key: string; until: number; peers: ReadonlyArray<RelayClientEnvironmentRecord> }
    | undefined;
  const list = Effect.gen(function* () {
    const current = yield* link;
    if (!current) {
      directory = undefined;
      return [];
    }
    const key = JSON.stringify(current);
    const now = yield* Clock.currentTimeMillis;
    if (directory?.key === key && directory.until > now) return directory.peers;
    const response = yield* relayRequest(
      current,
      "/v1/peers",
      { cloudUserId: current.userId },
      RelayListEnvironmentsResponse,
    );
    directory = { key, until: now + 30_000, peers: response.environments };
    return directory.peers;
  }).pipe(Effect.mapError(fleetError));

  const keys = NodeCrypto.generateKeyPairSync("ec", { namedCurve: "prime256v1" });
  const jwk = decodePublicJwk(keys.publicKey.export({ format: "jwk" }));
  const thumbprint = computeDpopJwkThumbprint(jwk);
  const proof = (url: URL, method: string, now: number, token?: string) => {
    const encode = (value: unknown) => Buffer.from(JSON.stringify(value)).toString("base64url");
    const input = `${encode({ typ: "dpop+jwt", alg: "ES256", jwk })}.${encode({
      htm: method.toUpperCase(),
      htu: normalizeDpopHtu(url.href),
      iat: Math.floor(now / 1000),
      jti: NodeCrypto.randomUUID(),
      ...(token ? { ath: computeDpopAccessTokenHash(token) } : {}),
    })}`;
    const signature = NodeCrypto.sign("sha256", Buffer.from(input), {
      key: keys.privateKey,
      dsaEncoding: "ieee-p1363",
    }).toString("base64url");
    return `${input}.${signature}`;
  };
  const tokens = new Map<string, { key: string; token: string; until: number; origin: URL }>();
  const session = (environmentId: string) =>
    Effect.gen(function* () {
      const current = yield* link;
      if (!current)
        return yield* fail("Connect this environment to T3 Connect to use your devices.");
      const peer = (yield* list).find((item) => item.environmentId === environmentId);
      if (!peer) return yield* fail("This device is no longer linked to your T3 Connect account.");
      const key = JSON.stringify([current, peer.endpoint]);
      const now = yield* Clock.currentTimeMillis;
      const existing = tokens.get(environmentId);
      if (existing?.key === key && existing.until > now) return existing;
      const connected = yield* relayRequest(
        current,
        `/v1/peers/${encodeURIComponent(environmentId)}/connect`,
        {
          cloudUserId: current.userId,
          clientProofKeyThumbprint: thumbprint,
        },
        RelayEnvironmentConnectResponse,
      );
      if (connected.environmentId !== environmentId)
        return yield* fail("T3 Connect returned a different device identity.");
      const origin = parseConnectOrigin(connected.endpoint.httpBaseUrl);
      if (!origin) return yield* fail("T3 Connect returned an invalid device endpoint.");
      const url = new URL("/oauth/token", origin);
      const response = yield* fetchResponse(url, {
        method: "POST",
        headers: {
          "content-type": "application/x-www-form-urlencoded",
          dpop: proof(url, "POST", now),
        },
        body: new URLSearchParams({
          grant_type: AuthTokenExchangeGrantType,
          subject_token: connected.credential,
          subject_token_type: AuthEnvironmentBootstrapTokenType,
          requested_token_type: AuthAccessTokenType,
          scope: AuthFleetDeviceScope,
          client_label: "T3 Connect device",
        }).toString(),
      });
      const exchanged = yield* decode(response, AuthAccessTokenResult);
      if (exchanged.token_type !== "DPoP" || exchanged.scope !== AuthFleetDeviceScope)
        return yield* fail("T3 Connect did not return a bound device credential.");
      const result = {
        key,
        token: exchanged.access_token,
        until: now + Math.min(120, exchanged.expires_in - 10) * 1000,
        origin,
      };
      tokens.set(environmentId, result);
      return result;
    });
  const transport = (environmentId: string) => {
    const request = (endpoint: string, init: RequestInit = {}) =>
      Effect.gen(function* () {
        const connected = yield* session(environmentId);
        const url = new URL(`/api/fleet/${endpoint}`, connected.origin);
        const method = init.method ?? "GET";
        const now = yield* Clock.currentTimeMillis;
        const headers = new Headers(init.headers);
        headers.set("authorization", `DPoP ${connected.token}`);
        headers.set("dpop", proof(url, method, now, connected.token));
        return yield* fetchResponse(url, { ...init, headers });
      }).pipe(Effect.mapError(fleetError));
    const json = <A>(endpoint: string, value: unknown, schema: Schema.Decoder<A, never>) =>
      request(endpoint, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(value),
      }).pipe(
        Effect.flatMap((response) => decode(response, schema)),
        Effect.timeout("15 seconds"),
        Effect.mapError(fleetError),
      );
    return { request, json };
  };
  const authorize = (userId: string, environmentId: string) =>
    Effect.gen(function* () {
      const current = yield* link;
      return (
        current?.userId === userId &&
        (yield* list).some((peer) => peer.environmentId === environmentId)
      );
    }).pipe(Effect.mapError(fleetError));
  return { list, transport, authorize };
});
export class ConnectPeers extends Context.Service<ConnectPeers, Effect.Success<typeof make>>()(
  "t3/cloud/ConnectPeers",
) {}
export const layer = Layer.effect(ConnectPeers, make);
