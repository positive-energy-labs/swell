import { type AnyFact, type Find, ofKind, Port, type Store } from "@swell/kernel";
import { Context, Crypto, Duration, Effect, Layer, Redacted, Schema } from "effect";
import { Hex } from "effect/encoding";
import { FetchHttpClient, HttpClient, HttpClientRequest } from "effect/http";
import { HttpApiClient, HttpApiMiddleware } from "effect/http-api";
import { ControllerApi, PeerAuth } from "./door.ts";
import { Cite, meta } from "./facts.ts";

/** A peer read that failed: where, the failure's own tag, and what it said. */
export class PeerError extends Schema.TaggedError<PeerError>()("PeerError", {
  where: Schema.String,
  reason: Schema.String,
  message: Schema.String,
}) {}
export class UnknownIndex extends Schema.TaggedError<UnknownIndex>()("UnknownIndex", {
  message: Schema.String,
}) {}

/** What one controller may read of another: facts by index and tallies by range, both bounded. Never a write. */
export interface PeerService {
  readonly find: (
    table: string,
    index: string,
    find: Find,
  ) => Effect.Effect<ReadonlyArray<Record<string, unknown>>, PeerError>;
  readonly tallies: (
    projection: string,
    range: { readonly gte: string; readonly lt: string; readonly limit: number },
  ) => Effect.Effect<
    ReadonlyArray<{ readonly key: string; readonly value: Record<string, number> }>,
    PeerError
  >;
}

export class Peer extends Context.Service<Peer, PeerService>()("swell/Peer") {}

export const fieldsOf = (
  table: string,
  index: string,
): Effect.Effect<ReadonlyArray<string>, UnknownIndex> => {
  const fields = (ofKind("fact") as ReadonlyArray<AnyFact>).find((f) => f.table === table)?.indexes[index] as
    | ReadonlyArray<string>
    | undefined;
  return fields === undefined
    ? Effect.fail(new UnknownIndex({ message: `no index ${index} on ${table}` }))
    : Effect.succeed(fields);
};

/** A peer over its own store, for tests and for a controller reading itself. */
export const peerOf = (store: Store): Layer.Layer<Peer> =>
  Layer.succeed(Peer, {
    find: (table, index, find) =>
      fieldsOf(table, index).pipe(
        Effect.mapError((e) => new PeerError({ where: table, reason: e._tag, message: e.message })),
        Effect.flatMap((fields) => store.find(table, index, fields, find)),
      ),
    tallies: (projection, { gte, lt, limit }) => store.tally.range(projection, gte, lt, limit),
  });

const PEER_TIMEOUT = Duration.seconds(30);

/**
 * A peer over HTTP, derived from the same contract the controller serves, under the operator who enabled the
 * reading loop. A read that hangs is a failure after thirty seconds; the error keeps the failure's own tag.
 */
export const peerHttp = (base: string, token: Redacted.Redacted): Layer.Layer<Peer> =>
  Layer.effect(
    Peer,
    Effect.gen(function* () {
      const c = yield* HttpApiClient.make(ControllerApi, {
        transformClient: HttpClient.mapRequest(HttpClientRequest.prependUrl(base)),
      });
      const wrap =
        (where: string) =>
        <A, E, R>(fa: Effect.Effect<A, E, R>): Effect.Effect<A, PeerError, R> =>
          fa.pipe(
            Effect.timeoutOrElse({
              duration: PEER_TIMEOUT,
              orElse: () =>
                Effect.fail(
                  new PeerError({ where: `${base} ${where}`, reason: "Timeout", message: "no answer" }),
                ),
            }),
            Effect.mapError((e: unknown) => {
              if (e instanceof PeerError) return e;
              const { _tag = "Error", message = String(e) } = (e ?? {}) as {
                _tag?: string;
                message?: string;
              };
              return new PeerError({ where: `${base} ${where}`, reason: _tag, message });
            }),
          );
      return {
        find: (table, index, { eq, gte, lt, order, limit }) =>
          c.peer.facts({ params: { table }, query: { index, eq, gte, lt, order, limit } }).pipe(wrap(table)),
        tallies: (id, query) => c.peer.tallies({ params: { id }, query }).pipe(wrap(id)),
      };
    }),
  ).pipe(
    Layer.provide(
      HttpApiMiddleware.layerClient(PeerAuth, ({ next, request }) =>
        next(HttpClientRequest.bearerToken(request, Redacted.value(token))),
      ),
    ),
    Layer.provide(FetchHttpClient.layer),
  );

export const PeerPort = Port.make({
  id: "control::peer",
  service: Peer,
  live: Layer.effect(
    Peer,
    Effect.die(new Error("control::peer live layer is provided by the controller, per peer")),
  ),
  fake: Layer.effect(Peer, Effect.die(new Error("control::peer fake is peerOf(store); a test provides one"))),
  meta: meta(
    "Peer",
    "Another controller, read by URN and never copied.",
    "evidence in by URN; proposals out through the peer's own operator",
  ),
});

/** A fact's address across controllers: the scheme is the domain, the authority is the controller that wrote it. */
export const urnOf = (controller: string, table: string, id: string) =>
  `control:${controller}/${table}/${id}`;

/** JSON with its keys sorted at every level, so one row always hashes the same. */
const canonical = (v: unknown): string =>
  Array.isArray(v)
    ? `[${v.map(canonical).join(",")}]`
    : typeof v === "object" && v !== null
      ? `{${Object.keys(v)
          .sort()
          .map((k) => `${JSON.stringify(k)}:${canonical((v as Record<string, unknown>)[k])}`)
          .join(",")}}`
      : JSON.stringify(v);

/** The row a peer fact becomes here: a pointer with a content hash, so a change elsewhere is visible without a copy. */
export const citeOf = Effect.fn("swell/citeOf")(function* (
  controller: string,
  table: string,
  row: { readonly _id: string },
) {
  const crypto = yield* Crypto.Crypto;
  const digest = yield* crypto.digest("SHA-256", new TextEncoder().encode(canonical(row)));
  return {
    fact: Cite,
    draft: { urn: urnOf(controller, table, row._id), source: controller, hash: Hex.encode(digest) },
  };
});
