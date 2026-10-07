import { type AnyFact, type Find, ofKind, Port, type Store } from "@tc/kernel";
import { Context, Data, Effect, Layer } from "effect";
import { FetchHttpClient, HttpClient, HttpClientRequest } from "effect/http";
import { HttpApiClient, HttpApiMiddleware } from "effect/http-api";
import { PeerAuth, TideApi } from "./door.ts";
import { meta, Observed } from "./facts.ts";

export class PeerError extends Data.TaggedError("PeerError")<{
  readonly where: string;
  readonly message: string;
}> {}
export class UnknownIndex extends Data.TaggedError("UnknownIndex")<{ readonly message: string }> {}

/** What one tide may read of another: facts by index and tallies by range, both bounded. Never a write. */
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

export class Peer extends Context.Service<Peer, PeerService>()("tide/Peer") {}

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

/** A peer over its own store, for tests and for a host reading itself. */
export const peerOf = (store: Store): Layer.Layer<Peer> =>
  Layer.succeed(Peer, {
    find: (table, index, find) =>
      fieldsOf(table, index).pipe(
        Effect.mapError((e) => new PeerError({ where: table, message: e.message })),
        Effect.flatMap((fields) => store.find(table, index, fields, find)),
      ),
    tallies: (projection, { gte, lt, limit }) => store.tally.range(projection, gte, lt, limit),
  });

/** A peer over HTTP, derived from the same contract the host serves, under the person who enabled the reading loop. */
export const peerHttp = (base: string, token: string): Layer.Layer<Peer> =>
  Layer.effect(
    Peer,
    Effect.gen(function* () {
      const c = yield* HttpApiClient.make(TideApi, {
        transformClient: HttpClient.mapRequest(HttpClientRequest.prependUrl(base)),
      });
      const wrap = (where: string) =>
        Effect.mapError(
          (e: unknown) =>
            new PeerError({ where: `${base} ${where}`, message: e instanceof Error ? e.message : String(e) }),
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
        next(HttpClientRequest.bearerToken(request, token)),
      ),
    ),
    Layer.provide(FetchHttpClient.layer),
  );

export const PeerPort = Port.make({
  id: "tide::peer",
  service: Peer,
  live: Layer.effect(Peer, Effect.die(new Error("tide::peer live layer is provided by the host, per peer"))),
  fake: Layer.effect(Peer, Effect.die(new Error("tide::peer fake is peerOf(store); a test provides one"))),
  meta: meta(
    "Peer",
    "Another tide, read by URN and never copied.",
    "evidence in by URN; proposals out through the peer's gate",
  ),
});

export const urnOf = (host: string, table: string, id: string) => `tide:${host}/${table}/${id}`;

const hash = (s: string) => {
  let h = 5381;
  for (const c of s) h = ((h << 5) + h + c.charCodeAt(0)) >>> 0;
  return h.toString(16);
};

/** The row a peer fact becomes here: a pointer with a content hash, so a change elsewhere is visible without a copy. */
export const observedOf = (host: string, table: string, row: { readonly _id: string }) => ({
  fact: Observed,
  draft: { urn: urnOf(host, table, row._id), source: host, hash: hash(JSON.stringify(row)) },
});
