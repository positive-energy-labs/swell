import { type AnyFact, type Find, ofKind, Port, type Store } from "@swell/kernel";
import { Context, Data, Effect, Layer } from "effect";
import { FetchHttpClient, HttpClient, HttpClientRequest } from "effect/http";
import { HttpApiClient, HttpApiMiddleware } from "effect/http-api";
import { ControllerApi, DoorAuth } from "./door.ts";
import { Cite, meta } from "./facts.ts";

export class PeerError extends Data.TaggedError("PeerError")<{
  readonly where: string;
  readonly message: string;
}> {}
export class UnknownIndex extends Data.TaggedError("UnknownIndex")<{ readonly message: string }> {}

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
        Effect.mapError((e) => new PeerError({ where: table, message: e.message })),
        Effect.flatMap((fields) => store.find(table, index, fields, find)),
      ),
    tallies: (projection, { gte, lt, limit }) => store.tally.range(projection, gte, lt, limit),
  });

/** A peer over HTTP, derived from the same contract the controller serves, under the operator who enabled the reading loop. */
export const peerHttp = (base: string, token: string): Layer.Layer<Peer> =>
  Layer.effect(
    Peer,
    Effect.gen(function* () {
      const c = yield* HttpApiClient.make(ControllerApi, {
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
      HttpApiMiddleware.layerClient(DoorAuth, ({ next, request }) =>
        next(HttpClientRequest.bearerToken(request, token)),
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

const hash = (s: string) => {
  let h = 5381;
  for (const c of s) h = ((h << 5) + h + c.charCodeAt(0)) >>> 0;
  return h.toString(16);
};

/** The row a peer fact becomes here: a pointer with a content hash, so a change elsewhere is visible without a copy. */
export const citeOf = (controller: string, table: string, row: { readonly _id: string }) => ({
  fact: Cite,
  draft: { urn: urnOf(controller, table, row._id), source: controller, hash: hash(JSON.stringify(row)) },
});
