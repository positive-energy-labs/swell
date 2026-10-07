import { type AnyFact, type Find, ofKind, Port, type Store } from "@tc/kernel";
import { Context, Effect, Layer } from "effect";
import { meta, Observed } from "./facts.ts";

/** What one tide may read of another: facts by index and tallies by range, both bounded. Never a write. */
export interface PeerService {
  readonly find: (
    table: string,
    index: string,
    find: Find,
  ) => Effect.Effect<ReadonlyArray<Record<string, unknown>>, Error>;
  readonly tallies: (
    projection: string,
    range: { readonly gte: string; readonly lt: string; readonly limit: number },
  ) => Effect.Effect<ReadonlyArray<{ readonly key: string; readonly value: Record<string, number> }>, Error>;
}

export class Peer extends Context.Service<Peer, PeerService>()("tide/Peer") {}

export const fieldsOf = (table: string, index: string): ReadonlyArray<string> => {
  const fact = (ofKind("fact") as ReadonlyArray<AnyFact>).find((f) => f.table === table);
  const fields = fact?.indexes[index] as ReadonlyArray<string> | undefined;
  if (fields === undefined) throw new Error(`no index ${index} on ${table}`);
  return fields;
};

/** A peer over its own store, for tests and for a host reading itself. */
export const peerOf = (store: Store): Layer.Layer<Peer> =>
  Layer.succeed(Peer, {
    find: (table, index, find) => store.find(table, index, fieldsOf(table, index), find),
    tallies: (projection, { gte, lt, limit }) => store.tally.range(projection, gte, lt, limit),
  });

/** A peer over HTTP: the host's read-only door, under the person who enabled the reading loop. */
export const peerHttp = (base: string, token: string): Layer.Layer<Peer> => {
  const get = (path: string, params: Record<string, string | number | boolean | undefined>) =>
    Effect.tryPromise({
      try: async () => {
        const url = new URL(path, base);
        for (const [k, v] of Object.entries(params)) if (v !== undefined) url.searchParams.set(k, String(v));
        const r = await fetch(url, { headers: { authorization: `Bearer ${token}` } });
        if (!r.ok) throw new Error(`${r.status} ${await r.text()}`);
        return (await r.json()) as never;
      },
      catch: (e) => new Error(`peer ${base}${path}: ${e instanceof Error ? e.message : String(e)}`),
    });
  return Layer.succeed(Peer, {
    find: (table, index, f) =>
      get(`/facts/${table}`, {
        index,
        eq: f.eq === undefined ? undefined : JSON.stringify(f.eq),
        gte: f.gte,
        lt: f.lt,
        order: f.order,
        limit: f.limit,
      }),
    tallies: (projection, r) => get(`/tallies/${projection}`, r),
  });
};

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
