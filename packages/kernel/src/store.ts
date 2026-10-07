import { Effect, Option, Schema } from "effect";
import { type Db, type Find, MAX_LIMIT, type Reader } from "./db.ts";
import { violation } from "./errors.ts";
import type { AnyFact, Draft, Id, IndexName } from "./fact.ts";
import type { Rollup, Snapshot } from "./projection.ts";
import { ofKind } from "./registry.ts";
import * as Trace from "./trace.ts";

/** The storage a backend supplies: raw table access only. All fact semantics live once, in `makeDb`, over any Store. */
export interface Store {
  readonly get: (table: string, id: string) => Effect.Effect<Option.Option<any>>;
  readonly find: (
    table: string,
    index: string,
    fields: ReadonlyArray<string>,
    find: Find,
  ) => Effect.Effect<ReadonlyArray<any>>;
  readonly insert: (table: string, doc: Record<string, unknown>) => Effect.Effect<string>;
  readonly tally: {
    readonly get: (id: string, key: string) => Effect.Effect<Readonly<Record<string, number>>>;
    /** The rows with `gte <= key < lt`, in key order. */
    readonly range: (
      id: string,
      gte: string,
      lt: string,
      limit: number,
    ) => Effect.Effect<
      ReadonlyArray<{ readonly key: string; readonly value: Readonly<Record<string, number>> }>
    >;
    readonly add: (id: string, key: string, delta: Readonly<Record<string, number>>) => Effect.Effect<void>;
  };
  /** Snapshot rows hold JSON text, so every backend stores any view shape the same way. */
  readonly snapshot: {
    readonly get: (id: string, key: string) => Effect.Effect<Option.Option<string>>;
    readonly list: (
      id: string,
      shelf: string,
      limit: number,
    ) => Effect.Effect<ReadonlyArray<{ readonly key: string; readonly value: string }>>;
    /** `null` deletes the row. */
    readonly put: (
      id: string,
      key: string,
      row: { readonly shelf: string; readonly value: string } | null,
    ) => Effect.Effect<void>;
  };
  /** Wake a rule soon because a fact it triggers on was appended. No-op where there is no scheduler. */
  readonly kick: (rule: string) => Effect.Effect<void>;
  /** Run one write unit atomically. Identity where the caller already is one (a Convex mutation); BEGIN/COMMIT for SQLite. */
  readonly transaction: <A, E, R>(fa: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>;
}

export interface WriteCtx {
  readonly by: string;
  readonly via?: string | undefined;
  readonly now: number;
  readonly trace: string | undefined;
}

/** An unbounded read is a defect of the caller, raised in the Effect so it fails the transaction it is in. */
const bounded = <A>(limit: number, read: () => Effect.Effect<A>): Effect.Effect<A> =>
  limit > 0 && limit <= MAX_LIMIT
    ? Effect.suspend(read)
    : Effect.die(new Error(`kernel: read limit ${limit} is outside (0, ${MAX_LIMIT}]`));

export const makeReader = (store: Store): Reader<AnyFact> => ({
  get: (fact, id) => store.get(fact.table, id),
  find: (fact, index, find) => {
    const fields = fact.indexes[index];
    if (fields === undefined) return Effect.die(new Error(`${fact.id} has no index ${index}`));
    return bounded(find.limit, () => store.find(fact.table, index, fields, find));
  },
  tally: store.tally.get,
  tallies: (projection, { gte, lt, limit }) =>
    bounded(limit, () => store.tally.range(projection, gte, lt, limit)),
  snapshot: (s, key) => store.snapshot.get(s.id, key).pipe(Effect.map(Option.map((v) => JSON.parse(v)))),
  shelf: (s, shelf, limit) =>
    bounded(limit, () =>
      store.snapshot
        .list(s.id, shelf, limit)
        .pipe(Effect.map((rows) => rows.map((r) => ({ key: r.key, value: JSON.parse(r.value) })))),
    ),
});

/** Every declared rollup and snapshot once, even when several projections declare the same one. */
const declared = () => {
  const rollups = new Set<Rollup<any>>();
  const snapshots = new Set<Snapshot<any>>();
  for (const p of ofKind("projection")) {
    for (const r of p.rollups) rollups.add(r);
    for (const s of p.snapshots) snapshots.add(s);
  }
  return { rollups: [...rollups], snapshots: [...snapshots] };
};

/** Snapshot keys appended to in this transaction, per store: a store is one transaction. */
const dirty = new WeakMap<Store, Map<Snapshot<any>, Set<string>>>();

/**
 * Rebuild every snapshot row this transaction touched, once each. Every writer calls it before its
 * transaction ends; a missed settle leaves a snapshot stale until the next write to that key.
 */
export const settle = (store: Store): Effect.Effect<void> =>
  Effect.gen(function* () {
    const reader = makeReader(store);
    const touched = dirty.get(store);
    dirty.delete(store);
    for (const [s, keys] of touched ?? []) {
      yield* Effect.forEach(
        keys,
        (key) =>
          s
            .build(key, reader)
            .pipe(
              Effect.flatMap((row) =>
                store.snapshot.put(
                  s.id,
                  key,
                  row === null ? null : { shelf: row.shelf, value: JSON.stringify(row.value) },
                ),
              ),
            ),
        { discard: true },
      );
    }
  });

/** One transaction: `f` writes through a Db, then the touched snapshots settle. */
export const transact = <A, E, R>(
  store: Store,
  ctx: WriteCtx,
  f: (db: Db<AnyFact, AnyFact>) => Effect.Effect<A, E, R>,
): Effect.Effect<A, E, R> => store.transaction(Effect.tap(f(makeDb(store, ctx)), () => settle(store)));

export const makeDb = (store: Store, ctx: WriteCtx): Db<AnyFact, AnyFact> => {
  const reader = makeReader(store);
  const kicked = new Set<string>();
  const { rollups, snapshots } = declared();
  const append = <F extends AnyFact>(fact: F, draft: Draft<F>) =>
    Effect.gen(function* () {
      if (fact.unique) {
        const eq = fact.key.map((k) => (draft as Record<string, string>)[k]!);
        const clash = yield* reader.find(fact, "by_key" as IndexName<F>, { eq, limit: 1 });
        if (clash.length > 0) {
          return yield* violation(
            `unique:${fact.id}`,
            `${fact.meta.label} already exists for ${eq.join(" / ")}`,
          );
        }
      }
      // The draft is checked against the fact's own schema at the disk edge: a cast upstream never lands a bad row.
      const encoded = yield* Schema.encodeUnknownEffect(
        fact.draft as unknown as Schema.Codec<unknown, unknown>,
      )(draft).pipe(
        Effect.mapError((e) => new Error(`kernel: ${fact.id} draft: ${e.message}`)),
        Effect.orDie,
      );
      const trace = ctx.trace ?? Option.getOrUndefined(yield* Trace.current);
      const doc = {
        ...(encoded as object),
        at: ctx.now,
        by: ctx.by,
        ...(ctx.via === undefined ? {} : { via: ctx.via }),
        ...(trace === undefined ? {} : { trace }),
      };
      const id = yield* store.insert(fact.table, doc);
      // Read back, so rollups and snapshots see the row as stored, with the store's own creation time.
      const row = Option.getOrElse(yield* store.get(fact.table, id), () => ({
        ...doc,
        _id: id,
        _creationTime: ctx.now,
      })) as never;
      for (const r of rollups) {
        if (r.on.id !== fact.id) continue;
        for (const [key, delta] of yield* r.apply(row, reader)) yield* store.tally.add(r.id, key, delta);
      }
      for (const s of snapshots) {
        if (!s.on.some((f) => f.id === fact.id)) continue;
        const touched = dirty.get(store) ?? dirty.set(store, new Map()).get(store)!;
        const keys = touched.get(s) ?? touched.set(s, new Set()).get(s)!;
        for (const key of yield* s.keys(fact, row, reader)) keys.add(key);
      }
      for (const rule of ofKind("rule")) {
        if (kicked.has(rule.id) || rule.impl === "stub") continue;
        if (rule.triggers.some((t) => t.on === "fact" && t.fact.id === fact.id)) {
          kicked.add(rule.id);
          yield* store.kick(rule.id);
        }
      }
      return id as Id<F["id"]>;
    });
  return { ...reader, append } as Db<AnyFact, AnyFact>;
};

export const latest = <T extends { readonly at: number }>(rows: ReadonlyArray<T>): Option.Option<T> =>
  rows.length === 0 ? Option.none() : Option.some(rows.reduce((a, b) => (b.at >= a.at ? b : a)));
