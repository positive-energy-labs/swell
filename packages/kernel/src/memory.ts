import { Clock, Effect, type Layer, Option, type Schema } from "effect";
import type { Db, Find } from "./db.ts";
import type { AnyFact } from "./fact.ts";
import type { Actor, Command } from "./command.ts";
import type { Entry } from "./entry.ts";
import { type CommandError, Unauthorized } from "./errors.ts";
import type { AnyPort } from "./port.ts";
import { lookup, ofKind } from "./registry.ts";
import type { AnyRule } from "./rule.ts";
import { makeDb, makeReader, settle, type Store, transact } from "./store.ts";
import { complete, execute, type Job, plan, sweep } from "./sweep.ts";
import * as Trace from "./trace.ts";

const cmp = (a: unknown, b: unknown) => (a === b ? 0 : (a as number) < (b as number) ? -1 : 1);

/** An in-memory Store with the same semantics as the Convex adapter, so kernel behavior is proven without a deployment. */
export const memoryStore = () => {
  const tables = new Map<string, Array<Record<string, unknown>>>();
  const tallies = new Map<string, Record<string, number>>();
  const snapshots = new Map<string, { id: string; key: string; shelf: string; value: string }>();
  const kicks: Array<string> = [];
  let seq = 0;
  let created = 0;
  const rows = (t: string) => tables.get(t) ?? tables.set(t, []).get(t)!;
  const store: Store = {
    get: (t, id) => Effect.sync(() => Option.fromNullishOr(rows(t).find((r) => r._id === id))),
    find: (t, _index, fields, f: Find) =>
      Effect.sync(() => {
        const eq = f.eq ?? [];
        const next = fields[eq.length];
        const hit = rows(t).filter(
          (r) =>
            eq.every((v, i) => r[fields[i]!] === v) &&
            (f.gte === undefined || cmp(r[next!], f.gte) >= 0) &&
            (f.lt === undefined || cmp(r[next!], f.lt) < 0),
        );
        const sorted = hit.sort((a, b) => {
          for (const k of [...fields, "_creationTime"]) {
            const c = cmp(a[k], b[k]);
            if (c !== 0) return c;
          }
          return 0;
        });
        return (f.order === "desc" ? sorted.reverse() : sorted).slice(0, f.limit);
      }),
    insert: (t, doc) =>
      Effect.sync(() => {
        const id = `${t}:${++seq}`;
        // Strictly increasing across every table, like Convex's creation order (1e-3 survives epoch-ms floats).
        created = Math.max(doc.at as number, created + 1e-3);
        rows(t).push({ ...doc, _id: id, _creationTime: created });
        return id;
      }),
    tally: {
      get: (id, key) => Effect.sync(() => tallies.get(`${id}|${key}`) ?? {}),
      range: (id, gte, lt, limit) =>
        Effect.sync(() =>
          [...tallies]
            .filter(([k]) => k.startsWith(`${id}|`))
            .map(([k, value]) => ({ key: k.slice(id.length + 1), value }))
            .filter((r) => r.key >= gte && r.key < lt)
            .sort((a, b) => cmp(a.key, b.key))
            .slice(0, limit),
        ),
      add: (id, key, delta) =>
        Effect.sync(() => {
          const cur = { ...tallies.get(`${id}|${key}`) };
          for (const [k, v] of Object.entries(delta)) cur[k] = (cur[k] ?? 0) + v;
          tallies.set(`${id}|${key}`, cur);
        }),
    },
    snapshot: {
      get: (id, key) => Effect.sync(() => Option.fromNullishOr(snapshots.get(`${id}|${key}`)?.value)),
      list: (id, shelf, limit) =>
        Effect.sync(() =>
          [...snapshots.values()]
            .filter((r) => r.id === id && r.shelf === shelf)
            .sort((a, b) => cmp(a.key, b.key))
            .slice(0, limit),
        ),
      put: (id, key, row) =>
        Effect.sync(() => {
          if (row === null) snapshots.delete(`${id}|${key}`);
          else snapshots.set(`${id}|${key}`, { id, key, ...row });
        }),
    },
    kick: (rule) => Effect.sync(() => void kicks.push(rule)),
    transaction: (fa) => fa,
  };
  return { store, tables, tallies, snapshots, kicks };
};

/**
 * The whole system in one process: commands, entries, sweep, a job queue, drain, health. Over a memory
 * store with every port faked it is the simulator; over SQLite with live ports it is a host. `now` comes
 * from Effect's Clock, so TestClock drives time in tests.
 */
export const simulator = <M extends { readonly store: Store } = ReturnType<typeof memoryStore>>(
  ports: (port: AnyPort) => Layer.Layer<any> = (p) => p.fake as Layer.Layer<any>,
  mem: M = memoryStore() as unknown as M,
  by = "host",
) => {
  const queue: Array<Job> = [];

  const entry = (e: Entry, payload: unknown) =>
    Effect.gen(function* () {
      const now = yield* Clock.currentTimeMillis;
      return yield* transact(mem.store, { by, now, trace: undefined }, (db) =>
        e.handle(payload, { db, now }),
      );
    });

  const command = <
    Args extends Schema.Struct.Fields,
    Ret extends Schema.Top,
    R extends AnyFact,
    W extends AnyFact,
  >(
    cmd: Command<string, Args, Ret, R, W>,
    args: Schema.Struct<Args>["Type"],
    actor: Actor,
  ): Effect.Effect<Ret["Type"], CommandError> =>
    Effect.gen(function* () {
      if (!actor.roles.has(cmd.role)) return yield* new Unauthorized({ need: cmd.role });
      const now = yield* Clock.currentTimeMillis;
      return yield* transact(mem.store, { by: actor.by, now, trace: undefined }, (db) =>
        cmd.run(args, { db: db as Db<R, W>, actor, now }),
      );
    });

  const sweepRule = (rule: AnyRule) =>
    Effect.gen(function* () {
      const now = yield* Clock.currentTimeMillis;
      return yield* transact(mem.store, { by: `rule:${rule.id}`, now, trace: undefined }, (db) =>
        sweep(rule, db, now, (job) => Effect.sync(() => void queue.push(job))),
      );
    });

  const drain = Effect.gen(function* () {
    while (queue.length > 0) {
      const job = queue.shift()!;
      const rule = lookup(job.rule) as AnyRule;
      const settled = yield* execute(rule, job, ports);
      if (settled.outcome === "killed") continue;
      const now = yield* Clock.currentTimeMillis;
      const db = makeDb(mem.store, { by: `rule:${rule.id}`, now, trace: undefined });
      yield* complete(rule, job, settled, db).pipe(Trace.continueFrom(job.traceparent));
      yield* settle(mem.store);
    }
  });

  const health = Effect.gen(function* () {
    const now = yield* Clock.currentTimeMillis;
    const reader = makeReader(mem.store);
    return yield* Effect.forEach(ofKind("rule"), (r) => plan(r, reader, now));
  });

  return { ...mem, reader: makeReader(mem.store), queue, command, entry, sweep: sweepRule, drain, health };
};
