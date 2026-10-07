import { Clock, Context, Effect, Exit, type Layer, Option, Schema, Semaphore } from "effect";
import type { Db, Find } from "./db.ts";
import type { AnyFact } from "./fact.ts";
import type { Actor, Command } from "./command.ts";
import type { Entry } from "./entry.ts";
import { type CommandError, Unauthorized } from "./errors.ts";
import type { AnyPort } from "./port.ts";
import { lookup, ofKind } from "./registry.ts";
import type { AnyRule } from "./rule.ts";
import { makeReader, type Store, transact, type WriteCtx } from "./store.ts";
import { complete, enablerOf, execute, type Job, plan, sweep } from "./sweep.ts";
import * as Trace from "./trace.ts";

const cmp = (a: unknown, b: unknown) => (a === b ? 0 : (a as number) < (b as number) ? -1 : 1);

let instances = 0;

/**
 * An in-memory Store with the same semantics as the Convex adapter, so kernel behavior is proven without a
 * deployment. One writer at a time, and a read outside a transaction waits for the open one, as on a single
 * SQLite connection: nobody sees a write that may still roll back. A nested transaction is a savepoint: its
 * failure rolls back its own writes only.
 */
export const memoryStore = () => {
  const tables = new Map<string, Array<Record<string, unknown>>>();
  const tallies = new Map<string, Record<string, number>>();
  const snapshots = new Map<string, { id: string; key: string; shelf: string; value: string }>();
  const kicks: Array<string> = [];
  let seq = 0;
  let created = 0;
  const rows = (t: string) => tables.get(t) ?? tables.set(t, []).get(t)!;
  const inTx = Context.Reference<boolean>(`swell/kernel/memory/inTx/${++instances}`, {
    defaultValue: () => false,
  });
  const lock = Semaphore.makeUnsafe(1);
  const read = <A>(f: () => A): Effect.Effect<A> =>
    Effect.gen(function* () {
      return (yield* inTx) ? f() : yield* lock.withPermits(1)(Effect.sync(f));
    });
  const savepoint = <A, E, R>(fa: Effect.Effect<A, E, R>) =>
    Effect.suspend(() => {
      const before = {
        tables: new Map<string, Array<Record<string, unknown>>>([...tables].map(([k, v]) => [k, [...v]])),
        tallies: new Map<string, Record<string, number>>([...tallies].map(([k, v]) => [k, { ...v }])),
        snapshots: new Map(snapshots),
        kicks: kicks.length,
        seq,
        created,
      };
      return fa.pipe(
        Effect.onExit((exit) =>
          Effect.sync(() => {
            if (Exit.isSuccess(exit)) return;
            tables.clear();
            for (const [k, v] of before.tables) tables.set(k, v);
            tallies.clear();
            for (const [k, v] of before.tallies) tallies.set(k, v);
            snapshots.clear();
            for (const [k, v] of before.snapshots) snapshots.set(k, v);
            kicks.length = before.kicks;
            seq = before.seq;
            created = before.created;
          }),
        ),
      );
    });
  const store: Store = {
    get: (t, id) => read(() => Option.fromNullishOr(rows(t).find((r) => r._id === id))),
    find: (t, _index, fields, f: Find) =>
      read(() => {
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
      read(() => {
        const id = `${t}:${++seq}`;
        // Strictly increasing across every table, like Convex's creation order (1e-3 survives epoch-ms floats).
        created = Math.max(doc.at as number, created + 1e-3);
        rows(t).push({ ...doc, _id: id, _creationTime: created });
        return id;
      }),
    tally: {
      get: (id, key) => read(() => tallies.get(`${id}|${key}`) ?? {}),
      range: (id, gte, lt, limit) =>
        read(() =>
          [...tallies]
            .filter(([k]) => k.startsWith(`${id}|`))
            .map(([k, value]) => ({ key: k.slice(id.length + 1), value }))
            .filter((r) => r.key >= gte && r.key < lt)
            .sort((a, b) => cmp(a.key, b.key))
            .slice(0, limit),
        ),
      add: (id, key, delta) =>
        read(() => {
          const cur = { ...tallies.get(`${id}|${key}`) };
          for (const [k, v] of Object.entries(delta)) cur[k] = (cur[k] ?? 0) + v;
          tallies.set(`${id}|${key}`, cur);
        }),
    },
    snapshot: {
      get: (id, key) => read(() => Option.fromNullishOr(snapshots.get(`${id}|${key}`)?.value)),
      list: (id, shelf, limit) =>
        read(() =>
          [...snapshots.values()]
            .filter((r) => r.id === id && r.shelf === shelf)
            .sort((a, b) => cmp(a.key, b.key))
            .slice(0, limit),
        ),
      put: (id, key, row) =>
        read(() => {
          if (row === null) snapshots.delete(`${id}|${key}`);
          else snapshots.set(`${id}|${key}`, { id, key, ...row });
        }),
    },
    kick: (rule) => read(() => void kicks.push(rule)),
    // A failed or interrupted transaction leaves nothing behind, as SQLite's ROLLBACK does: the spec covers the failure path.
    transaction: (fa) =>
      Effect.gen(function* () {
        if (yield* inTx) return yield* savepoint(fa);
        return yield* lock.withPermits(1)(Effect.provideService(savepoint(fa), inTx, true));
      }),
  };
  return { store, tables, tallies, snapshots, kicks };
};

export interface SimulatorOptions {
  /** The rules this simulator answers for in `health`. Default: every registered rule. */
  readonly rules?: () => ReadonlyArray<AnyRule>;
}

/**
 * The whole system in one process: commands, entries, sweep, a job queue, drain, health. Over a memory
 * store with every port faked it is the simulator; over SQLite with live ports it is a host. `now` comes
 * from Effect's Clock, so TestClock drives time in tests. Each simulator has its own queue, so several over
 * one store drain only their own jobs.
 */
export const simulator = <M extends { readonly store: Store } = ReturnType<typeof memoryStore>>(
  ports: (port: AnyPort) => Layer.Layer<any> = (p) => p.fake as Layer.Layer<any>,
  mem: M = memoryStore() as unknown as M,
  by = "host",
  options: SimulatorOptions = {},
) => {
  const queue: Array<Job> = [];
  const reader = makeReader(mem.store);

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
      // The command's declared args are applied, so a caller cannot smuggle a string where a boolean is declared.
      const decoded = (yield* (
        Schema.decodeUnknownEffect(Schema.Struct(cmd.args))(args) as Effect.Effect<
          unknown,
          Schema.SchemaError
        >
      ).pipe(Effect.orDie)) as Schema.Struct<Args>["Type"];
      const now = yield* Clock.currentTimeMillis;
      return yield* transact(mem.store, { by: actor.by, via: actor.via, now, trace: undefined }, (db) =>
        cmd.run(decoded, { db: db as Db<R, W>, actor, now }),
      );
    });

  /** A rule writes as itself, for whoever enabled it. */
  const ruleCtx = (rule: AnyRule) =>
    Effect.gen(function* () {
      const now = yield* Clock.currentTimeMillis;
      const via = yield* enablerOf(rule.id, reader);
      return { by: `rule:${rule.id}`, via, now, trace: undefined } satisfies WriteCtx;
    });

  const sweepRule = (rule: AnyRule) =>
    Effect.gen(function* () {
      const ctx = yield* ruleCtx(rule);
      return yield* transact(mem.store, ctx, (db) =>
        sweep(rule, db, ctx.now, (job) => Effect.sync(() => void queue.push(job))),
      );
    });

  const drain = Effect.gen(function* () {
    while (queue.length > 0) {
      const job = queue.shift()!;
      const rule = lookup(job.rule) as AnyRule | undefined;
      // A rule forgotten since its sweep (its controller closed) has nobody left to settle for.
      if (rule === undefined) continue;
      const settled = yield* execute(rule, job, ports);
      if (settled.outcome === "killed") continue;
      const ctx = yield* ruleCtx(rule);
      // One transaction: the output facts and the receipt land together, so a retry never re-appends them. A
      // write the store refuses settles the attempt as failed instead, so it counts toward maxAttempts.
      yield* transact(mem.store, ctx, (db) => complete(rule, job, settled, db)).pipe(
        Effect.catchTag("InvariantViolation", (e) =>
          transact(mem.store, ctx, (db) =>
            complete(rule, job, { outcome: "failed", error: `${e.invariant}: ${e.message}` }, db),
          ).pipe(Effect.orDie),
        ),
        Trace.continueFrom(job.traceparent),
      );
    }
  });

  const health = Effect.gen(function* () {
    const now = yield* Clock.currentTimeMillis;
    return yield* Effect.forEach(options.rules?.() ?? ofKind("rule"), (r) => plan(r, reader, now));
  });

  return { ...mem, reader, queue, command, entry, sweep: sweepRule, drain, health };
};
