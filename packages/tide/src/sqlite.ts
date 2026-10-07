import { randomUUID } from "node:crypto";
import { DatabaseSync } from "node:sqlite";
import { type AnyFact, type Find, ofKind, type Store } from "@tc/kernel";
import { Context, Effect, Exit, Option, Semaphore } from "effect";

type Row = { _id: string; _creationTime: number; doc: string };

const bind = (v: unknown) => (typeof v === "boolean" ? (v ? 1 : 0) : (v as string | number));

/**
 * The kernel's Store over one SQLite file: a JSON `doc` column per table, an expression index per declared
 * fact index, tallies and snapshots as two small tables. Same semantics as the memory store, which is the spec.
 */
export const sqliteStore = (path: string) => {
  const db = new DatabaseSync(path);
  db.exec(`
    PRAGMA journal_mode = WAL;
    PRAGMA busy_timeout = 5000;
    CREATE TABLE IF NOT EXISTS tally(id TEXT NOT NULL, key TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY(id, key));
    CREATE TABLE IF NOT EXISTS snap(id TEXT NOT NULL, key TEXT NOT NULL, shelf TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY(id, key));
  `);
  const col = (f: string) => (f === "_creationTime" ? "_creationTime" : `json_extract(doc, '$.${f}')`);
  const table = (t: string) => `"${t}"`;
  // Every registered fact's table and index exists before the first transaction, so a rolled-back CREATE can never poison a cache.
  for (const f of ofKind("fact") as ReadonlyArray<AnyFact>) {
    db.exec(
      `CREATE TABLE IF NOT EXISTS ${table(f.table)}(_id TEXT PRIMARY KEY, _creationTime REAL NOT NULL, doc TEXT NOT NULL)`,
    );
    for (const [name, fields] of Object.entries(f.indexes as Record<string, ReadonlyArray<string>>)) {
      db.exec(
        `CREATE INDEX IF NOT EXISTS "${f.table}__${name}" ON ${table(f.table)}(${[...fields.map(col), "_creationTime"].join(", ")})`,
      );
    }
  }
  const parse = (r: Row) => ({
    ...(JSON.parse(r.doc) as object),
    _id: r._id,
    _creationTime: r._creationTime,
  });

  // Strictly increasing across every table, like Convex's creation order; resumed from the file on open.
  let created = 0;
  for (const { name } of db.prepare(`SELECT name FROM sqlite_master WHERE type = 'table'`).all() as Array<{
    name: string;
  }>) {
    if (name === "tally" || name === "snap") continue;
    const { m } = db.prepare(`SELECT MAX(_creationTime) m FROM "${name}"`).get() as { m: number | null };
    created = Math.max(created, m ?? 0);
  }

  // One writer at a time, and only the fiber that opened the transaction is inside it: another fiber waits, it never joins.
  const inTx = Context.Reference<boolean>(`tide/inTx/${randomUUID()}`, { defaultValue: () => false });
  const lock = Semaphore.makeUnsafe(1);
  const finish = (exit: Exit.Exit<unknown, unknown>) =>
    Effect.sync(() => {
      if (!Exit.isSuccess(exit)) return db.exec("ROLLBACK");
      try {
        db.exec("COMMIT");
      } catch (e) {
        db.exec("ROLLBACK");
        throw e;
      }
    });

  const store: Store = {
    get: (t, id) =>
      Effect.sync(() => {
        const r = db.prepare(`SELECT _id, _creationTime, doc FROM ${table(t)} WHERE _id = ?`).get(id) as
          | Row
          | undefined;
        return r === undefined ? Option.none() : Option.some(parse(r));
      }),
    find: (t, _name, fields, f: Find) =>
      Effect.sync(() => {
        const eq = f.eq ?? [];
        const where: Array<string> = [];
        const params: Array<string | number> = [];
        eq.forEach((v, i) => {
          where.push(`${col(fields[i]!)} = ?`);
          params.push(bind(v));
        });
        const next = fields[eq.length];
        if (next !== undefined && f.gte !== undefined) {
          where.push(`${col(next)} >= ?`);
          params.push(bind(f.gte));
        }
        if (next !== undefined && f.lt !== undefined) {
          where.push(`${col(next)} < ?`);
          params.push(bind(f.lt));
        }
        const dir = f.order === "desc" ? "DESC" : "ASC";
        const order = [...fields.map(col), "_creationTime"].map((c) => `${c} ${dir}`).join(", ");
        const sql = `SELECT _id, _creationTime, doc FROM ${table(t)}${where.length > 0 ? ` WHERE ${where.join(" AND ")}` : ""} ORDER BY ${order} LIMIT ?`;
        return (db.prepare(sql).all(...params, f.limit) as Array<Row>).map(parse);
      }),
    insert: (t, doc) =>
      Effect.sync(() => {
        const id = `${t}:${randomUUID().slice(0, 12)}`;
        created = Math.max(doc.at as number, created + 1e-3);
        db.prepare(`INSERT INTO ${table(t)}(_id, _creationTime, doc) VALUES(?, ?, ?)`).run(
          id,
          created,
          JSON.stringify(doc),
        );
        return id;
      }),
    tally: {
      get: (id, key) =>
        Effect.sync(() => {
          const r = db.prepare(`SELECT value FROM tally WHERE id = ? AND key = ?`).get(id, key) as
            | { value: string }
            | undefined;
          return r === undefined ? {} : (JSON.parse(r.value) as Record<string, number>);
        }),
      range: (id, gte, lt, limit) =>
        Effect.sync(() =>
          (
            db
              .prepare(
                `SELECT key, value FROM tally WHERE id = ? AND key >= ? AND key < ? ORDER BY key LIMIT ?`,
              )
              .all(id, gte, lt, limit) as Array<{ key: string; value: string }>
          ).map((r) => ({ key: r.key, value: JSON.parse(r.value) as Record<string, number> })),
        ),
      add: (id, key, delta) =>
        Effect.sync(() => {
          const r = db.prepare(`SELECT value FROM tally WHERE id = ? AND key = ?`).get(id, key) as
            | { value: string }
            | undefined;
          const cur: Record<string, number> =
            r === undefined ? {} : (JSON.parse(r.value) as Record<string, number>);
          for (const [k, v] of Object.entries(delta)) cur[k] = (cur[k] ?? 0) + v;
          db.prepare(`INSERT OR REPLACE INTO tally(id, key, value) VALUES(?, ?, ?)`).run(
            id,
            key,
            JSON.stringify(cur),
          );
        }),
    },
    snapshot: {
      get: (id, key) =>
        Effect.sync(() => {
          const r = db.prepare(`SELECT value FROM snap WHERE id = ? AND key = ?`).get(id, key) as
            | { value: string }
            | undefined;
          return r === undefined ? Option.none() : Option.some(r.value);
        }),
      list: (id, shelf, limit) =>
        Effect.sync(
          () =>
            db
              .prepare(`SELECT key, value FROM snap WHERE id = ? AND shelf = ? ORDER BY key LIMIT ?`)
              .all(id, shelf, limit) as Array<{ key: string; value: string }>,
        ),
      put: (id, key, row) =>
        Effect.sync(() => {
          if (row === null) db.prepare(`DELETE FROM snap WHERE id = ? AND key = ?`).run(id, key);
          else
            db.prepare(`INSERT OR REPLACE INTO snap(id, key, shelf, value) VALUES(?, ?, ?, ?)`).run(
              id,
              key,
              row.shelf,
              row.value,
            );
        }),
    },
    // The host sweeps every rule each tick, so a kick has nothing to wake.
    kick: () => Effect.void,
    transaction: (fa) =>
      Effect.gen(function* () {
        if (yield* inTx) return yield* fa;
        return yield* lock.withPermits(1)(
          Effect.acquireUseRelease(
            Effect.sync(() => db.exec("BEGIN IMMEDIATE")),
            () => Effect.provideService(fa, inTx, true),
            (_, exit) => finish(exit),
          ),
        );
      }),
  };
  return { store, db, close: () => db.close() };
};
