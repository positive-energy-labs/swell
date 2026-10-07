import { randomUUID } from "node:crypto";
import { SqliteClient } from "@effect/sql-sqlite-node";
import { type AnyFact, type Find, ofKind, type Store } from "@swell/kernel";
import { Context, Effect, Layer, Option, Schema, type Scope } from "effect";
import { SqlClient, SqlError, SqlSchema } from "effect/sql";

const Doc = Schema.fromJsonString(Schema.Record(Schema.String, Schema.Unknown));
const FactRow = Schema.Struct({ _id: Schema.String, _creationTime: Schema.Number, doc: Doc });
const Counts = Schema.fromJsonString(Schema.Record(Schema.String, Schema.Number));
const flat = (r: typeof FactRow.Type) => ({ ...r.doc, _id: r._id, _creationTime: r._creationTime });

/** The Store has no error channel: a storage or decode failure is a defect, which fails the transaction it is in. */
const orDie = <A, E, R>(fa: Effect.Effect<A, E, R>) =>
  fa.pipe(
    Effect.catchIf(
      (e: unknown): e is SqlError.SqlError | Schema.SchemaError =>
        SqlError.isSqlError(e) || Schema.isSchemaError(e),
      Effect.die,
    ),
  ) as Effect.Effect<A, Exclude<E, SqlError.SqlError | Schema.SchemaError>, R>;

/**
 * The controller's historian: the kernel's Store over SQLite, through Effect's SQL client as a driver under the
 * facts (it owns no state the facts do not). A JSON `doc` column per table, an expression index per declared fact
 * index, tallies and snapshots as two small tables. One connection: every query waits for an open transaction, so
 * nobody reads a write that may roll back, and a nested transaction is a savepoint. Same semantics as the kernel's
 * memory store, which is the spec.
 */
export const makeHistorian: Effect.Effect<Store, never, SqlClient.SqlClient> = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const col = (f: string) =>
    sql.literal(f === "_creationTime" ? "_creationTime" : `json_extract(doc, '$.${f}')`);
  const bind = (v: string | number | boolean) => (typeof v === "boolean" ? Number(v) : v);
  yield* orDie(
    Effect.gen(function* () {
      yield* sql`CREATE TABLE IF NOT EXISTS tally(id TEXT NOT NULL, key TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY(id, key))`;
      yield* sql`CREATE TABLE IF NOT EXISTS snap(id TEXT NOT NULL, key TEXT NOT NULL, shelf TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY(id, key))`;
      // Every registered fact's table and index exists before the first transaction, so a rolled-back CREATE can never poison a cache.
      for (const f of ofKind("fact") as ReadonlyArray<AnyFact>) {
        yield* sql`CREATE TABLE IF NOT EXISTS ${sql(f.table)}(_id TEXT PRIMARY KEY, _creationTime REAL NOT NULL, doc TEXT NOT NULL)`;
        for (const [name, fields] of Object.entries(f.indexes as Record<string, ReadonlyArray<string>>))
          yield* sql`CREATE INDEX IF NOT EXISTS ${sql(`${f.table}__${name}`)} ON ${sql(f.table)}(${sql.csv([...fields.map(col), col("_creationTime")])})`;
      }
    }),
  );

  // Strictly increasing across every table, like Convex's creation order; resumed from the file on open.
  let created = 0;
  for (const { name } of yield* orDie(
    sql<{
      name: string;
    }>`SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT IN ('tally', 'snap')`,
  )) {
    const [r] = yield* orDie(sql<{ m: number | null }>`SELECT MAX(_creationTime) m FROM ${sql(name)}`);
    created = Math.max(created, r?.m ?? 0);
  }

  const getRow = SqlSchema.findOneOption({
    Request: Schema.Struct({ t: Schema.String, id: Schema.String }),
    Result: FactRow,
    execute: ({ t, id }) => sql`SELECT _id, _creationTime, doc FROM ${sql(t)} WHERE _id = ${id}`,
  });
  const tallyRow = SqlSchema.findOneOption({
    Request: Schema.Struct({ id: Schema.String, key: Schema.String }),
    Result: Schema.Struct({ value: Counts }),
    execute: ({ id, key }) => sql`SELECT value FROM tally WHERE id = ${id} AND key = ${key}`,
  });
  const tallyRange = SqlSchema.findAll({
    Request: Schema.Struct({
      id: Schema.String,
      gte: Schema.String,
      lt: Schema.String,
      limit: Schema.Number,
    }),
    Result: Schema.Struct({ key: Schema.String, value: Counts }),
    execute: ({ id, gte, lt, limit }) =>
      sql`SELECT key, value FROM tally WHERE id = ${id} AND key >= ${gte} AND key < ${lt} ORDER BY key LIMIT ${limit}`,
  });
  const counts = (r: Option.Option<{ readonly value: Readonly<Record<string, number>> }>) =>
    Option.match(r, { onNone: () => ({}), onSome: (x) => x.value });

  return {
    get: (t, id) => orDie(getRow({ t, id }).pipe(Effect.map(Option.map(flat)))),
    find: (t, _name, fields, f: Find) => {
      const eq = f.eq ?? [];
      const next = fields[eq.length];
      const where = [
        ...eq.map((v, i) => sql`${col(fields[i]!)} = ${bind(v)}`),
        ...(next !== undefined && f.gte !== undefined ? [sql`${col(next)} >= ${bind(f.gte)}`] : []),
        ...(next !== undefined && f.lt !== undefined ? [sql`${col(next)} < ${bind(f.lt)}`] : []),
      ];
      const dir = sql.literal(f.order === "desc" ? "DESC" : "ASC");
      const order = [...fields, "_creationTime"].map((c) => sql`${col(c)} ${dir}`);
      return orDie(
        SqlSchema.findAll({
          Request: Schema.Void,
          Result: FactRow,
          execute: () =>
            sql`SELECT _id, _creationTime, doc FROM ${sql(t)} WHERE ${sql.and(where)} ORDER BY ${sql.csv(order)} LIMIT ${f.limit}`,
        })(undefined).pipe(Effect.map((rows) => rows.map(flat))),
      );
    },
    insert: (t, doc) =>
      Effect.suspend(() => {
        const _id = `${t}:${randomUUID()}`;
        created = Math.max(doc.at as number, created + 1e-3);
        return orDie(
          sql`INSERT INTO ${sql(t)} ${sql.insert({ _id, _creationTime: created, doc: JSON.stringify(doc) })}`.pipe(
            Effect.as(_id),
          ),
        );
      }),
    tally: {
      get: (id, key) => orDie(tallyRow({ id, key }).pipe(Effect.map(counts))),
      range: (id, gte, lt, limit) => orDie(tallyRange({ id, gte, lt, limit })),
      add: (id, key, delta) =>
        orDie(
          Effect.gen(function* () {
            const cur: Record<string, number> = { ...counts(yield* tallyRow({ id, key })) };
            for (const [k, v] of Object.entries(delta)) cur[k] = (cur[k] ?? 0) + v;
            yield* sql`INSERT OR REPLACE INTO tally(id, key, value) VALUES(${id}, ${key}, ${JSON.stringify(cur)})`;
          }),
        ),
    },
    snapshot: {
      get: (id, key) =>
        orDie(
          sql<{ value: string }>`SELECT value FROM snap WHERE id = ${id} AND key = ${key}`.pipe(
            Effect.map((rows) => Option.fromNullishOr(rows[0]?.value)),
          ),
        ),
      list: (id, shelf, limit) =>
        orDie(
          sql<{
            key: string;
            value: string;
          }>`SELECT key, value FROM snap WHERE id = ${id} AND shelf = ${shelf} ORDER BY key LIMIT ${limit}`,
        ),
      put: (id, key, row) =>
        orDie(
          row === null
            ? sql`DELETE FROM snap WHERE id = ${id} AND key = ${key}`.pipe(Effect.asVoid)
            : sql`INSERT OR REPLACE INTO snap(id, key, shelf, value) VALUES(${id}, ${key}, ${row.shelf}, ${row.value})`.pipe(
                Effect.asVoid,
              ),
        ),
    },
    // The controller sweeps every rule each tick, so a kick has nothing to wake.
    kick: () => Effect.void,
    transaction: <A, E, R>(fa: Effect.Effect<A, E, R>) =>
      sql.withTransaction(fa).pipe(Effect.catchIf(SqlError.isSqlError, Effect.die)) as Effect.Effect<A, E, R>,
  } satisfies Store;
});

/** The historian as a service: a Store over one SQLite file, open for the layer's lifetime. */
export class Historian extends Context.Service<Historian, Store>()("swell/Historian") {
  static readonly layer = (filename: string): Layer.Layer<Historian> =>
    Layer.effect(Historian, makeHistorian).pipe(Layer.provide(SqliteClient.layer({ filename })), Layer.orDie);
}

/** A Store over `filename`, open until the caller's scope closes. */
export const historian = (filename: string): Effect.Effect<Store, never, Scope.Scope> =>
  Layer.build(Historian.layer(filename)).pipe(Effect.map((ctx) => Context.get(ctx, Historian)));
