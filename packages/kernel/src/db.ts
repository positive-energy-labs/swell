import type { Effect, Option } from "effect";
import type { InvariantViolation } from "./errors.ts";
import type { AnyFact, Draft, Id, IndexName, Row } from "./fact.ts";
import type { Snapshot } from "./projection.ts";

export type Value = string | number | boolean;

/**
 * Every read names an index and a limit. There is no scan: Convex fails a transaction past 32k
 * documents or 16 MiB, so an unbounded read is a latent outage, not a slow query.
 */
export interface Find {
  readonly eq?: ReadonlyArray<Value>;
  readonly gte?: Value;
  readonly lt?: Value;
  readonly order?: "asc" | "desc";
  readonly limit: number;
}
export const MAX_LIMIT = 2048;

export interface Reader<R extends AnyFact> {
  readonly get: <F extends R>(fact: F, id: Id<F["id"]>) => Effect.Effect<Option.Option<Row<F>>>;
  readonly find: <F extends R>(
    fact: F,
    index: IndexName<F>,
    find: Find,
  ) => Effect.Effect<ReadonlyArray<Row<F>>>;
  readonly tally: (projection: string, key: string) => Effect.Effect<Readonly<Record<string, number>>>;
  /** The tally rows with `gte <= key < lt`, in key order: one read for a run of keys that share a prefix. */
  readonly tallies: (
    projection: string,
    range: { readonly gte: string; readonly lt: string; readonly limit: number },
  ) => Effect.Effect<
    ReadonlyArray<{ readonly key: string; readonly value: Readonly<Record<string, number>> }>
  >;
  readonly snapshot: <V>(s: Snapshot<V>, key: string) => Effect.Effect<Option.Option<V>>;
  /** The rows on one shelf of a snapshot, in key order. */
  readonly shelf: <V>(
    s: Snapshot<V>,
    shelf: string,
    limit: number,
  ) => Effect.Effect<ReadonlyArray<{ readonly key: string; readonly value: V }>>;
}

export interface Writer<W extends AnyFact> {
  readonly append: <F extends W>(fact: F, draft: Draft<F>) => Effect.Effect<Id<F["id"]>, InvariantViolation>;
}

export interface Db<R extends AnyFact, W extends AnyFact> extends Reader<R | W>, Writer<W> {}
