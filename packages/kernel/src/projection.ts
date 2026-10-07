import { Effect, type Schema } from "effect";
import type { Actor } from "./command.ts";
import type { Reader } from "./db.ts";
import type { AnyFact, Row } from "./fact.ts";
import { type Declared, type FnName, fnName, type GroupName, groupName, type Meta } from "./meta.ts";
import { register } from "./registry.ts";
import { intentOf, isStub, NotImplemented, type Stub } from "./stub.ts";

/**
 * When a fact of kind `on` is appended, the kernel adds each returned delta to the numeric map at
 * (`id`, key) in the same transaction, so a projection stays index-bounded as history grows.
 * A correction can move value between keys, so `apply` returns every key it touches.
 * Several rollups may share an `id` to fold different facts into one row.
 */
// OPEN: tallies and snapshots only move on new appends, so changing an `apply` or `build` leaves old
// rows on the old maths. Before cutover a wiped deployment fixes it; after, the first such change
// ships with a one-off recount from facts (decided 2026-09-30: no general rebuild until then).
export interface Rollup<F extends AnyFact = AnyFact> {
  readonly id: string;
  readonly on: F;
  readonly apply: (
    row: Row<F>,
    db: Reader<AnyFact>,
  ) => Effect.Effect<ReadonlyArray<readonly [key: string, delta: Readonly<Record<string, number>>]>>;
}
export const rollup = <F extends AnyFact>(r: Rollup<F>): Rollup<F> => r;

/**
 * A whole derived row per key, rebuilt by `build` once per transaction that appended a fact in `on`
 * touching that key; `settle` runs the rebuilds before the transaction ends. For views over facts that
 * change rarely and are read often. `shelf` groups the rows a view lists, so a list reads one row per
 * key and never a history. The value must be plain JSON.
 */
export interface Snapshot<V = unknown> {
  readonly id: string;
  readonly on: ReadonlyArray<AnyFact>;
  readonly keys: (
    fact: AnyFact,
    row: Row<AnyFact>,
    db: Reader<AnyFact>,
  ) => Effect.Effect<ReadonlyArray<string>>;
  readonly build: (
    key: string,
    db: Reader<AnyFact>,
  ) => Effect.Effect<{ readonly shelf: string; readonly value: V } | null>;
}
export const snapshot = <V>(s: Snapshot<V>): Snapshot<V> => s;

export interface Projection<
  Id extends string,
  Args extends Schema.Struct.Fields,
  Ret extends Schema.Top,
  R extends AnyFact,
> extends Declared {
  readonly kind: "projection";
  readonly id: Id;
  readonly fn: FnName<Id>;
  readonly group: GroupName<Id>;
  readonly args: Args;
  readonly returns: Ret;
  readonly reads: ReadonlyArray<R>;
  readonly readsRules: boolean;
  readonly shows: ReadonlyArray<string>;
  readonly rollups: ReadonlyArray<Rollup<any>>;
  readonly snapshots: ReadonlyArray<Snapshot<any>>;
  /** `viewer` is the resolved caller: every view is read by a person, never anonymously. */
  readonly run: (
    args: Schema.Struct<Args>["Type"],
    db: Reader<R>,
    viewer: Actor,
  ) => Effect.Effect<Ret["Type"]>;
}

export type AnyProjection = Projection<string, any, any, any>;

export const make = <
  const Id extends string,
  const Args extends Schema.Struct.Fields,
  Ret extends Schema.Top,
  const R extends AnyFact,
  const RR extends boolean = false,
>(def: {
  readonly id: Id;
  readonly args: Args;
  readonly returns: Ret;
  readonly reads: ReadonlyArray<R>;
  /** It runs rules' `want`, so it reads whatever any rule reads: its reader is untyped on purpose. */
  readonly readsRules?: RR;
  readonly shows: ReadonlyArray<string>;
  readonly rollups?: ReadonlyArray<Rollup<any>>;
  readonly snapshots?: ReadonlyArray<Snapshot<any>>;
  readonly run:
    | Stub
    | ((
        args: Schema.Struct<Args>["Type"],
        db: Reader<RR extends true ? AnyFact : R>,
        viewer: Actor,
      ) => Effect.Effect<Ret["Type"]>);
  readonly meta: Meta;
}): Projection<Id, Args, Ret, RR extends true ? AnyFact : R> => {
  const body = def.run;
  return register({
    kind: "projection",
    impl: isStub(body) ? "stub" : "real",
    id: def.id,
    fn: fnName(def.id),
    group: groupName(def.id),
    args: def.args,
    returns: def.returns,
    reads: def.reads,
    readsRules: def.readsRules ?? false,
    shows: def.shows,
    rollups: def.rollups ?? [],
    snapshots: def.snapshots ?? [],
    run: isStub(body)
      ? () => Effect.die(new NotImplemented({ id: def.id, intent: intentOf(body) }))
      : Effect.fn(def.id)(function* (
          args: Schema.Struct<Args>["Type"],
          db: Reader<RR extends true ? AnyFact : R>,
          viewer: Actor,
        ) {
          return yield* body(args, db, viewer);
        }),
    meta: def.meta,
  });
};
