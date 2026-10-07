import { Effect, Schema } from "effect";
import type { Db } from "./db.ts";
import type { InvariantViolation } from "./errors.ts";
import type { AnyFact } from "./fact.ts";
import type { Declared, Meta } from "./meta.ts";
import { register } from "./registry.ts";
import { isStub, NotImplemented, intentOf, type Stub } from "./stub.ts";

/** Where an inbound thing came from: the origin system by name. A domain narrows it to its own literals. */
export const Source = Schema.String;
export type Source = typeof Source.Type;

export interface EntryCtx {
  readonly db: Db<AnyFact, AnyFact>;
  readonly now: number;
}

/** An inbound door (Gmail push, Chat event, Revit runner queue, legacy import). It declares what it writes. */
export interface Entry extends Declared {
  readonly kind: "entry";
  readonly source: Source;
  readonly from: string;
  readonly reads: ReadonlyArray<AnyFact>;
  readonly writes: ReadonlyArray<AnyFact>;
  readonly handle: (payload: unknown, ctx: EntryCtx) => Effect.Effect<unknown, InvariantViolation>;
}

export const make = (def: {
  readonly id: string;
  readonly source: Source;
  readonly from: string;
  readonly reads?: ReadonlyArray<AnyFact>;
  readonly writes: ReadonlyArray<AnyFact>;
  readonly handle: Stub | ((payload: unknown, ctx: EntryCtx) => Effect.Effect<unknown, InvariantViolation>);
  readonly meta: Meta;
}): Entry => {
  const body = def.handle;
  return register({
    kind: "entry",
    impl: isStub(body) ? "stub" : "real",
    id: def.id,
    source: def.source,
    from: def.from,
    reads: def.reads ?? [],
    writes: def.writes,
    handle: isStub(body)
      ? () => Effect.die(new NotImplemented({ id: def.id, intent: intentOf(body) }))
      : Effect.fn(def.id)(function* (payload: unknown, ctx: EntryCtx) {
          return yield* body(payload, ctx);
        }),
    meta: def.meta,
  });
};
