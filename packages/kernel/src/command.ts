import { Effect, Schema } from "effect";
import type { Db } from "./db.ts";
import type { InvariantViolation } from "./errors.ts";
import type { AnyFact } from "./fact.ts";
import { type Declared, type FnName, fnName, type GroupName, groupName, type Meta } from "./meta.ts";
import { register } from "./registry.ts";
import { isStub, NotImplemented, intentOf, type Stub } from "./stub.ts";

/** An agent that acts for a person under a `people::agent-grant`. */
export type Agent = "pea" | "pi";

/**
 * Who is acting. Roles are TC facts (memberships, access grants), not Clerk facts.
 * An agent acts as `agent:<name>` with `via` naming the person it acts for.
 */
export interface Actor {
  readonly by: string;
  readonly person: string;
  readonly roles: ReadonlySet<string>;
  readonly via?: string;
  readonly agent?: Agent;
}

export interface CommandCtx<R extends AnyFact, W extends AnyFact> {
  readonly db: Db<R, W>;
  readonly actor: Actor;
  /** Transaction time. Convex freezes Date.now per mutation, so this is deterministic. */
  readonly now: number;
}

/**
 * A human action that becomes facts. Runs inside one Convex mutation, which is a serializable
 * transaction, so "read an index, then insert" is a sound invariant check.
 */
export interface Command<
  Id extends string,
  Args extends Schema.Struct.Fields,
  Ret extends Schema.Top,
  R extends AnyFact,
  W extends AnyFact,
> extends Declared {
  readonly kind: "command";
  readonly id: Id;
  readonly fn: FnName<Id>;
  readonly group: GroupName<Id>;
  /** The role the actor must hold. `pm` is further narrowed by the command's own invariant. */
  // OPEN: `role` is an untyped string, so a misspelled role compiles and only fails when called;
  // a literal union from the actors list would make it a compile error.
  readonly role: string;
  readonly args: Args;
  readonly returns: Ret;
  readonly reads: ReadonlyArray<R>;
  readonly writes: ReadonlyArray<W>;
  readonly run: (
    args: Schema.Struct<Args>["Type"],
    ctx: CommandCtx<R, W>,
  ) => Effect.Effect<Ret["Type"], InvariantViolation>;
}

export type AnyCommand = Command<string, any, any, any, any>;

export const make = <
  const Id extends string,
  const Args extends Schema.Struct.Fields,
  Ret extends Schema.Top,
  const R extends AnyFact = never,
  const W extends AnyFact = never,
>(def: {
  readonly id: Id;
  readonly role: string;
  readonly args: Args;
  readonly returns: Ret;
  readonly reads?: ReadonlyArray<R>;
  readonly writes: ReadonlyArray<W>;
  readonly run:
    | Stub
    | ((
        args: Schema.Struct<Args>["Type"],
        ctx: CommandCtx<R, W>,
      ) => Effect.Effect<Ret["Type"], InvariantViolation>);
  readonly meta: Meta;
}): Command<Id, Args, Ret, R, W> => {
  const body = def.run;
  const run = isStub(body)
    ? () => Effect.die(new NotImplemented({ id: def.id, intent: intentOf(body) }))
    : Effect.fn(def.id)(function* (args: Schema.Struct<Args>["Type"], ctx: CommandCtx<R, W>) {
        yield* Effect.annotateCurrentSpan({ "tc.actor": ctx.actor.by, "tc.kind": "command" });
        return yield* body(args, ctx);
      });
  return register({
    kind: "command",
    impl: isStub(body) ? "stub" : "real",
    id: def.id,
    fn: fnName(def.id),
    group: groupName(def.id),
    role: def.role,
    args: def.args,
    returns: def.returns,
    reads: def.reads ?? [],
    writes: def.writes,
    run,
    meta: def.meta,
  });
};
