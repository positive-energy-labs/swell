import { Effect, type Schema } from "effect";
import type { Reader } from "./db.ts";
import type { AnyFact, Draft } from "./fact.ts";
import type { Declared, Meta } from "./meta.ts";
import type { AnyPort, ServiceOf } from "./port.ts";
import { register } from "./registry.ts";
import { intentOf, isStub, NotImplemented, type Stub } from "./stub.ts";

export type Trigger<R extends AnyFact> =
  | { readonly on: "fact"; readonly fact: R }
  | { readonly on: "cron"; readonly cron: string; readonly clock: AnyPort };

export const onFact = <F extends AnyFact>(fact: F): Trigger<F> => ({ on: "fact", fact });
export const onCron = (cron: string, clock: AnyPort): Trigger<never> => ({
  on: "cron",
  cron,
  clock,
});

/** One thing a rule wants to exist. `urn` plus the rule id is the receipt key. */
export interface Subject {
  readonly urn: string;
}

export type Append<W extends AnyFact> = W extends AnyFact
  ? { readonly fact: W; readonly draft: Draft<W> }
  : never;

export interface Outcome<W extends AnyFact> {
  readonly result: string;
  readonly append?: ReadonlyArray<Append<W>>;
}

export interface WantCtx {
  readonly now: number;
  /** Only subjects born after the rule was enabled count, so imported history is inert. */
  readonly enabledAt: number;
}

/**
 * A level-triggered rule: `want(facts) -> subjects`, and `effect(subject)` makes one exist.
 * The effect's R channel must sit inside `uses`: an undeclared port is a compile error.
 */
export interface Rule<
  Id extends string,
  R extends AnyFact,
  U extends AnyPort,
  W extends AnyFact,
  S extends Subject,
> extends Declared {
  readonly kind: "rule";
  readonly id: Id;
  readonly reads: ReadonlyArray<R>;
  readonly uses: ReadonlyArray<U>;
  readonly writes: ReadonlyArray<W>;
  readonly triggers: ReadonlyArray<Trigger<R>>;
  readonly subject: Schema.Codec<S, any>;
  readonly maxAttempts: number;
  /** An attempt with no receipt older than this is presumed dead (its action was killed). */
  readonly leaseMs: number;
  readonly want: (db: Reader<R>, ctx: WantCtx) => Effect.Effect<ReadonlyArray<S>>;
  readonly effect: (subject: S) => Effect.Effect<Outcome<W>, unknown, ServiceOf<U>>;
}

export type AnyRule = Rule<string, any, any, any, any>;

export const make = <
  const Id extends string,
  const R extends AnyFact,
  const U extends AnyPort,
  S extends Subject,
  const W extends AnyFact = never,
>(def: {
  readonly id: Id;
  readonly reads: ReadonlyArray<R>;
  readonly uses: ReadonlyArray<U>;
  readonly writes?: ReadonlyArray<W>;
  readonly triggers: ReadonlyArray<Trigger<R>>;
  readonly subject: Schema.Codec<S, any>;
  readonly maxAttempts?: number;
  readonly leaseMs?: number;
  readonly want: Stub | ((db: Reader<R>, ctx: WantCtx) => Effect.Effect<ReadonlyArray<S>>);
  readonly effect: Stub | ((subject: S) => Effect.Effect<Outcome<W>, unknown, ServiceOf<U>>);
  readonly meta: Meta;
}): Rule<Id, R, U, W, S> => {
  const { want, effect } = def;
  return register({
    kind: "rule",
    impl: isStub(want) || isStub(effect) ? "stub" : "real",
    id: def.id,
    reads: def.reads,
    uses: def.uses,
    writes: def.writes ?? [],
    triggers: def.triggers,
    subject: def.subject,
    maxAttempts: def.maxAttempts ?? 5,
    leaseMs: def.leaseMs ?? 15 * 60_000,
    // A stub want wants nothing, so a stub rule is inert instead of crashing every sweep.
    want: isStub(want)
      ? () => Effect.succeed([])
      : Effect.fn(`${def.id}/want`)(function* (db: Reader<R>, ctx: WantCtx) {
          return yield* want(db, ctx);
        }),
    effect: isStub(effect)
      ? () => Effect.die(new NotImplemented({ id: def.id, intent: intentOf(effect) }))
      : Effect.fn(def.id)(function* (subject: S) {
          yield* Effect.annotateCurrentSpan({ "kernel.kind": "rule", "kernel.subject": subject.urn });
          return yield* effect(subject);
        }),
    meta: def.meta,
  });
};
