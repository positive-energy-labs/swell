import { Cause, Effect, Exit, Layer, Option, Schema } from "effect";
import type { Db, Reader } from "./db.ts";
import type { AnyFact, Id } from "./fact.ts";
import { Attempt, Receipt, RetryGranted, RuleEnabled } from "./facts.ts";
import type { AnyPort } from "./port.ts";
import type { AnyRule, Outcome } from "./rule.ts";
import { latest } from "./store.ts";
import * as Trace from "./trace.ts";

export interface Plan {
  readonly rule: string;
  readonly enabled: boolean;
  readonly wanted: number;
  readonly done: number;
  readonly inflight: number;
  readonly pending: ReadonlyArray<{
    readonly subject: unknown;
    readonly urn: string;
    readonly failures: number;
  }>;
  /** Subjects that failed `maxAttempts` times: the dead-letter queue, waiting for a retry grant. */
  readonly dead: ReadonlyArray<{ readonly urn: string; readonly failures: number; readonly error: string }>;
}

const PER_SUBJECT = 64;

export const plan = Effect.fn("kernel/plan")(function* (rule: AnyRule, db: Reader<AnyFact>, now: number) {
  const enabled = latest(yield* db.find(RuleEnabled, "by_key", { eq: [rule.id], limit: 16 }));
  const empty: Plan = {
    rule: rule.id,
    enabled: false,
    wanted: 0,
    done: 0,
    inflight: 0,
    pending: [],
    dead: [],
  };
  if (Option.isNone(enabled)) return { ...empty, enabled: Option.isSome(enabled) };
  const wants = yield* rule.want(db, { now, enabledAt: enabled.value.at });
  let done = 0;
  let inflight = 0;
  const pending: Array<Plan["pending"][number]> = [];
  const dead: Array<Plan["dead"][number]> = [];
  for (const subject of wants) {
    const eq = [rule.id, subject.urn];
    const receipts = yield* db.find(Receipt, "by_key", { eq, limit: PER_SUBJECT });
    if (receipts.some((r) => r.outcome === "ok")) {
      done++;
      continue;
    }
    const attempts = yield* db.find(Attempt, "by_key", { eq, limit: PER_SUBJECT });
    const settled = new Set(receipts.map((r) => r.attempt));
    if (attempts.some((a) => !settled.has(a._id) && a.at > now - rule.leaseMs)) {
      inflight++;
      continue;
    }
    const grant = latest(yield* db.find(RetryGranted, "by_key", { eq, limit: PER_SUBJECT }));
    const since = Option.match(grant, { onNone: () => 0, onSome: (g) => g.at });
    const failed = receipts.filter((r) => r.outcome === "failed" && r.at >= since);
    if (failed.length >= rule.maxAttempts) {
      dead.push({
        urn: subject.urn,
        failures: failed.length,
        error: latest(failed).pipe(
          Option.map((r) => r.error ?? ""),
          Option.getOrElse(() => ""),
        ),
      });
      continue;
    }
    pending.push({ subject, urn: subject.urn, failures: failed.length });
  }
  return { rule: rule.id, enabled: true, wanted: wants.length, done, inflight, pending, dead } satisfies Plan;
});

/** What crosses the scheduler hop into the action. Everything is encoded; nothing is ambient. */
export interface Job {
  readonly rule: string;
  readonly attempt: string;
  readonly subject: string;
  readonly traceparent: string | undefined;
}

/** The sweep, inside one mutation: the attempt and the scheduled effect commit together or not at all. */
export const sweep = Effect.fn("kernel/sweep")(function* (
  rule: AnyRule,
  db: Db<AnyFact, AnyFact>,
  now: number,
  schedule: (job: Job) => Effect.Effect<void>,
) {
  const p = yield* plan(rule, db, now);
  const tp = yield* Trace.current;
  for (const item of p.pending) {
    const attempt = yield* db.append(Attempt, { rule: rule.id, subject: item.urn }).pipe(Effect.orDie);
    const subject = JSON.stringify(
      yield* Schema.encodeUnknownEffect(rule.subject)(item.subject).pipe(Effect.orDie),
    );
    yield* schedule({ rule: rule.id, attempt, subject, traceparent: Option.getOrUndefined(tp) });
  }
  return p;
});

export type Settled =
  | {
      readonly outcome: "ok";
      readonly result: string;
      readonly append: ReadonlyArray<{ readonly fact: string; readonly draft: unknown }>;
    }
  | { readonly outcome: "failed"; readonly error: string }
  /** The worker died. Write nothing; the lease expires and the next sweep retries. */
  | { readonly outcome: "killed" };

export const execute = Effect.fn("kernel/execute")(function* (
  rule: AnyRule,
  job: Job,
  ports: (port: AnyPort) => Layer.Layer<any>,
) {
  const subject = yield* Schema.decodeUnknownEffect(rule.subject)(JSON.parse(job.subject)).pipe(Effect.orDie);
  const layer = rule.uses.reduce(
    (acc: Layer.Layer<any>, p: AnyPort) => Layer.merge(acc, ports(p)),
    Layer.empty,
  );
  const provided = Effect.provide(rule.effect(subject), layer) as Effect.Effect<Outcome<AnyFact>, unknown>;
  const exit = yield* provided.pipe(Trace.continueFrom(job.traceparent), Effect.exit);
  return Exit.match(exit, {
    onSuccess: (o: Outcome<AnyFact>): Settled => ({
      outcome: "ok",
      result: o.result,
      append: (o.append ?? []).map((a) => ({
        fact: a.fact.id,
        draft: Schema.encodeUnknownSync(a.fact.draft as unknown as Schema.Codec<unknown, unknown>)(a.draft),
      })),
    }),
    onFailure: (cause): Settled =>
      Cause.hasInterruptsOnly(cause)
        ? { outcome: "killed" }
        : { outcome: "failed", error: Cause.pretty(cause).slice(0, 2000) },
  });
});

export const complete = Effect.fn("kernel/complete")(function* (
  rule: AnyRule,
  job: Job,
  settled: Exclude<Settled, { outcome: "killed" }>,
  db: Db<AnyFact, AnyFact>,
) {
  const attempt = job.attempt as Id<"kernel::attempt">;
  if (settled.outcome === "failed") {
    return yield* db
      .append(Receipt, {
        rule: rule.id,
        subject: urnOf(job),
        attempt,
        outcome: "failed",
        error: settled.error,
      })
      .pipe(Effect.orDie);
  }
  for (const a of settled.append) {
    const fact = rule.writes.find((w: AnyFact) => w.id === a.fact);
    if (fact === undefined)
      return yield* Effect.die(
        new Error(`${rule.id} appended ${a.fact}, which it does not declare in writes`),
      );
    const draft = yield* Schema.decodeUnknownEffect(fact.draft as unknown as Schema.Codec<unknown, unknown>)(
      a.draft,
    ).pipe(Effect.orDie);
    yield* db.append(fact, draft).pipe(Effect.orDie);
  }
  return yield* db
    .append(Receipt, { rule: rule.id, subject: urnOf(job), attempt, outcome: "ok", result: settled.result })
    .pipe(Effect.orDie);
});

const urnOf = (job: Job): string => (JSON.parse(job.subject) as { urn: string }).urn;
