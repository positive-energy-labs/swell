import { Cause, Effect, Exit, Layer, Option, Schema } from "effect";
import type { Db, Reader } from "./db.ts";
import type { InvariantViolation } from "./errors.ts";
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

/** The newest rows first: a long-lived subject's latest attempt and its ok receipt are never past the limit. */
const newest = { order: "desc", limit: PER_SUBJECT } as const;

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
  if (Option.isNone(enabled)) return empty;
  const wants = yield* rule.want(db, { now, enabledAt: enabled.value.at });
  let done = 0;
  let inflight = 0;
  const pending: Array<Plan["pending"][number]> = [];
  const dead: Array<Plan["dead"][number]> = [];
  for (const subject of wants) {
    const eq = [rule.id, subject.urn];
    const receipts = yield* db.find(Receipt, "by_key", { eq, ...newest });
    if (receipts.some((r) => r.outcome === "ok")) {
      done++;
      continue;
    }
    const attempts = yield* db.find(Attempt, "by_key", { eq, ...newest });
    const settled = new Set(receipts.map((r) => r.attempt));
    if (attempts.some((a) => !settled.has(a._id) && a.at > now - rule.leaseMs)) {
      inflight++;
      continue;
    }
    // Creation order, not `at`: a grant and a failure in the same millisecond are still ordered.
    const grant = latest(yield* db.find(RetryGranted, "by_key", { eq, ...newest }));
    const since = Option.match(grant, { onNone: () => -1, onSome: (g) => g._creationTime });
    const failed = receipts.filter((r) => r.outcome === "failed" && r._creationTime > since);
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
export const Job = Schema.Struct({
  rule: Schema.String,
  attempt: Schema.String,
  urn: Schema.String,
  /** The subject as JSON, decoded by the rule's own subject schema on the far side. */
  subject: Schema.String,
  traceparent: Schema.UndefinedOr(Schema.String),
});
export type Job = typeof Job.Type;

/** The enabler is who a rule acts for: its writes carry `via`, the person (or controller) that switched it on. */
export const enablerOf = (rule: string, db: Reader<AnyFact>) =>
  db.find(RuleEnabled, "by_key", { eq: [rule], limit: 16 }).pipe(
    Effect.map((rows) =>
      latest(rows).pipe(
        Option.map((r) => r.via ?? r.by),
        Option.getOrUndefined,
      ),
    ),
  );

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
    yield* schedule({
      rule: rule.id,
      attempt,
      urn: item.urn,
      subject,
      traceparent: Option.getOrUndefined(tp),
    });
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

export class UndeclaredWrite extends Schema.TaggedError<UndeclaredWrite>()("UndeclaredWrite", {
  rule: Schema.String,
  fact: Schema.String,
}) {}

type AnyCodec = Schema.Codec<unknown, unknown>;

/**
 * Run the rule's effect and everything that can fail after it (the declared-writes check, encoding each
 * draft) inside one Exit, so every failure becomes a `failed` receipt and counts toward `maxAttempts`. Only
 * an interrupt is `killed`.
 */
export const execute = Effect.fn("kernel/execute")(function* (
  rule: AnyRule,
  job: Job,
  ports: (port: AnyPort) => Layer.Layer<any>,
) {
  const layer = rule.uses.reduce(
    (acc: Layer.Layer<any>, p: AnyPort) => Layer.merge(acc, ports(p)),
    Layer.empty,
  );
  const writes = new Set(rule.writes.map((w: AnyFact) => w.id));
  const settled = Effect.gen(function* () {
    const subject = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(rule.subject as AnyCodec))(
      job.subject,
    );
    const o = yield* Effect.provide(rule.effect(subject), layer) as Effect.Effect<Outcome<AnyFact>, unknown>;
    const append = yield* Effect.forEach(
      o.append ?? [],
      (a): Effect.Effect<{ fact: string; draft: unknown }, UndeclaredWrite | Schema.SchemaError> =>
        writes.has(a.fact.id)
          ? Schema.encodeUnknownEffect(a.fact.draft as unknown as AnyCodec)(a.draft).pipe(
              Effect.map((draft) => ({ fact: a.fact.id, draft })),
            )
          : Effect.fail(new UndeclaredWrite({ rule: rule.id, fact: a.fact.id })),
    );
    return { outcome: "ok", result: o.result, append } satisfies Settled;
  });
  const exit = yield* settled.pipe(Trace.continueFrom(job.traceparent), Effect.exit);
  return Exit.match(exit, {
    onSuccess: (s): Settled => s,
    onFailure: (cause): Settled =>
      Cause.hasInterruptsOnly(cause)
        ? { outcome: "killed" }
        : { outcome: "failed", error: Cause.pretty(cause).slice(0, 2000) },
  });
});

/**
 * Write the settled outcome: the output facts and the receipt in the caller's one transaction. A write the
 * store refuses (a unique key already taken) fails here with the violation, and the caller settles the
 * attempt as failed in a fresh transaction instead.
 */
export const complete = Effect.fn("kernel/complete")(function* (
  rule: AnyRule,
  job: Job,
  settled: Exclude<Settled, { outcome: "killed" }>,
  db: Db<AnyFact, AnyFact>,
): Effect.fn.Return<Id<"kernel::receipt">, InvariantViolation> {
  const attempt = job.attempt as Id<"kernel::attempt">;
  const base = { rule: rule.id, subject: job.urn, attempt };
  if (settled.outcome === "failed") {
    return yield* db.append(Receipt, { ...base, outcome: "failed", error: settled.error });
  }
  for (const a of settled.append) {
    const fact = rule.writes.find((w: AnyFact) => w.id === a.fact)!;
    const draft = yield* Schema.decodeUnknownEffect(fact.draft as unknown as AnyCodec)(a.draft).pipe(
      Effect.orDie,
    );
    yield* db.append(fact, draft);
  }
  return yield* db.append(Receipt, { ...base, outcome: "ok", result: settled.result });
});
