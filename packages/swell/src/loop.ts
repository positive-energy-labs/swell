import { type AnyFact, Command, Entry, Kernel, latest, type Reader, Rule, violation } from "@swell/kernel";
import { Array as Arr, Duration, Effect, Option, Order, Schema } from "effect";
import {
  dayStart,
  Measurement,
  meta,
  Proposal,
  Sample,
  Signal,
  type Signature,
  signaturesOf,
  Verdict,
} from "./facts.ts";
import { type Brief, type Instrument, Plant, PlantError, PlantPort, Sampled } from "./plant.ts";
import type { ControlSpec, ObserverSpec, SensorSpec } from "./spec.ts";

/** Two sources agree, or a known signature was seen in every run of the window, over at least three runs. */
export const defaultThreshold = (sig: Signature): boolean =>
  sig.sources.length >= 2 || (!sig.signature.startsWith("new:") && sig.runs >= 3 && sig.rate >= 1);

/**
 * A move's subject: the signature, the evidence set and the arming, readable as is in the HMI and in a URN.
 * Sources are kebab ids, so the joined set is unambiguous without a hash. A new source is a new subject, a new
 * arming is a new subject, and the kernel's receipt dedupes the rest.
 */
export const subjectOf = (signature: string, sources: ReadonlyArray<string>, arming: number): string =>
  `${signature}@${Arr.sort(sources, Order.String).join("+")}#${arming}`;

const MeasureSubject = Schema.Struct({
  urn: Schema.String,
  instrument: Schema.String,
  sample: Schema.String,
});
const LoopSubject = Schema.Struct({
  urn: Schema.String,
  subject: Schema.String,
  signature: Schema.String,
  arming: Schema.Int,
  sample: Schema.String,
  sources: Schema.Array(Schema.String),
  signals: Schema.Array(Signal),
  feedback: Schema.String,
  /** The newest failed receipt's error for this subject, or why the last arming's move could not land. */
  previous: Schema.String,
});
const ApplySubject = Schema.Struct({ urn: Schema.String, apply: Schema.String });

const BRIEF_SIGNALS = 50;
/** Proposals per signature a loop reads, newest first: the current arming and the dismissals behind it. */
const PER_SIGNATURE = 64;
/** Accepted verdicts the apply rule reads, newest first: an older one has long since landed or given up. */
const RECENT_ACCEPTED = 256;

/** What the feedback path carries in: a new sample, and decisions made where the plant keeps its operator. */
export const FeedbackPayload = Schema.Struct({
  sample: Schema.optionalKey(Sampled),
  verdicts: Schema.Array(
    Schema.Struct({
      loop: Schema.String,
      subject: Schema.String,
      accept: Schema.Boolean,
      text: Schema.String,
      cite: Schema.String,
    }),
  ),
});
export type FeedbackPayload = typeof FeedbackPayload.Type;

const latestSample = (db: Reader<AnyFact>, plant: string, enabledAt: number) =>
  db
    .find(Sample, "by_plant", { eq: [plant], gte: enabledAt, order: "desc", limit: 1 })
    .pipe(Effect.map((r) => r[0]));

const newest = { order: "desc", limit: 64 } as const;

/**
 * What the kernel will make of a subject, read from its own rows the way `plan` reads them: done, in flight, or
 * how many failures since the last retry grant, with the newest failure's error.
 */
const kernelState = (db: Reader<AnyFact>, rule: string, urn: string, now: number, leaseMs: number) =>
  Effect.gen(function* () {
    const eq = [rule, urn];
    const receipts = yield* db.find(Kernel.Receipt, "by_key", { eq, ...newest });
    const ok = receipts.find((r) => r.outcome === "ok");
    const attempts = yield* db.find(Kernel.Attempt, "by_key", { eq, ...newest });
    const settled = new Set(receipts.map((r) => r.attempt));
    const grant = latest(yield* db.find(Kernel.RetryGranted, "by_key", { eq, ...newest }));
    const since = Option.match(grant, { onNone: () => -1, onSome: (g) => g._creationTime });
    const failed = receipts.filter((r) => r.outcome === "failed" && r._creationTime > since);
    return {
      ok: Option.fromNullishOr(ok),
      inflight: attempts.some((a) => !settled.has(a._id) && a.at > now - leaseMs),
      failures: failed.length,
      previous: latest(failed).pipe(
        Option.map((r) => r.error ?? ""),
        Option.getOrElse(() => ""),
      ),
    };
  });

/** A loop rule gives up after this many failed attempts: an actuator is an agent, and a retry costs real money. */
const LOOP_ATTEMPTS = 2;
/** An apply gives up after this many; then the move can never land as made, and the signature re-arms. */
const APPLY_ATTEMPTS = 5;
const LEASE_SLACK = Duration.minutes(5);

const SENSOR_TIMEOUT = 600_000;
const OBSERVER_TIMEOUT = 1_800_000;
const ACTUATOR_TIMEOUT = 3_600_000;

/** Compile one declaration into kernel primitives: a measure rule, one rule per loop, an apply rule, and the feedback entry. */
export const rulesOf = (spec: ControlSpec) => {
  const { plant } = spec;
  const observers = spec.observers ?? [];
  type Kind =
    | { readonly kind: "measured"; readonly spec: SensorSpec }
    | { readonly kind: "estimated"; readonly spec: ObserverSpec };
  const instruments = new Map<string, Kind>([
    ...spec.sensors.map((s) => [s.id, { kind: "measured", spec: s }] as const),
    ...observers.map((o) => [o.id, { kind: "estimated", spec: o }] as const),
  ]);
  const actuators = new Map(spec.actuators.map((a) => [a.id, a]));
  const timeoutOf = (i: Kind) =>
    i.spec.timeoutMs ?? (i.kind === "estimated" ? OBSERVER_TIMEOUT : SENSOR_TIMEOUT);
  const applyRuleId = `control::${plant.id}-apply`;
  const applyUrn = (loop: string, subject: string) => `${plant.id}/${loop}/${subject}`;

  const measure = Rule.make({
    id: `control::${plant.id}-measure`,
    reads: [Sample, Measurement],
    uses: [PlantPort],
    writes: [Measurement],
    triggers: [Rule.onFact(Sample)],
    subject: MeasureSubject,
    // The slowest instrument is never presumed dead and started a second time.
    lease: Duration.sum(
      Duration.millis(Math.max(SENSOR_TIMEOUT, ...[...instruments.values()].map(timeoutOf))),
      LEASE_SLACK,
    ),
    want: (db, { enabledAt }) =>
      Effect.gen(function* () {
        const head = yield* latestSample(db, plant.id, enabledAt);
        if (head === undefined) return [];
        const out: Array<typeof MeasureSubject.Type> = [];
        for (const [id, i] of instruments) {
          // An observer is budgeted: it estimates again only after enough commits.
          if (i.kind === "estimated" && i.spec.every !== undefined) {
            const last = (yield* db.find(Measurement, "by_instrument", {
              eq: [plant.id, id],
              order: "desc",
              limit: 1,
            }))[0];
            if (last !== undefined) {
              const since = yield* db.find(Sample, "by_plant", { eq: [plant.id], gte: last.at, limit: 2048 });
              const commits = since
                .filter((x) => x.sample !== last.sample)
                .reduce((n, x) => n + x.commits, 0);
              if (commits < i.spec.every.commits) continue;
            }
          }
          out.push({ urn: `${plant.id}/${id}@${head.sample}`, instrument: id, sample: head.sample });
        }
        return out;
      }),
    effect: ({ instrument, sample }) =>
      Effect.gen(function* () {
        const p = yield* Plant;
        const i = instruments.get(instrument)!;
        const run: Instrument = {
          id: i.spec.id,
          run: i.spec.run,
          timeoutMs: timeoutOf(i),
          env: i.spec.env ?? [],
        };
        const measured = yield* p.measure(plant, run, sample);
        const signals =
          measured.error !== undefined
            ? []
            : i.kind === "estimated"
              ? measured.signals.map((s) =>
                  i.spec.vocabulary.includes(s.signature) ? s : { ...s, signature: `new:${s.signature}` },
                )
              : measured.signals;
        return {
          result:
            measured.error === undefined
              ? `${signals.length} signals over ${measured.analyzed}`
              : `failed: ${measured.error}`,
          append: [
            {
              fact: Measurement,
              draft: {
                plant: plant.id,
                instrument,
                sample,
                kind: i.kind,
                signals,
                analyzed: measured.analyzed,
                excluded: measured.excluded,
                failed: measured.failed,
                ...(measured.error === undefined ? {} : { error: measured.error }),
              },
            },
          ],
        };
      }),
    meta: meta(
      `Measure ${plant.id}`,
      "Run every sensor, and every observer that is due, over the newest sample.",
      "an instrument is a pure function of the sample",
    ),
  });

  const loops = spec.loops.map((loop) => {
    const ruleId = `control::${plant.id}-${loop.id}` as const;
    const actuator = actuators.get(loop.actuator)!;
    // A long actuator is never presumed dead and started a second time.
    const lease = Duration.sum(Duration.millis(actuator.timeoutMs ?? ACTUATOR_TIMEOUT), LEASE_SLACK);
    const leaseMs = Duration.toMillis(lease);
    const sees = (signature: string) => (s: Signal) =>
      s.signature === signature || s.signature === `new:${signature}`;

    /** The newest measurement by each input that saw this signature, or saw it after `after`. */
    const newestBy = (db: Reader<AnyFact>, src: string) =>
      db
        .find(Measurement, "by_instrument", { eq: [plant.id, src], order: "desc", limit: 1 })
        .pipe(Effect.map((r) => Option.fromNullishOr(r[0])));

    /**
     * After a move on this signature landed, it re-arms only when an input measured a sample taken after the apply
     * and still sees it: the move did not fix it, or it came back. The weekly tallies lag; this does not.
     */
    const seenSince = (db: Reader<AnyFact>, signature: string, appliedAt: number) =>
      Effect.gen(function* () {
        for (const src of loop.inputs) {
          const m = yield* newestBy(db, src);
          if (Option.isNone(m) || m.value.at <= appliedAt || !m.value.signals.some(sees(signature))) continue;
          const s = (yield* db.find(Sample, "by_key", { eq: [plant.id, m.value.sample], limit: 1 }))[0];
          if (s !== undefined && s.at > appliedAt) return true;
        }
        return false;
      });

    return Rule.make({
      id: ruleId,
      reads: [Measurement, Proposal, Verdict, Sample, Kernel.Attempt, Kernel.Receipt, Kernel.RetryGranted],
      uses: [PlantPort],
      writes: [Proposal, Verdict],
      triggers: [Rule.onFact(Measurement)],
      subject: LoopSubject,
      maxAttempts: LOOP_ATTEMPTS,
      lease,
      want: (db, { now, enabledAt }) =>
        Effect.gen(function* () {
          const threshold = loop.threshold ?? defaultThreshold;
          const sigs = (yield* signaturesOf(db, plant.id, now)).filter(
            (s) => s.sources.some((src) => loop.inputs.includes(src)) && threshold(s),
          );
          if (sigs.length === 0) return [];
          const head = yield* latestSample(db, plant.id, enabledAt);
          if (head === undefined) return [];
          // The limit counts the kernel's attempts since the UTC day began, not proposals: a failed propose or a
          // crashed actuator spent real money.
          const today = yield* db.find(Kernel.Attempt, "by_rule", {
            eq: [ruleId],
            gte: dayStart(now),
            limit: 2048,
          });
          let budget = (loop.limit?.perDay ?? Number.POSITIVE_INFINITY) - today.length;
          const out: Array<typeof LoopSubject.Type> = [];
          for (const sig of sigs) {
            const mine = yield* db.find(Proposal, "by_signature", {
              eq: [plant.id, loop.id, sig.signature],
              order: "desc",
              limit: PER_SIGNATURE,
            });
            const verdicts = new Map<string, typeof Verdict.row.Type>();
            for (const p of mine) {
              const v = (yield* db.find(Verdict, "by_key", {
                eq: [plant.id, loop.id, p.subject],
                limit: 1,
              }))[0];
              if (v !== undefined) verdicts.set(p.subject, v);
            }
            const arming = mine.reduce((n, p) => Math.max(n, p.arming), 0);
            const current = mine.filter((p) => p.arming === arming);
            let next = Math.max(arming, 1);
            let dismissed = current.filter((p) => verdicts.get(p.subject)?.accept === false);
            let carried = "";
            // An open proposal in this arming waits for its operator.
            if (current.some((p) => !verdicts.has(p.subject))) continue;
            const accepted = current.find((p) => verdicts.get(p.subject)?.accept === true);
            if (accepted !== undefined) {
              const applied = yield* kernelState(
                db,
                applyRuleId,
                applyUrn(loop.id, accepted.subject),
                now,
                Number.POSITIVE_INFINITY,
              );
              if (Option.isSome(applied.ok)) {
                // The move landed. The signature re-arms only on evidence measured after it.
                if (!(yield* seenSince(db, sig.signature, applied.ok.value.at))) continue;
              } else if (applied.failures >= APPLY_ATTEMPTS) {
                // The move can never land as made (a conflict, a closed PR): make it again from the plant as it is now.
                carried = `the last move on this signature (${accepted.subject}) could not land: ${applied.previous}`;
              } else continue;
              next = arming + 1;
              dismissed = [];
            }
            // Hysteresis: dismissed in this arming, and the evidence set has not grown past what was dismissed.
            if (dismissed.some((p) => sig.sources.every((s) => p.sources.includes(s)))) continue;
            const subject = subjectOf(sig.signature, sig.sources, next);
            const urn = `${plant.id}/${loop.id}/${subject}`;
            const state = yield* kernelState(db, ruleId, urn, now, leaseMs);
            if (Option.isSome(state.ok)) continue;
            // Only a new attempt spends the day: one in flight or given up is visible to the kernel, never metered.
            if (!state.inflight && state.failures < LOOP_ATTEMPTS) {
              if (budget <= 0) continue;
              budget--;
            }
            const signals: Array<Signal> = [];
            for (const s of sig.sources) {
              const m = yield* newestBy(db, s);
              if (Option.isSome(m)) signals.push(...m.value.signals.filter(sees(sig.signature)));
            }
            const feedback = latest(
              mine.flatMap((p) => {
                const v = verdicts.get(p.subject);
                return v !== undefined && !v.accept ? [v] : [];
              }),
            ).pipe(
              Option.map((v) => v.text ?? ""),
              Option.getOrElse(() => ""),
            );
            out.push({
              urn,
              subject,
              signature: sig.signature,
              arming: next,
              sample: head.sample,
              sources: sig.sources,
              signals: signals.slice(0, BRIEF_SIGNALS),
              feedback,
              previous: state.previous || carried,
            });
          }
          return out;
        }),
      effect: (s) =>
        Effect.gen(function* () {
          const p = yield* Plant;
          const brief: Brief = {
            loop: loop.id,
            signature: s.signature,
            subject: s.subject,
            sample: s.sample,
            sources: s.sources,
            signals: s.signals,
            feedback: s.feedback,
            previous: s.previous,
          };
          const changes = yield* p.act(plant, actuator, brief);
          if (changes === null)
            return yield* new PlantError({ op: `act ${loop.actuator}`, message: "actuator changed nothing" });
          const text = `${loop.id}: ${s.signature}, seen by ${s.sources.join(", ")} at ${s.sample.slice(0, 7)}`;
          const proposed = yield* p.propose(plant, changes, text, loop.mode);
          const base = { plant: plant.id, loop: loop.id, subject: s.subject };
          const proposal = {
            fact: Proposal,
            draft: {
              ...base,
              signature: s.signature,
              arming: s.arming,
              operator: loop.operator,
              apply: proposed.apply,
              text,
              cites: [proposed.cite, ...s.signals.flatMap((x) => (x.cite === undefined ? [] : [x.cite]))],
              sources: s.sources,
            },
          };
          const auto = {
            fact: Verdict,
            draft: { ...base, accept: true, text: `auto: ${loop.id} is in auto mode`, cite: proposed.cite },
          };
          return { result: proposed.cite, append: loop.mode === "auto" ? [proposal, auto] : [proposal] };
        }),
      meta: meta(
        `Loop ${plant.id}/${loop.id}`,
        "When the evidence for one signature crosses the threshold, run the actuator once and propose the move it made.",
        "one move per signature per arming; evidence in, proposal out",
      ),
    });
  });

  const apply = Rule.make({
    id: applyRuleId,
    reads: [Verdict, Proposal],
    uses: [PlantPort],
    triggers: [Rule.onFact(Verdict)],
    subject: ApplySubject,
    maxAttempts: APPLY_ATTEMPTS,
    want: (db, { enabledAt }) =>
      Effect.gen(function* () {
        const accepted = yield* db.find(Verdict, "by_accept", {
          eq: [plant.id, true],
          gte: enabledAt,
          order: "desc",
          limit: RECENT_ACCEPTED,
        });
        const out: Array<typeof ApplySubject.Type> = [];
        for (const v of accepted) {
          const p = (yield* db.find(Proposal, "by_key", { eq: [plant.id, v.loop, v.subject], limit: 1 }))[0];
          if (p !== undefined) out.push({ urn: applyUrn(v.loop, v.subject), apply: p.apply });
        }
        return out;
      }),
    effect: ({ apply }) =>
      Effect.gen(function* () {
        const p = yield* Plant;
        return { result: yield* p.apply(plant, apply) };
      }),
    meta: meta(
      `Apply ${plant.id}`,
      "Make an accepted move real; the receipt is the history entry.",
      "nothing auto-reverts; the plant keeps restore",
    ),
  });

  const feedback = Entry.make({
    id: `control::${plant.id}-feedback`,
    source: plant.kind,
    from: plant.root,
    reads: [Sample, Verdict],
    writes: [Sample, Verdict],
    handle: (payload, { db }) =>
      Effect.gen(function* () {
        const { sample, verdicts } = yield* Schema.decodeUnknownEffect(FeedbackPayload)(payload).pipe(
          Effect.mapError((e) => violation("feedback", e.message)),
        );
        if (sample !== undefined) {
          const seen = yield* db.find(Sample, "by_key", { eq: [plant.id, sample.sample], limit: 1 });
          if (seen.length === 0) {
            yield* db.append(Sample, {
              plant: plant.id,
              sample: sample.sample,
              commits: sample.commits,
              churn: sample.churn,
              ...(sample.parent === undefined ? {} : { parent: sample.parent }),
            });
          }
        }
        for (const v of verdicts) {
          const seen = yield* db.find(Verdict, "by_key", { eq: [plant.id, v.loop, v.subject], limit: 1 });
          if (seen.length > 0) continue;
          yield* db.append(Verdict, {
            plant: plant.id,
            loop: v.loop,
            subject: v.subject,
            accept: v.accept,
            text: v.text,
            cite: v.cite,
          });
        }
        return null;
      }),
    meta: meta(
      `Feedback ${plant.id}`,
      "The feedback path: a new sample, and decisions made where the plant keeps its operator.",
      "the controller samples; rules decide",
    ),
  });

  /** Who each rule acts for: a loop for its operator, the rest for the controller itself. */
  const enablers = new Map<string, string | undefined>([
    [measure.id, undefined],
    [apply.id, undefined],
    ...spec.loops.map((l, i) => [loops[i]!.id, `operator:${l.operator}`] as const),
  ]);

  return { measure, loops, apply, feedback, rules: [measure, ...loops, apply], enablers };
};

/** The operator's yes or no from the HMI. The same row a decision read from a PR writes. */
export const Decide = Command.make({
  id: "control::decide",
  role: "operator",
  args: {
    plant: Schema.String,
    loop: Schema.String,
    subject: Schema.String,
    accept: Schema.Boolean,
    text: Schema.String,
  },
  returns: Schema.String,
  reads: [Proposal, Verdict],
  writes: [Verdict],
  run: ({ plant, loop, subject, accept, text }, { db, actor }) =>
    Effect.gen(function* () {
      const p = (yield* db.find(Proposal, "by_key", { eq: [plant, loop, subject], limit: 1 }))[0];
      if (p === undefined) return yield* violation("proposal", `no proposal ${plant}/${loop}/${subject}`);
      if (p.operator !== actor.person)
        return yield* violation("operator", `only ${p.operator} decides this proposal`);
      if (!accept && text.trim() === "")
        return yield* violation("text", "a rejection says why, so the next attempt reads it");
      return yield* db.append(Verdict, { plant, loop, subject, accept, text });
    }),
  meta: meta("Decide", "Say yes or no to a proposed move.", "only its operator decides"),
});

/** An operator lifts a dead letter: the kernel tries the subject again, its failures counted from now. */
export const Retry = Command.make({
  id: "control::retry",
  role: "operator",
  args: { rule: Schema.String, subject: Schema.String },
  returns: Schema.String,
  writes: [Kernel.RetryGranted],
  run: ({ rule, subject }, { db }) =>
    rule.startsWith("control::")
      ? db.append(Kernel.RetryGranted, { rule, subject })
      : Effect.fail(violation("rule", `${rule} is not a control rule`)),
  meta: meta("Retry", "Try a given-up move or apply again.", "a person lifts a dead letter"),
});
