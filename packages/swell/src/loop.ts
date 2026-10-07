import { type AnyFact, Command, Entry, type Reader, Rule, violation } from "@tc/kernel";
import { Effect, Schema } from "effect";
import {
  Measurement,
  meta,
  Proposal,
  Sample,
  Signal,
  type Signature,
  signaturesOf,
  Verdict,
} from "./facts.ts";
import {
  type ActuatorSpec,
  type Brief,
  type Instrument,
  type ObserverSpec,
  Plant,
  PlantError,
  PlantPort,
  type PlantSpec,
  type Sampled,
  type SensorSpec,
} from "./plant.ts";

export interface LoopSpec {
  readonly id: string;
  /** Sensor and observer ids whose signatures this loop acts on. */
  readonly inputs: ReadonlyArray<string>;
  /** Default: two sources agree, or one sensor sees it in every run of the window. */
  readonly threshold?: (signature: Signature) => boolean;
  /** The actuator's rate limit: moves proposed per day. */
  readonly limit?: { readonly perDay: number };
  readonly actuator: string;
  /** `manual`: the operator approves each move. `auto`: the move applies, for pre-approved classes. */
  readonly mode: "manual" | "auto";
  /** Who approves, and who the loop acts for. */
  readonly operator: string;
}

/** One plant's instruments, actuators and loops: everything a controller needs to close its loops. */
export interface ControlSpec {
  readonly plant: PlantSpec;
  readonly sensors: ReadonlyArray<SensorSpec>;
  readonly observers?: ReadonlyArray<ObserverSpec>;
  readonly actuators: ReadonlyArray<ActuatorSpec>;
  readonly loops: ReadonlyArray<LoopSpec>;
}

const KEBAB = /^[a-z][a-z0-9-]*$/;

/** One `swell.config.ts` at a plant's root. Validation throws at load, so a bad declaration never ticks. */
export const defineControl = (spec: ControlSpec): ControlSpec => {
  const observers = spec.observers ?? [];
  const instruments = [...spec.sensors, ...observers];
  const ids = [
    spec.plant.id,
    ...instruments.map((s) => s.id),
    ...spec.actuators.map((a) => a.id),
    ...spec.loops.map((l) => l.id),
  ];
  for (const id of ids) if (!KEBAB.test(id)) throw new Error(`swell: id '${id}' is not kebab-case`);
  for (const list of [instruments, spec.actuators, spec.loops] as ReadonlyArray<
    ReadonlyArray<{ id: string }>
  >) {
    const seen = new Set<string>();
    for (const { id } of list) {
      if (seen.has(id)) throw new Error(`swell: id '${id}' is declared twice`);
      seen.add(id);
    }
  }
  for (const x of [...instruments, ...spec.actuators])
    if (x.run.length === 0) throw new Error(`swell: '${x.id}' has an empty run`);
  for (const o of observers)
    if (o.vocabulary.length === 0) throw new Error(`swell: observer '${o.id}' has an empty vocabulary`);
  const known = new Set(instruments.map((s) => s.id));
  const actuators = new Set(spec.actuators.map((a) => a.id));
  for (const loop of spec.loops) {
    for (const s of loop.inputs)
      if (!known.has(s)) throw new Error(`swell: loop '${loop.id}' reads unknown instrument '${s}'`);
    if (!actuators.has(loop.actuator))
      throw new Error(`swell: loop '${loop.id}' moves with unknown actuator '${loop.actuator}'`);
  }
  return spec;
};

export const defaultThreshold = (sig: Signature): boolean =>
  sig.sources.length >= 2 || (!sig.signature.startsWith("new:") && sig.rate >= 1);

/** djb2 over the sorted evidence set; a new source is a new subject, and the kernel's receipt dedupes the rest. */
export const evidenceHash = (sources: ReadonlyArray<string>): string => {
  let h = 5381;
  for (const c of [...sources].sort().join(",")) h = ((h << 5) + h + c.charCodeAt(0)) >>> 0;
  return h.toString(16);
};

const MeasureSubject = Schema.Struct({
  urn: Schema.String,
  instrument: Schema.String,
  sample: Schema.String,
});
const LoopSubject = Schema.Struct({
  urn: Schema.String,
  subject: Schema.String,
  signature: Schema.String,
  sample: Schema.String,
  sources: Schema.Array(Schema.String),
  signals: Schema.Array(Signal),
  feedback: Schema.String,
});
const ApplySubject = Schema.Struct({ urn: Schema.String, apply: Schema.String });

const WINDOW_MS = 35 * 86_400_000;
const DAY_MS = 86_400_000;
const BRIEF_SIGNALS = 50;

/** What the feedback path carries in: a new sample, and decisions made where the plant keeps its operator. */
export interface FeedbackPayload {
  readonly sample?: Sampled;
  readonly verdicts: ReadonlyArray<{
    readonly loop: string;
    readonly subject: string;
    readonly accept: boolean;
    readonly text: string;
    readonly cite: string;
  }>;
}

const latestSample = (db: Reader<AnyFact>, plant: string, enabledAt: number) =>
  db
    .find(Sample, "by_plant", { eq: [plant], gte: enabledAt, order: "desc", limit: 1 })
    .pipe(Effect.map((r) => r[0]));

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

  const measure = Rule.make({
    id: `control::${plant.id}-measure`,
    reads: [Sample, Measurement],
    uses: [PlantPort],
    writes: [Measurement],
    triggers: [Rule.onFact(Sample)],
    subject: MeasureSubject,
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
          timeoutMs: i.spec.timeoutMs ?? (i.kind === "estimated" ? 1_800_000 : 600_000),
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

  const loops = spec.loops.map((loop) =>
    Rule.make({
      id: `control::${plant.id}-${loop.id}`,
      reads: [Measurement, Proposal, Verdict, Sample],
      uses: [PlantPort],
      writes: [Proposal, Verdict],
      triggers: [Rule.onFact(Measurement)],
      subject: LoopSubject,
      want: (db, { now, enabledAt }) =>
        Effect.gen(function* () {
          const threshold = loop.threshold ?? defaultThreshold;
          const sigs = (yield* signaturesOf(db, plant.id, now)).filter(
            (s) => s.sources.some((src) => loop.inputs.includes(src)) && threshold(s),
          );
          if (sigs.length === 0) return [];
          const head = yield* latestSample(db, plant.id, enabledAt);
          if (head === undefined) return [];
          const proposals = yield* db.find(Proposal, "by_loop", {
            eq: [plant.id, loop.id],
            gte: now - WINDOW_MS,
            limit: 2048,
          });
          const verdicts = yield* db.find(Verdict, "by_plant", {
            eq: [plant.id],
            gte: now - WINDOW_MS,
            limit: 2048,
          });
          const verdictOf = new Map(verdicts.filter((v) => v.loop === loop.id).map((v) => [v.subject, v]));
          const dayStart = now - (now % DAY_MS);
          let spent = proposals.filter((p) => p.at >= dayStart).length;
          const limit = loop.limit?.perDay ?? Number.POSITIVE_INFINITY;
          const out: Array<typeof LoopSubject.Type> = [];
          for (const sig of sigs) {
            const mine = proposals.filter((p) => p.signature === sig.signature);
            // Covered: a move on this signature is open, or accepted and not yet applied.
            if (mine.some((p) => verdictOf.get(p.subject)?.accept !== false)) continue;
            // Hysteresis: dismissed, and the evidence set has not grown past what was dismissed.
            const dismissed = mine.filter((p) => verdictOf.get(p.subject)?.accept === false);
            if (dismissed.some((p) => sig.sources.every((s) => p.sources.includes(s)))) continue;
            if (spent >= limit) break;
            spent++;
            const signals: Array<Signal> = [];
            for (const s of sig.sources) {
              const last = (yield* db.find(Measurement, "by_instrument", {
                eq: [plant.id, s],
                order: "desc",
                limit: 1,
              }))[0];
              for (const x of last?.signals ?? []) if (x.signature === sig.signature) signals.push(x);
            }
            const feedback = dismissed
              .map((p) => verdictOf.get(p.subject))
              .sort((a, b) => (b?.at ?? 0) - (a?.at ?? 0))[0]?.text;
            const subject = `${sig.signature}@${evidenceHash(sig.sources)}`;
            out.push({
              urn: `${plant.id}/${loop.id}/${subject}`,
              subject,
              signature: sig.signature,
              sample: head.sample,
              sources: sig.sources,
              signals: signals.slice(0, BRIEF_SIGNALS),
              feedback: feedback ?? "",
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
            sample: s.sample,
            sources: s.sources,
            signals: s.signals,
            feedback: s.feedback,
          };
          const changes = yield* p.act(plant, actuators.get(loop.actuator)!, brief);
          if (changes === null)
            return yield* Effect.fail(
              new PlantError({ op: `act ${loop.actuator}`, message: "actuator changed nothing" }),
            );
          const text = `${loop.id}: ${s.signature}, seen by ${s.sources.join(", ")} at ${s.sample.slice(0, 7)}`;
          const proposed = yield* p.propose(plant, changes, text, loop.mode);
          const base = { plant: plant.id, loop: loop.id, subject: s.subject };
          const proposal = {
            fact: Proposal,
            draft: {
              ...base,
              signature: s.signature,
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
        "one move per signature; evidence in, proposal out",
      ),
    }),
  );

  const apply = Rule.make({
    id: `control::${plant.id}-apply`,
    reads: [Verdict, Proposal],
    uses: [PlantPort],
    triggers: [Rule.onFact(Verdict)],
    subject: ApplySubject,
    want: (db, { enabledAt }) =>
      Effect.gen(function* () {
        const verdicts = yield* db.find(Verdict, "by_plant", { eq: [plant.id], gte: enabledAt, limit: 2048 });
        const out: Array<typeof ApplySubject.Type> = [];
        for (const v of verdicts) {
          if (!v.accept) continue;
          const p = (yield* db.find(Proposal, "by_key", { eq: [plant.id, v.loop, v.subject], limit: 1 }))[0];
          if (p !== undefined) out.push({ urn: `${plant.id}/${v.loop}/${v.subject}`, apply: p.apply });
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
        const { sample, verdicts } = payload as FeedbackPayload;
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

  return { measure, loops, apply, feedback, rules: [measure, ...loops, apply] };
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
