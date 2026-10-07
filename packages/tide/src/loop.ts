import { type AnyFact, type AnyPort, Command, Entry, Port, type Reader, Rule, violation } from "@tc/kernel";
import { Effect, Layer, Schema } from "effect";
import { Finding, type Issue, issuesOf, meta, Proposal, Reading, Snapshot, Verdict } from "./facts.ts";
import {
  type ActuatorSpec,
  type Brief,
  type Head,
  Plant,
  PlantPort,
  type PlantSpec,
  type SensorSpec,
} from "./plant.ts";

/** Effect's own Clock stands behind this port, so a cron trigger has a port to name. */
export const ClockPort = Port.make({
  id: "tide::clock",
  service: undefined,
  live: Layer.empty,
  fake: Layer.empty,
  impl: "real",
  meta: meta("Clock", "Time, for cron triggers.", "Effect's Clock; TestClock in tests"),
});

export interface LoopSpec {
  readonly id: string;
  /** Sensor ids whose issues this loop acts on. */
  readonly sense: ReadonlyArray<string>;
  /** The bar. Default: two sources agree, or one measured source sees it in every run of the window. */
  readonly when?: (issue: Issue) => boolean;
  readonly budget?: { readonly perDay: number };
  readonly act: string;
  /** `pr`: a person decides where the plant keeps its gate. `auto`: a policy verdict, for pre-approved classes. */
  readonly gate: "pr" | "auto";
  /** Who decides, and who the loop acts for. */
  readonly person: string;
  readonly cron?: string;
}

export interface TideSpec {
  readonly plant: PlantSpec;
  readonly sensors: ReadonlyArray<SensorSpec>;
  readonly actuators: ReadonlyArray<ActuatorSpec>;
  readonly loops: ReadonlyArray<LoopSpec>;
}

const KEBAB = /^[a-z][a-z0-9-]*$/;

/** One `tide.config.ts` at a plant's root. Validation throws at load, so a bad declaration never ticks. */
export const defineTide = (spec: TideSpec): TideSpec => {
  const ids = [
    spec.plant.id,
    ...spec.sensors.map((s) => s.id),
    ...spec.actuators.map((a) => a.id),
    ...spec.loops.map((l) => l.id),
  ];
  for (const id of ids) if (!KEBAB.test(id)) throw new Error(`tide: id '${id}' is not kebab-case`);
  const sensors = new Set(spec.sensors.map((s) => s.id));
  const actuators = new Set(spec.actuators.map((a) => a.id));
  for (const loop of spec.loops) {
    for (const s of loop.sense)
      if (!sensors.has(s)) throw new Error(`tide: loop '${loop.id}' senses unknown sensor '${s}'`);
    if (!actuators.has(loop.act))
      throw new Error(`tide: loop '${loop.id}' acts with unknown actuator '${loop.act}'`);
  }
  return spec;
};

export const defaultWhen = (issue: Issue): boolean =>
  issue.sources.length >= 2 || (!issue.fingerprint.startsWith("new:") && issue.rate >= 1);

/** djb2 over the sorted evidence set; a new source is a new subject, and the kernel's receipt dedupes the rest. */
export const evidenceHash = (sources: ReadonlyArray<string>): string => {
  let h = 5381;
  for (const c of [...sources].sort().join(",")) h = ((h << 5) + h + c.charCodeAt(0)) >>> 0;
  return h.toString(16);
};

const SenseSubject = Schema.Struct({ urn: Schema.String, sensor: Schema.String, snapshot: Schema.String });
const LoopSubject = Schema.Struct({
  urn: Schema.String,
  subject: Schema.String,
  fingerprint: Schema.String,
  snapshot: Schema.String,
  sources: Schema.Array(Schema.String),
  findings: Schema.Array(Finding),
  feedback: Schema.String,
});
const ApplySubject = Schema.Struct({ urn: Schema.String, apply: Schema.String });

const WINDOW_MS = 35 * 86_400_000;
const DAY_MS = 86_400_000;
const BRIEF_FINDINGS = 50;

export interface ObservePayload {
  readonly head?: Head;
  readonly verdicts: ReadonlyArray<{
    readonly loop: string;
    readonly subject: string;
    readonly accept: boolean;
    readonly text: string;
    readonly cite: string;
  }>;
}

const latestSnapshot = (db: Reader<AnyFact>, plant: string, enabledAt: number) =>
  db
    .find(Snapshot, "by_plant", { eq: [plant], gte: enabledAt, order: "desc", limit: 1 })
    .pipe(Effect.map((r) => r[0]));

/** Compile one declaration into kernel primitives: a sense rule, one rule per loop, an apply rule, and the host's observe door. */
export const rulesOf = (tide: TideSpec, clock: AnyPort = ClockPort) => {
  const { plant } = tide;
  const sensors = new Map(tide.sensors.map((s) => [s.id, s]));
  const actuators = new Map(tide.actuators.map((a) => [a.id, a]));

  const sense = Rule.make({
    id: `tide::${plant.id}-sense`,
    reads: [Snapshot, Reading],
    uses: [PlantPort],
    writes: [Reading],
    triggers: [Rule.onFact(Snapshot)],
    subject: SenseSubject,
    want: (db, { enabledAt }) =>
      Effect.gen(function* () {
        const head = yield* latestSnapshot(db, plant.id, enabledAt);
        if (head === undefined) return [];
        const out: Array<typeof SenseSubject.Type> = [];
        for (const s of tide.sensors) {
          if (s.every !== undefined) {
            const last = (yield* db.find(Reading, "by_sensor", {
              eq: [plant.id, s.id],
              order: "desc",
              limit: 1,
            }))[0];
            if (last !== undefined) {
              const since = yield* db.find(Snapshot, "by_plant", {
                eq: [plant.id],
                gte: last.at,
                limit: 2048,
              });
              const commits = since
                .filter((x) => x.snapshot !== last.snapshot)
                .reduce((n, x) => n + x.commits, 0);
              if (commits < s.every.commits) continue;
            }
          }
          out.push({ urn: `${plant.id}/${s.id}@${head.snapshot}`, sensor: s.id, snapshot: head.snapshot });
        }
        return out;
      }),
    effect: ({ sensor, snapshot }) =>
      Effect.gen(function* () {
        const p = yield* Plant;
        const spec = sensors.get(sensor)!;
        const sensed = yield* p.sense(plant, spec, snapshot);
        const vocabulary = spec.vocabulary;
        const findings =
          sensed.error !== undefined
            ? []
            : spec.kind === "model" && vocabulary !== undefined
              ? sensed.findings.map((f) =>
                  vocabulary.includes(f.fingerprint) ? f : { ...f, fingerprint: `new:${f.fingerprint}` },
                )
              : sensed.findings;
        return {
          result:
            sensed.error === undefined
              ? `${findings.length} findings over ${sensed.analyzed}`
              : `failed: ${sensed.error}`,
          append: [
            {
              fact: Reading,
              draft: {
                plant: plant.id,
                sensor,
                snapshot,
                kind: spec.kind,
                findings,
                analyzed: sensed.analyzed,
                excluded: sensed.excluded,
                failed: sensed.failed,
                ...(sensed.error === undefined ? {} : { error: sensed.error }),
              },
            },
          ],
        };
      }),
    meta: meta(
      `Sense ${plant.id}`,
      "Run every sensor over the newest snapshot.",
      "a sensor is a pure function of the snapshot",
    ),
  });

  const loops = tide.loops.map((loop) =>
    Rule.make({
      id: `tide::${plant.id}-${loop.id}`,
      reads: [Reading, Proposal, Verdict, Snapshot],
      uses: [PlantPort],
      writes: [Proposal, Verdict],
      triggers: [Rule.onFact(Reading), Rule.onCron(loop.cron ?? "0 * * * *", clock)],
      subject: LoopSubject,
      want: (db, { now, enabledAt }) =>
        Effect.gen(function* () {
          const when = loop.when ?? defaultWhen;
          const issues = (yield* issuesOf(db, plant.id, now)).filter(
            (i) => i.sources.some((s) => loop.sense.includes(s)) && when(i),
          );
          if (issues.length === 0) return [];
          const head = yield* latestSnapshot(db, plant.id, enabledAt);
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
          const budget = loop.budget?.perDay ?? Number.POSITIVE_INFINITY;
          const out: Array<typeof LoopSubject.Type> = [];
          for (const issue of issues) {
            const mine = proposals.filter((p) => p.fingerprint === issue.fingerprint);
            // Covered: a wave of this fingerprint is open, or accepted and not yet applied.
            if (mine.some((p) => verdictOf.get(p.subject)?.accept !== false)) continue;
            // Dismissed, and the evidence set has not grown past what was dismissed.
            const dismissed = mine.filter((p) => verdictOf.get(p.subject)?.accept === false);
            if (dismissed.some((p) => issue.sources.every((s) => p.sources.includes(s)))) continue;
            if (spent >= budget) break;
            spent++;
            const findings: Array<Finding> = [];
            for (const s of issue.sources) {
              const last = (yield* db.find(Reading, "by_sensor", {
                eq: [plant.id, s],
                order: "desc",
                limit: 1,
              }))[0];
              for (const f of last?.findings ?? []) if (f.fingerprint === issue.fingerprint) findings.push(f);
            }
            const feedback = dismissed
              .map((p) => verdictOf.get(p.subject))
              .sort((a, b) => (b?.at ?? 0) - (a?.at ?? 0))[0]?.text;
            const subject = `${issue.fingerprint}@${evidenceHash(issue.sources)}`;
            out.push({
              urn: `${plant.id}/${loop.id}/${subject}`,
              subject,
              fingerprint: issue.fingerprint,
              snapshot: head.snapshot,
              sources: issue.sources,
              findings: findings.slice(0, BRIEF_FINDINGS),
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
            fingerprint: s.fingerprint,
            snapshot: s.snapshot,
            sources: s.sources,
            findings: s.findings,
            feedback: s.feedback,
          };
          const changes = yield* p.act(plant, actuators.get(loop.act)!, brief);
          if (changes === null) return yield* Effect.fail(new Error("actuator changed nothing"));
          const text = `${loop.id}: ${s.fingerprint}, seen by ${s.sources.join(", ")} at ${s.snapshot.slice(0, 7)}`;
          const proposed = yield* p.propose(plant, changes, text, loop.gate);
          const base = { plant: plant.id, loop: loop.id, subject: s.subject };
          const proposal = {
            fact: Proposal,
            draft: {
              ...base,
              fingerprint: s.fingerprint,
              person: loop.person,
              apply: proposed.apply,
              text,
              cites: [proposed.cite, ...s.findings.flatMap((f) => (f.cite === undefined ? [] : [f.cite]))],
              sources: s.sources,
            },
          };
          const policy = {
            fact: Verdict,
            draft: { ...base, accept: true, text: `policy: ${loop.id} is pre-approved`, cite: proposed.cite },
          };
          return { result: proposed.cite, append: loop.gate === "auto" ? [proposal, policy] : [proposal] };
        }),
      meta: meta(
        `Loop ${plant.id}/${loop.id}`,
        "When the evidence for one fingerprint clears the bar, run the actuator once and propose what it made.",
        "one wave per fingerprint; evidence in, proposal out",
      ),
    }),
  );

  const apply = Rule.make({
    id: `tide::${plant.id}-apply`,
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
      "Make an accepted proposal real; the receipt is the history entry.",
      "nothing auto-reverts; the plant keeps restore",
    ),
  });

  const observe = Entry.make({
    id: `tide::${plant.id}-observe`,
    source: plant.kind,
    from: plant.root,
    reads: [Snapshot, Verdict],
    writes: [Snapshot, Verdict],
    handle: (payload, { db }) =>
      Effect.gen(function* () {
        const { head, verdicts } = payload as ObservePayload;
        if (head !== undefined) {
          const seen = yield* db.find(Snapshot, "by_key", { eq: [plant.id, head.snapshot], limit: 1 });
          if (seen.length === 0) {
            yield* db.append(Snapshot, {
              plant: plant.id,
              snapshot: head.snapshot,
              commits: head.commits,
              churn: head.churn,
              ...(head.parent === undefined ? {} : { parent: head.parent }),
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
      `Observe ${plant.id}`,
      "The host's door: a new snapshot, and decisions made where the plant keeps its gate.",
      "the host observes; rules decide",
    ),
  });

  return { sense, loops, apply, observe, rules: [sense, ...loops, apply] };
};

/** A person's yes or no from the page. The same row an observed PR decision writes. */
export const Decide = Command.make({
  id: "tide::decide",
  role: "owner",
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
      if (p.person !== actor.person)
        return yield* violation("person", `only ${p.person} decides this proposal`);
      if (!accept && text.trim() === "")
        return yield* violation("text", "a rejection says why, so the next attempt reads it");
      return yield* db.append(Verdict, { plant, loop, subject, accept, text });
    }),
  meta: meta("Decide", "Say yes or no to a proposal.", "only its person decides"),
});
