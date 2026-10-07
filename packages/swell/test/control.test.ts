import { assert, describe, it } from "@effect/vitest";
import { type AnyRule, Kernel, Memory, type Store, transact } from "@swell/kernel";
import { Cause, Clock, Effect, Exit, Schema } from "effect";
import { TestClock } from "effect/testing";
import { historian } from "../src/historian.ts";
import {
  dayStart,
  Decide,
  defaultThreshold,
  defineControl,
  Measurement,
  MeasuredJson,
  PlantPort,
  Proposal,
  Retry,
  rulesOf,
  Sample,
  Signature,
  signaturesOf,
  subjectOf,
  Verdict,
  week,
} from "../src/index.ts";
import { fakePlant, fakeWorld } from "../src/testing.ts";

const spec = defineControl({
  plant: { id: "repo", kind: "git", root: "/plant", ref: "main" },
  sensors: [{ id: "lint", run: ["lint"] }],
  observers: [{ id: "review", run: ["review"], vocabulary: ["dup-code", "todo"] }],
  actuators: [{ id: "fix", run: ["fix"] }],
  loops: [
    {
      id: "purge",
      inputs: ["lint", "review"],
      actuator: "fix",
      mode: "manual",
      operator: "kai",
      limit: { perDay: 1 },
    },
  ],
});
const { rules, feedback, enablers } = rulesOf(spec);
/** A second plant whose loop allows two moves a day. */
const wide = rulesOf(
  defineControl({
    ...spec,
    plant: { ...spec.plant, id: "wide" },
    loops: [{ ...spec.loops[0]!, limit: { perDay: 2 } }],
  }),
);
const widePurge = wide.loops[0]!;
const purge = rules.find((r) => r.id === "control::repo-purge")!;
const measure = rules.find((r) => r.id === "control::repo-measure")!;
const apply = rules.find((r) => r.id === "control::repo-apply")!;
const kai = { by: "operator:kai", person: "kai", roles: new Set(["operator"]) };
const DAY = 86_400_000;

const signal = (signature: string, extra: Record<string, string> = {}) => ({
  signature,
  mechanism: "m",
  path: "a.ts",
  ...extra,
});
const measured = (sigs: ReadonlyArray<string>) => ({
  signals: sigs.map((s) => signal(s)),
  analyzed: 10,
  excluded: 0,
  failed: 0,
});

const stores: Array<[string, () => Effect.Effect<Store, never, import("effect").Scope.Scope>]> = [
  ["memory", () => Effect.succeed(Memory.memoryStore().store)],
  ["historian", () => historian(":memory:")],
];

describe("defineControl", () => {
  const base = {
    plant: { id: "p", kind: "git" as const, root: ".", ref: "main" },
    actuators: [{ id: "a", run: ["x"] }],
    loops: [],
  };
  it("refuses a duplicate instrument id across sensors and observers", () => {
    assert.throws(
      () =>
        defineControl({
          ...base,
          sensors: [{ id: "s", run: ["x"] }],
          observers: [{ id: "s", run: ["y"], vocabulary: ["v"] }],
        }),
      /twice/,
    );
  });
  it("refuses an empty run and an observer with no vocabulary", () => {
    assert.throws(() => defineControl({ ...base, sensors: [{ id: "s", run: [] }] }), /non-empty argv/);
    assert.throws(
      () => defineControl({ ...base, sensors: [], observers: [{ id: "o", run: ["x"], vocabulary: [] }] }),
      /non-empty vocabulary/,
    );
  });
  it("refuses a loop over an unknown instrument", () => {
    assert.throws(
      () =>
        defineControl({
          ...base,
          sensors: [],
          loops: [{ id: "l", inputs: ["nope"], actuator: "a", mode: "manual", operator: "k" }],
        }),
      /unknown instrument/,
    );
  });
  it("refuses an auto loop on a plant with no remote, because apply is push-only", () => {
    const auto = { id: "l", inputs: ["s"], actuator: "a", mode: "auto" as const, operator: "k" };
    const sensors = [{ id: "s", run: ["x"] }];
    assert.throws(() => defineControl({ ...base, sensors, loops: [auto] }), /push-only.*no remote/);
    defineControl({ ...base, plant: { ...base.plant, remote: "origin" }, sensors, loops: [auto] });
    defineControl({ ...base, sensors, loops: [{ ...auto, mode: "manual" }] });
  });
  it("refuses a typo by its path: a misspelled limit or budget is never silently dropped", () => {
    const sensors = [{ id: "s", run: ["x"] }];
    const loop = { id: "l", inputs: ["s"], actuator: "a", mode: "manual", operator: "k" };
    const typo = (patch: object) => () =>
      defineControl({ ...base, sensors, loops: [{ ...loop, ...patch }] } as never);
    assert.throws(typo({ limit: { perday: 1 } }), /perday|perDay/);
    assert.throws(typo({ mode: "Auto" }), /mode/);
    assert.throws(typo({ thresold: () => true }), /thresold/);
    assert.throws(
      () =>
        defineControl({
          ...base,
          sensors,
          observers: [{ id: "o", run: ["x"], vocabulary: ["v"], every: { commit: 20 } }],
        } as never),
      /commit/,
    );
    assert.throws(
      () => defineControl({ ...base, plant: { ...base.plant, kind: "svn" }, sensors } as never),
      /kind/,
    );
  });
  it("refuses passing the controller's own SWELL_* environment to a child", () => {
    assert.throws(
      () => defineControl({ ...base, sensors: [{ id: "s", run: ["x"], env: ["SWELL_TOKEN"] }] }),
      /SWELL_/,
    );
    defineControl({ ...base, sensors: [{ id: "s", run: ["x"], env: ["ANTHROPIC_API_KEY"] }] });
  });
});

describe("time", () => {
  it("a tally week is the UTC Monday it starts on, whatever the time of day", () => {
    assert.strictEqual(week(Date.UTC(2026, 9, 7, 9)), "2026-10-05");
    assert.strictEqual(week(Date.UTC(2026, 9, 7, 23, 59)), "2026-10-05");
    assert.strictEqual(week(Date.UTC(2026, 9, 5)), "2026-10-05");
    assert.strictEqual(week(Date.UTC(2026, 0, 1)), "2025-12-29");
    assert.strictEqual(week(Date.UTC(2027, 0, 3, 23, 59)), "2026-12-28");
  });
  it("the day starts at UTC midnight", () => {
    assert.strictEqual(dayStart(Date.UTC(2026, 9, 7, 15, 30)), Date.UTC(2026, 9, 7));
  });
});

describe("signatures are checked at the instrument edge", () => {
  it("a signature with | or ~ or a non-ASCII character is a failed measurement, never an invisible tally", () => {
    const decode = (signature: string) =>
      Exit.isSuccess(
        Schema.decodeUnknownExit(MeasuredJson)(JSON.stringify({ signals: [{ signature, mechanism: "m" }] })),
      );
    assert.isTrue(decode("dup-code"));
    assert.isTrue(decode("new:dup_code"));
    for (const bad of ["a|b", "~x", "é-unicode", ""]) assert.isFalse(decode(bad), bad);
  });
});

describe("threshold properties", () => {
  const Sig = Schema.Struct({ ...Signature.fields, sources: Schema.Array(Schema.String) });
  // Sources are a set: a fold never yields the same instrument twice.
  it.prop("a new source never un-crosses the default threshold", [Sig, Schema.String], ([raw, src]) => {
    const sig = { ...raw, sources: [...new Set(raw.sources)] };
    return (
      !defaultThreshold(sig) || defaultThreshold({ ...sig, sources: [...new Set([...sig.sources, src])] })
    );
  });
  it.prop(
    "a subject ignores the order of its sources",
    [Schema.Array(Schema.String)],
    ([xs]) => subjectOf("s", xs, 1) === subjectOf("s", [...xs].reverse(), 1),
  );
});

describe.each(stores)("control over %s", (_name, mkStore) => {
  const boot = (at = 0) =>
    Effect.gen(function* () {
      yield* TestClock.setTime(at);
      const world = fakeWorld();
      const store = yield* mkStore();
      const sim = Memory.simulator(
        (p) => (p.id === PlantPort.id ? fakePlant(world) : (p.fake as never)),
        { store },
        "controller:test",
      );
      const now = yield* Clock.currentTimeMillis;
      for (const r of [...rules, ...wide.rules]) {
        const via = enablers.get(r.id) ?? wide.enablers.get(r.id);
        yield* transact(store, { by: "controller:test", via, now, trace: undefined }, (db) =>
          db.append(Kernel.RuleEnabled, { rule: r.id }),
        );
      }
      yield* TestClock.adjust(1000);
      const sample = (id: string, commits = 1) =>
        sim.entry(feedback, { sample: { sample: id, commits, churn: commits * 10 }, verdicts: [] });
      const sweep = (...rs: Array<AnyRule>) =>
        Effect.gen(function* () {
          const plans = [];
          for (const r of rs) {
            plans.push(yield* sim.sweep(r));
            yield* sim.drain;
          }
          return plans;
        });
      const proposals = () =>
        sim.reader.find(Proposal, "by_loop", { eq: ["repo", "purge"], gte: 0, limit: 100 });
      const decide = (subject: string, accept: boolean, text = accept ? "ship" : "not now") =>
        sim.command(Decide, { plant: "repo", loop: "purge", subject, accept, text }, kai);
      return { world, store, sim, sample, sweep, proposals, decide };
    });

  it.effect("a failed transaction leaves nothing behind", () =>
    Effect.gen(function* () {
      const t = yield* boot();
      const now = yield* Clock.currentTimeMillis;
      const exit = yield* Effect.exit(
        transact(t.store, { by: "test", now, trace: undefined }, (db) =>
          Effect.gen(function* () {
            yield* db.append(Sample, { plant: "repo", sample: "lost", commits: 0, churn: 0 });
            return yield* Effect.fail(new Error("after the append"));
          }),
        ),
      );
      assert.isTrue(exit._tag === "Failure");
      const rows = yield* t.sim.reader.find(Sample, "by_key", { eq: ["repo", "lost"], limit: 1 });
      assert.strictEqual(rows.length, 0);
    }),
  );

  it.effect("a nested transaction is a savepoint on both stores", () =>
    Effect.gen(function* () {
      const t = yield* boot();
      const now = yield* Clock.currentTimeMillis;
      yield* transact(t.store, { by: "test", now, trace: undefined }, (db) =>
        Effect.gen(function* () {
          yield* db.append(Sample, { plant: "repo", sample: "outer", commits: 0, churn: 0 });
          yield* t.store
            .transaction(
              Effect.gen(function* () {
                yield* db.append(Sample, { plant: "repo", sample: "inner", commits: 0, churn: 0 });
                return yield* Effect.fail("inner fails");
              }),
            )
            .pipe(Effect.ignore);
        }),
      );
      const rows = yield* t.sim.reader.find(Sample, "by_plant", { eq: ["repo"], limit: 10 });
      assert.deepStrictEqual(
        rows.map((r) => r.sample),
        ["outer"],
      );
    }),
  );

  it.effect("a measurement per instrument per sample; a failed one is an error row, never a zero", () =>
    Effect.gen(function* () {
      const t = yield* boot();
      t.world.measured.set("lint@s1", measured(["dup-code"]));
      yield* t.sample("s1");
      const [plan] = yield* t.sweep(measure);
      assert.strictEqual(plan!.wanted, 2);
      const estimates = yield* t.sim.reader.find(Measurement, "by_instrument", {
        eq: ["repo", "review"],
        gte: 0,
        limit: 10,
      });
      assert.strictEqual(estimates.length, 1);
      assert.deepStrictEqual(
        [estimates[0]!.kind, estimates[0]!.failed, estimates[0]!.signals.length],
        ["estimated", 1, 0],
      );
      assert.include(estimates[0]!.error ?? "", "no scripted measurement");
      const now = yield* Clock.currentTimeMillis;
      const sigs = yield* signaturesOf(t.sim.reader, "repo", now);
      assert.deepStrictEqual(
        sigs.map((s) => [s.signature, s.sources, s.rate]),
        [["dup-code", ["lint"], 1]],
      );
    }),
  );

  it.effect("two sources agree, the loop moves once, a yes applies it, and the receipt is the history", () =>
    Effect.gen(function* () {
      const t = yield* boot();
      t.world.measured.set("lint@s1", measured(["dup-code"]));
      t.world.measured.set("review@s1", {
        signals: [signal("dup-code", { cite: "replay:1" })],
        analyzed: 3,
        excluded: 0,
        failed: 0,
      });
      t.world.changes = { ref: "swell/purge/dup", head: "h1", summary: "+1 -3" };
      yield* t.sample("s1");
      yield* t.sweep(measure);
      const [plan] = yield* t.sweep(purge);
      assert.strictEqual(plan!.wanted, 1);
      assert.strictEqual(t.world.acts.length, 1);
      assert.deepStrictEqual(t.world.acts[0]!.sources, ["lint", "review"]);
      const [p] = yield* t.proposals();
      assert.strictEqual(p!.subject, subjectOf("dup-code", ["review", "lint"], 1));
      assert.strictEqual(p!.subject, "dup-code@lint+review#1");
      assert.deepStrictEqual(p!.cites, ["fake:swell/purge/dup", "replay:1"]);
      // The loop writes as itself, for the operator it was switched on for.
      assert.deepStrictEqual([p!.by, p!.via], ["rule:control::repo-purge", "operator:kai"]);
      // The same evidence is covered while the proposal is open.
      assert.strictEqual((yield* t.sweep(purge))[0]!.wanted, 0);

      yield* t.decide(p!.subject, true);
      yield* t.sweep(apply);
      assert.deepStrictEqual(t.world.applied, [p!.apply]);
      const receipts = yield* t.sim.reader.find(Kernel.Receipt, "by_key", { eq: [apply.id], limit: 10 });
      assert.deepStrictEqual(
        receipts.map((r) => r.outcome),
        ["ok"],
      );
      assert.strictEqual(receipts[0]!.result, "merged:h1");
      // Still covered after the apply: no sample since, so nothing says the move failed to fix it.
      assert.strictEqual((yield* t.sweep(purge))[0]!.wanted, 0);
    }),
  );

  it.effect(
    "re-arm: after a move lands, the signature moves again only if a later sample still shows it",
    () =>
      Effect.gen(function* () {
        const t = yield* boot();
        t.world.changes = { ref: "swell/purge/dup", head: "h1", summary: "" };
        t.world.measured.set("lint@s1", measured(["dup-code"]));
        t.world.measured.set("review@s1", measured(["dup-code"]));
        yield* t.sample("s1");
        yield* t.sweep(measure, purge);
        const [p1] = yield* t.proposals();
        yield* t.decide(p1!.subject, true);
        yield* t.sweep(apply);
        // The move landed and the next sample is clean: the tallies still remember dup-code, but it does not re-arm.
        yield* TestClock.adjust(DAY);
        t.world.measured.set("lint@s2", measured([]));
        t.world.measured.set("review@s2", measured([]));
        yield* t.sample("s2");
        yield* t.sweep(measure, purge);
        assert.strictEqual((yield* t.proposals()).length, 1);
        // It comes back on a later sample: a second arming, a new subject, a new move.
        yield* TestClock.adjust(DAY);
        t.world.measured.set("lint@s3", measured(["dup-code"]));
        t.world.measured.set("review@s3", measured(["dup-code"]));
        yield* t.sample("s3");
        yield* t.sweep(measure, purge);
        const ps = yield* t.proposals();
        assert.deepStrictEqual(
          ps.map((p) => [p.subject, p.arming]),
          [
            ["dup-code@lint+review#1", 1],
            ["dup-code@lint+review#2", 2],
          ],
        );
      }),
  );

  it.effect("a move that can never land re-arms, and the next attempt reads why", () =>
    Effect.gen(function* () {
      const t = yield* boot();
      t.world.changes = { ref: "swell/purge/dup", head: "h1", summary: "" };
      t.world.applyFails = 5;
      t.world.measured.set("lint@s1", measured(["dup-code"]));
      t.world.measured.set("review@s1", measured(["dup-code"]));
      yield* t.sample("s1");
      yield* t.sweep(measure, purge);
      const [p1] = yield* t.proposals();
      yield* t.decide(p1!.subject, true);
      for (let i = 0; i < 5; i++) yield* t.sweep(apply);
      const [plan] = yield* t.sweep(apply);
      assert.strictEqual(plan!.dead.length, 1);
      yield* TestClock.adjust(DAY);
      yield* t.sweep(purge);
      const ps = yield* t.proposals();
      assert.deepStrictEqual(
        ps.map((p) => p.arming),
        [1, 2],
      );
      assert.include(t.world.acts.at(-1)!.previous, "could not land");
      assert.include(t.world.acts.at(-1)!.previous, "squash conflict");
    }),
  );

  it.effect("an operator lifts a dead letter: the retry grant resets the failures", () =>
    Effect.gen(function* () {
      const t = yield* boot();
      t.world.changes = { ref: "swell/purge/dup", head: "h1", summary: "" };
      t.world.applyFails = 5;
      t.world.measured.set("lint@s1", measured(["dup-code"]));
      t.world.measured.set("review@s1", measured(["dup-code"]));
      yield* t.sample("s1");
      yield* t.sweep(measure, purge);
      const [p1] = yield* t.proposals();
      yield* t.decide(p1!.subject, true);
      for (let i = 0; i < 5; i++) yield* t.sweep(apply);
      const urn = `repo/purge/${p1!.subject}`;
      yield* t.sim.command(Retry, { rule: apply.id, subject: urn }, kai);
      yield* TestClock.adjust(1000);
      yield* t.sweep(apply);
      assert.deepStrictEqual(t.world.applied, [p1!.apply]);
    }),
  );

  it.effect("a verdict's args are decoded: a string where a boolean is declared is a defect, not a yes", () =>
    Effect.gen(function* () {
      const t = yield* boot();
      t.world.measured.set("lint@s1", measured(["dup-code"]));
      t.world.measured.set("review@s1", measured(["dup-code"]));
      t.world.changes = { ref: "swell/purge/dup", head: "h1", summary: "" };
      yield* t.sample("s1");
      yield* t.sweep(measure, purge);
      const [p] = yield* t.proposals();
      const exit = yield* Effect.exit(
        t.sim.command(
          Decide,
          { plant: "repo", loop: "purge", subject: p!.subject, accept: "false" as never, text: "" },
          kai,
        ),
      );
      assert.isTrue(Exit.isFailure(exit) && Cause.hasDies(exit.cause));
      assert.strictEqual(
        (yield* t.sim.reader.find(Verdict, "by_plant", { eq: ["repo"], gte: 0, limit: 10 })).length,
        0,
      );
    }),
  );

  it.effect("hysteresis: a dismissal holds until a source joins the evidence set", () =>
    Effect.gen(function* () {
      const t = yield* boot();
      t.world.changes = { ref: "swell/purge/todo", head: "h2", summary: "" };
      // One sensor alone crosses the default threshold only on its third run.
      for (const s of ["s1", "s2"]) {
        t.world.measured.set(`lint@${s}`, measured(["todo"]));
        yield* t.sample(s);
        yield* t.sweep(measure, purge);
        assert.strictEqual((yield* t.proposals()).length, 0);
      }
      t.world.measured.set("lint@s3", measured(["todo"]));
      yield* t.sample("s3");
      yield* t.sweep(measure, purge);
      const [p1] = yield* t.proposals();
      assert.isDefined(p1);
      yield* t.decide(p1!.subject, false);
      // The rate wobbles above the threshold on the next sample; same evidence, still dismissed.
      t.world.measured.set("lint@s4", measured(["todo", "todo"]));
      yield* TestClock.adjust(DAY);
      yield* t.sample("s4");
      yield* t.sweep(measure, purge);
      assert.strictEqual((yield* t.proposals()).length, 1);
      // A second source joins: a new subject, a new move, and the brief carries the rejection text.
      t.world.measured.set("lint@s5", measured(["todo"]));
      t.world.measured.set("review@s5", measured(["todo"]));
      yield* TestClock.adjust(DAY);
      yield* t.sample("s5");
      yield* t.sweep(measure, purge);
      const ps = yield* t.proposals();
      assert.strictEqual(ps.length, 2);
      assert.deepStrictEqual(ps[1]!.sources, ["lint", "review"]);
      assert.strictEqual(t.world.acts.at(-1)!.feedback, "not now");
    }),
  );

  it.effect("the rate limit caps the heavy day; the next day the second move goes", () =>
    Effect.gen(function* () {
      const t = yield* boot();
      t.world.measured.set("lint@s1", measured(["dup-code", "todo"]));
      t.world.measured.set("review@s1", measured(["dup-code", "todo"]));
      t.world.changes = { ref: "swell/purge/x", head: "h3", summary: "" };
      yield* t.sample("s1");
      yield* t.sweep(measure, purge);
      assert.strictEqual((yield* t.proposals()).length, 1);
      assert.strictEqual((yield* t.sweep(purge))[0]!.wanted, 0);
      yield* TestClock.adjust(DAY);
      yield* t.sweep(purge);
      assert.strictEqual((yield* t.proposals()).length, 2);
    }),
  );

  it.effect("the limit resets at UTC midnight, not a rolling 24 hours later", () =>
    Effect.gen(function* () {
      const t = yield* boot(Date.UTC(2026, 9, 7, 23, 50));
      t.world.measured.set("lint@s1", measured(["dup-code", "todo"]));
      t.world.measured.set("review@s1", measured(["dup-code", "todo"]));
      t.world.changes = { ref: "swell/purge/x", head: "h3", summary: "" };
      yield* t.sample("s1");
      yield* t.sweep(measure, purge);
      assert.strictEqual((yield* t.proposals()).length, 1);
      // Twenty minutes later it is a new UTC day: the second move goes.
      yield* TestClock.adjust(20 * 60_000);
      yield* t.sweep(purge);
      assert.strictEqual((yield* t.proposals()).length, 2);
    }),
  );

  it.effect(
    "an attempt in flight never spends the day twice, so a second signature still gets its move",
    () =>
      Effect.gen(function* () {
        const t = yield* boot();
        const now = () => Clock.currentTimeMillis;
        const write = (instrument: string, sample: string, sigs: ReadonlyArray<string>) =>
          Effect.gen(function* () {
            yield* transact(t.store, { by: "t", now: yield* now(), trace: undefined }, (db) =>
              db.append(Measurement, {
                plant: "wide",
                instrument,
                sample,
                kind: "measured",
                signals: sigs.map((s) => signal(s)),
                analyzed: 1,
                excluded: 0,
                failed: 0,
              }),
            );
          });
        yield* t.sim.entry(wide.feedback, { sample: { sample: "w1", commits: 1, churn: 1 }, verdicts: [] });
        yield* write("lint", "w1", ["a-sig"]);
        yield* write("review", "w1", ["a-sig"]);
        // Swept but not drained: a-sig's attempt is in flight, its actuator still running.
        assert.strictEqual((yield* t.sim.sweep(widePurge)).pending.length, 1);
        // b-sig crosses too. perDay 2: a-sig spent one; its attempt in flight is not metered again.
        yield* write("lint", "w1b", ["a-sig", "b-sig"]);
        yield* write("review", "w1b", ["a-sig", "b-sig"]);
        const plan = yield* t.sim.sweep(widePurge);
        assert.deepStrictEqual(
          [plan.inflight, plan.pending.map((p) => p.urn)],
          [1, [`wide/purge/${subjectOf("b-sig", ["lint", "review"], 1)}`]],
        );
        t.sim.queue.length = 0;
      }),
  );

  it.effect(
    "the limit counts attempts: a failed propose spends the day, and the rule gives up after two",
    () =>
      Effect.gen(function* () {
        const t = yield* boot();
        t.world.measured.set("lint@s1", measured(["dup-code"]));
        t.world.measured.set("review@s1", measured(["dup-code"]));
        t.world.changes = { ref: "swell/purge/dup", head: "h5", summary: "" };
        t.world.proposeFails = true;
        yield* t.sample("s1");
        yield* t.sweep(measure, purge);
        assert.strictEqual(t.world.acts.length, 1);
        assert.strictEqual((yield* t.proposals()).length, 0);
        // The actuator ran and spent; perDay 1 holds across repeated sweeps that day.
        for (let i = 0; i < 3; i++) assert.strictEqual((yield* t.sweep(purge))[0]!.pending.length, 0);
        assert.strictEqual(t.world.acts.length, 1);
        const attempts = () =>
          t.sim.reader.find(Kernel.Attempt, "by_rule", { eq: [purge.id], gte: 0, limit: 10 });
        assert.strictEqual((yield* attempts()).length, 1);
        // The next day the retry goes, and its brief carries why the last one failed.
        yield* TestClock.adjust(DAY);
        yield* t.sweep(purge);
        assert.strictEqual(t.world.acts.length, 2);
        assert.deepStrictEqual(t.world.acts[0]!.previous, "");
        assert.include(t.world.acts[1]!.previous, "gh is down");
        // Two failed attempts and the loop rule is done, however many days pass.
        yield* TestClock.adjust(DAY);
        const [plan] = yield* t.sweep(purge);
        assert.deepStrictEqual([plan!.pending.length, plan!.dead.length], [0, 1]);
        assert.strictEqual(t.world.acts.length, 2);
        assert.strictEqual((yield* attempts()).length, 2);
        assert.deepStrictEqual([purge.maxAttempts, purge.leaseMs], [2, 3_600_000 + 300_000]);
        // The measure rule's lease outlasts its slowest instrument, the observer's thirty minutes.
        assert.strictEqual(measure.leaseMs, 1_800_000 + 300_000);
      }),
  );

  it.effect("a crashed actuator spends the day too", () =>
    Effect.gen(function* () {
      const t = yield* boot();
      t.world.measured.set("lint@s1", measured(["dup-code"]));
      t.world.measured.set("review@s1", measured(["dup-code"]));
      t.world.changes = { ref: "swell/purge/dup", head: "h6", summary: "" };
      t.world.failNext = 1;
      yield* t.sample("s1");
      yield* t.sweep(measure, purge);
      assert.strictEqual((yield* t.sweep(purge))[0]!.pending.length, 0);
      assert.strictEqual(t.world.acts.length, 0);
      yield* TestClock.adjust(DAY);
      yield* t.sweep(purge);
      assert.strictEqual((yield* t.proposals()).length, 1);
    }),
  );

  it.effect(
    "rate is the share of runs that saw it: two signals in one run is rate 1, one sensor needs three runs",
    () =>
      Effect.gen(function* () {
        const t = yield* boot();
        t.world.changes = { ref: "swell/purge/dup", head: "h7", summary: "" };
        const tally = () =>
          Effect.gen(function* () {
            const now = yield* Clock.currentTimeMillis;
            const [sig] = yield* signaturesOf(t.sim.reader, "repo", now);
            return [sig!.hits, sig!.seen, sig!.runs, sig!.rate];
          });
        t.world.measured.set("lint@s1", measured(["dup-code", "dup-code"]));
        yield* t.sample("s1");
        yield* t.sweep(measure, purge);
        assert.deepStrictEqual(yield* tally(), [2, 1, 1, 1]);
        assert.strictEqual((yield* t.proposals()).length, 0);
        t.world.measured.set("lint@s2", measured(["dup-code"]));
        yield* t.sample("s2");
        yield* t.sweep(measure, purge);
        assert.deepStrictEqual(yield* tally(), [3, 2, 2, 1]);
        assert.strictEqual((yield* t.proposals()).length, 0);
        t.world.measured.set("lint@s3", measured(["dup-code"]));
        yield* t.sample("s3");
        yield* t.sweep(measure, purge);
        assert.deepStrictEqual(yield* tally(), [4, 3, 3, 1]);
        assert.strictEqual((yield* t.proposals()).length, 1);
      }),
  );

  it.effect("a failed measurement is not a run: it never pulls the rate below 1", () =>
    Effect.gen(function* () {
      const t = yield* boot();
      t.world.changes = { ref: "swell/purge/dup", head: "h7", summary: "" };
      for (const s of ["s1", "s2", "s3", "s4"]) {
        // s2's lint is unscripted, so it fails.
        if (s !== "s2") t.world.measured.set(`lint@${s}`, measured(["dup-code"]));
        yield* t.sample(s);
        yield* t.sweep(measure, purge);
      }
      const now = yield* Clock.currentTimeMillis;
      const [sig] = yield* signaturesOf(t.sim.reader, "repo", now);
      assert.deepStrictEqual([sig!.seen, sig!.runs, sig!.rate], [3, 3, 1]);
      assert.strictEqual((yield* t.proposals()).length, 1);
    }),
  );

  it.effect(
    "a window of four weeks reads four weeks, however much history sorts before a live signature",
    () =>
      Effect.gen(function* () {
        const t = yield* boot(Date.UTC(2026, 9, 7));
        const now = yield* Clock.currentTimeMillis;
        // A year of stale history: 60 signatures over 40 old weeks, all sorting before the live one.
        for (let w = 10; w < 50; w++)
          for (let s = 0; s < 60; s++)
            yield* t.store.tally.add("control::signatures", `repo|${week(now - w * 7 * DAY)}|sig|a-${s}`, {
              hits: 1,
              seen: 1,
              "src:lint": 1,
            });
        t.world.measured.set("lint@s1", measured(["zz-live"]));
        t.world.measured.set("review@s1", measured(["zz-live"]));
        yield* t.sample("s1");
        yield* t.sweep(measure);
        const sigs = yield* signaturesOf(t.sim.reader, "repo", yield* Clock.currentTimeMillis);
        assert.deepStrictEqual(
          sigs.map((s) => s.signature),
          ["zz-live"],
        );
      }),
  );

  it.effect("an observer's outsider joins a sensor's signature of the same name: its second source", () =>
    Effect.gen(function* () {
      const t = yield* boot();
      t.world.changes = { ref: "swell/purge/x", head: "h8", summary: "" };
      t.world.measured.set("review@s1", measured(["weird"]));
      yield* t.sample("s1");
      yield* t.sweep(measure, purge);
      const now = yield* Clock.currentTimeMillis;
      assert.deepStrictEqual(
        (yield* signaturesOf(t.sim.reader, "repo", now)).map((s) => s.signature),
        ["new:weird"],
      );
      assert.strictEqual((yield* t.proposals()).length, 0);
      t.world.measured.set("lint@s2", measured(["weird"]));
      t.world.measured.set("review@s2", measured(["weird"]));
      yield* t.sample("s2");
      yield* t.sweep(measure, purge);
      const sigs = yield* signaturesOf(t.sim.reader, "repo", yield* Clock.currentTimeMillis);
      assert.deepStrictEqual(
        sigs.map((s) => [s.signature, s.sources]),
        [["weird", ["lint", "review"]]],
      );
      assert.strictEqual((yield* t.proposals()).length, 1);
    }),
  );

  it.effect("an actuator that changes nothing is a failed attempt, retried, never a proposal", () =>
    Effect.gen(function* () {
      const t = yield* boot();
      t.world.measured.set("lint@s1", measured(["dup-code"]));
      t.world.measured.set("review@s1", measured(["dup-code"]));
      t.world.changes = null;
      yield* t.sample("s1");
      yield* t.sweep(measure, purge);
      assert.strictEqual((yield* t.proposals()).length, 0);
      const receipts = yield* t.sim.reader.find(Kernel.Receipt, "by_key", { eq: [purge.id], limit: 10 });
      assert.deepStrictEqual(
        receipts.map((r) => r.outcome),
        ["failed"],
      );
      assert.include(receipts[0]!.error ?? "", "changed nothing");
    }),
  );

  it.effect("a verdict read where the plant keeps its operator lands through the feedback path", () =>
    Effect.gen(function* () {
      const t = yield* boot();
      t.world.measured.set("lint@s1", measured(["dup-code"]));
      t.world.measured.set("review@s1", measured(["dup-code"]));
      t.world.changes = { ref: "swell/purge/dup", head: "h4", summary: "" };
      yield* t.sample("s1");
      yield* t.sweep(measure, purge);
      const [p] = yield* t.proposals();
      yield* t.sim.entry(feedback, {
        verdicts: [{ loop: "purge", subject: p!.subject, accept: true, text: "", cite: "https://pr/1" }],
      });
      const v = yield* t.sim.reader.find(Verdict, "by_key", { eq: ["repo", "purge", p!.subject], limit: 1 });
      assert.deepStrictEqual([v[0]!.accept, v[0]!.cite], [true, "https://pr/1"]);
      yield* t.sweep(apply);
      assert.strictEqual(t.world.applied.length, 1);
    }),
  );

  it.effect("a malformed feedback payload is refused, never cast", () =>
    Effect.gen(function* () {
      const t = yield* boot();
      const err = yield* Effect.flip(t.sim.entry(feedback, { verdicts: [{ loop: "purge" }] }));
      assert.strictEqual(err._tag, "InvariantViolation");
    }),
  );
});

/** The memory store is the spec; the historian must agree on any measurement history, not only the scripted ones. */
describe("memory and historian agree", () => {
  // A sensor prints known signatures; an observer adds outsiders, renamed `new:`, never beside the same known one.
  const Draw = Schema.Array(
    Schema.Struct({
      instrument: Schema.Literals(["lint", "review"]),
      lint: Schema.Array(Schema.Literals(["a", "b"])).check(Schema.isMaxLength(4)),
      review: Schema.Array(Schema.Literals(["b", "new:a", "new:c"])).check(Schema.isMaxLength(4)),
      fails: Schema.Boolean,
      gapH: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 200 })),
    }),
  ).check(Schema.isMaxLength(12));

  const fold = (store: Store, draws: typeof Draw.Type) =>
    Effect.gen(function* () {
      const reader = Memory.simulator(undefined, { store }).reader;
      let i = 0;
      for (const d of draws) {
        yield* TestClock.adjust(d.gapH * 3_600_000 + 1);
        const now = yield* Clock.currentTimeMillis;
        yield* transact(store, { by: "t", now, trace: undefined }, (db) =>
          db.append(Measurement, {
            plant: "p",
            instrument: d.instrument,
            sample: `s${i++}`,
            kind: "measured",
            signals: d.fails ? [] : d[d.instrument].map((signature) => ({ signature, mechanism: "m" })),
            analyzed: 1,
            excluded: 0,
            failed: d.fails ? 1 : 0,
            ...(d.fails ? { error: "broke" } : {}),
          }),
        );
      }
      const rows = yield* reader.find(Measurement, "by_instrument", {
        eq: ["p", "lint"],
        order: "desc",
        limit: 5,
      });
      return {
        sigs: yield* signaturesOf(reader, "p", yield* Clock.currentTimeMillis),
        rows: rows.map((r) => [r.sample, r.at]),
      };
    });

  it.effect.prop("signatures and reads agree on both stores", [Draw], ([draws]) =>
    Effect.gen(function* () {
      yield* TestClock.setTime(Date.UTC(2026, 9, 7));
      const mem = yield* fold(Memory.memoryStore().store, draws);
      yield* TestClock.setTime(Date.UTC(2026, 9, 7));
      const sql = yield* fold(yield* historian(":memory:"), draws);
      assert.deepStrictEqual(sql, mem);
      for (const s of mem.sigs)
        assert.isTrue(s.rate >= 0 && s.rate <= 1 && s.seen <= s.runs && s.seen <= s.hits);
    }).pipe(Effect.scoped),
  );
});
