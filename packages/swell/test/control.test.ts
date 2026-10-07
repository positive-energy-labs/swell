import { assert, describe, it } from "@effect/vitest";
import { type AnyRule, Kernel, Memory, type Store, transact } from "@tc/kernel";
import { Clock, Effect } from "effect";
import { TestClock } from "effect/testing";
import { historian } from "../src/historian.ts";
import {
  Decide,
  defineControl,
  evidenceHash,
  fakePlant,
  fakeWorld,
  Measurement,
  PlantPort,
  Proposal,
  rulesOf,
  Sample,
  signaturesOf,
  Verdict,
  week,
} from "../src/index.ts";

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
const { rules, feedback } = rulesOf(spec);
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

const stores: Array<[string, () => Store]> = [
  ["memory", () => Memory.memoryStore().store],
  ["historian", () => historian(":memory:").store],
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
    assert.throws(() => defineControl({ ...base, sensors: [{ id: "s", run: [] }] }), /empty run/);
    assert.throws(
      () => defineControl({ ...base, sensors: [], observers: [{ id: "o", run: ["x"], vocabulary: [] }] }),
      /empty vocabulary/,
    );
  });
  it("refuses a loop over an unknown instrument", () => {
    assert.throws(
      () =>
        defineControl({
          ...base,
          sensors: [],
          loops: [{ id: "l", inputs: ["nope"], actuator: "a", mode: "auto", operator: "k" }],
        }),
      /unknown instrument/,
    );
  });
});

describe("week", () => {
  it("is the ISO week whatever the time of day", () => {
    assert.strictEqual(week(Date.UTC(2026, 9, 7, 9)), "2026-W41");
    assert.strictEqual(week(Date.UTC(2026, 9, 7, 15)), "2026-W41");
    assert.strictEqual(week(Date.UTC(2026, 0, 1)), "2026-W01");
    assert.strictEqual(week(Date.UTC(2027, 0, 3, 23, 59)), "2026-W53");
  });
});

describe.each(stores)("control over %s", (_name, mkStore) => {
  const boot = () =>
    Effect.gen(function* () {
      const world = fakeWorld();
      const store = mkStore();
      const sim = Memory.simulator(
        (p) => (p.id === PlantPort.id ? fakePlant(world) : (p.fake as never)),
        { store },
        "controller:test",
      );
      const now = yield* Clock.currentTimeMillis;
      for (const r of rules) {
        yield* transact(store, { by: "test", now, trace: undefined }, (db) =>
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
      return { world, store, sim, sample, sweep, proposals };
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
      assert.strictEqual(p!.subject, `dup-code@${evidenceHash(["lint", "review"])}`);
      assert.deepStrictEqual(p!.cites, ["fake:swell/purge/dup", "replay:1"]);
      // The same evidence is covered while the proposal is open.
      assert.strictEqual((yield* t.sweep(purge))[0]!.wanted, 0);

      yield* t.sim.command(
        Decide,
        { plant: "repo", loop: "purge", subject: p!.subject, accept: true, text: "ship" },
        kai,
      );
      yield* t.sweep(apply);
      assert.deepStrictEqual(t.world.applied, [p!.apply]);
      const receipts = yield* t.sim.reader.find(Kernel.Receipt, "by_key", { eq: [apply.id], limit: 10 });
      assert.deepStrictEqual(
        receipts.map((r) => [r.outcome, r.result]),
        [["ok", "merged:h1"]],
      );
      // Still covered after the apply: an accepted move never re-fires on the same evidence.
      assert.strictEqual((yield* t.sweep(purge))[0]!.wanted, 0);
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
      assert.isTrue(exit._tag === "Failure");
      assert.strictEqual(
        (yield* t.sim.reader.find(Verdict, "by_plant", { eq: ["repo"], gte: 0, limit: 10 })).length,
        0,
      );
    }),
  );

  it.effect("hysteresis: a dismissal holds until a source joins the evidence set", () =>
    Effect.gen(function* () {
      const t = yield* boot();
      t.world.measured.set("lint@s1", measured(["todo"]));
      t.world.changes = { ref: "swell/purge/todo", head: "h2", summary: "" };
      yield* t.sample("s1");
      yield* t.sweep(measure, purge);
      const [p1] = yield* t.proposals();
      assert.isDefined(p1);
      yield* t.sim.command(
        Decide,
        { plant: "repo", loop: "purge", subject: p1!.subject, accept: false, text: "not now" },
        kai,
      );
      // The rate wobbles above the threshold on the next sample; same evidence, still dismissed.
      t.world.measured.set("lint@s2", measured(["todo", "todo"]));
      yield* TestClock.adjust(DAY);
      yield* t.sample("s2");
      yield* t.sweep(measure, purge);
      assert.strictEqual((yield* t.proposals()).length, 1);
      // A second source joins: a new subject, a new move, and the brief carries the rejection text.
      t.world.measured.set("lint@s3", measured(["todo"]));
      t.world.measured.set("review@s3", measured(["todo"]));
      yield* TestClock.adjust(DAY);
      yield* t.sample("s3");
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

  it.effect(
    "an observer's signature outside its vocabulary is `new:` and alone never crosses the threshold",
    () =>
      Effect.gen(function* () {
        const t = yield* boot();
        t.world.measured.set("review@s1", measured(["weird"]));
        yield* t.sample("s1");
        yield* t.sweep(measure, purge);
        const now = yield* Clock.currentTimeMillis;
        const sigs = yield* signaturesOf(t.sim.reader, "repo", now);
        assert.deepStrictEqual(
          sigs.map((s) => s.signature),
          ["new:weird"],
        );
        assert.strictEqual((yield* t.proposals()).length, 0);
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
});
