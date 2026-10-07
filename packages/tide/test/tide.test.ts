import { assert, describe, it } from "@effect/vitest";
import { type AnyRule, Kernel, Memory, type Store, transact } from "@tc/kernel";
import { Clock, Effect } from "effect";
import { TestClock } from "effect/testing";
import {
  Decide,
  defineTide,
  evidenceHash,
  fakePlant,
  fakeWorld,
  issuesOf,
  PlantPort,
  Proposal,
  Reading,
  rulesOf,
  Verdict,
} from "../src/index.ts";
import { sqliteStore } from "../src/sqlite.ts";

const tide = defineTide({
  plant: { id: "repo", kind: "git", root: "/plant", ref: "main" },
  sensors: [
    { id: "lint", kind: "measured", run: ["lint"] },
    { id: "review", kind: "model", run: ["review"], vocabulary: ["dup-code", "todo"] },
  ],
  actuators: [{ id: "fix", run: ["fix"] }],
  loops: [
    { id: "purge", sense: ["lint", "review"], act: "fix", gate: "pr", person: "kai", budget: { perDay: 1 } },
  ],
});
const { rules, observe } = rulesOf(tide);
const purge = rules.find((r) => r.id === "tide::repo-purge")!;
const sense = rules.find((r) => r.id === "tide::repo-sense")!;
const apply = rules.find((r) => r.id === "tide::repo-apply")!;
const kai = { by: "person:kai", person: "kai", roles: new Set(["owner"]) };
const DAY = 86_400_000;

const found = (fingerprint: string, extra: Record<string, string> = {}) => ({
  fingerprint,
  mechanism: "m",
  path: "a.ts",
  ...extra,
});
const sensed = (fps: ReadonlyArray<string>) => ({
  findings: fps.map((f) => found(f)),
  analyzed: 10,
  excluded: 0,
  failed: 0,
});

const stores: Array<[string, () => Store]> = [
  ["memory", () => Memory.memoryStore().store],
  ["sqlite", () => sqliteStore(":memory:").store],
];

describe.each(stores)("tide over %s", (_name, mkStore) => {
  const boot = () =>
    Effect.gen(function* () {
      const world = fakeWorld();
      const store = mkStore();
      const sim = Memory.simulator(
        (p) => (p.id === PlantPort.id ? fakePlant(world) : (p.fake as never)),
        { store },
        "host:test",
      );
      const now = yield* Clock.currentTimeMillis;
      for (const r of rules) {
        yield* transact(store, { by: "test", now, trace: undefined }, (db) =>
          db.append(Kernel.RuleEnabled, { rule: r.id }),
        );
      }
      yield* TestClock.adjust(1000);
      const snapshot = (id: string, commits = 1) =>
        sim.entry(observe, { head: { snapshot: id, commits, churn: commits * 10 }, verdicts: [] });
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
      return { world, store, sim, snapshot, sweep, proposals };
    });

  it.effect("a reading per sensor per snapshot; a failed sensor is an error row, never a zero", () =>
    Effect.gen(function* () {
      const t = yield* boot();
      t.world.sensed.set("lint@s1", sensed(["dup-code"]));
      yield* t.snapshot("s1");
      const [plan] = yield* t.sweep(sense);
      assert.strictEqual(plan!.wanted, 2);
      const readings = yield* t.sim.reader.find(Reading, "by_sensor", {
        eq: ["repo", "review"],
        gte: 0,
        limit: 10,
      });
      assert.strictEqual(readings.length, 1);
      assert.deepStrictEqual([readings[0]!.failed, readings[0]!.findings.length], [1, 0]);
      assert.include(readings[0]!.error ?? "", "no scripted reading");
      const now = yield* Clock.currentTimeMillis;
      const issues = yield* issuesOf(t.sim.reader, "repo", now);
      assert.deepStrictEqual(
        issues.map((i) => [i.fingerprint, i.sources, i.rate]),
        [["dup-code", ["lint"], 1]],
      );
    }),
  );

  it.effect("two sources agree, the loop acts once, a yes applies it, and the receipt is the history", () =>
    Effect.gen(function* () {
      const t = yield* boot();
      t.world.sensed.set("lint@s1", sensed(["dup-code"]));
      t.world.sensed.set("review@s1", {
        findings: [found("dup-code", { cite: "replay:1" })],
        analyzed: 3,
        excluded: 0,
        failed: 0,
      });
      t.world.changes = { ref: "tide/purge/dup", head: "h1", summary: "+1 -3" };
      yield* t.snapshot("s1");
      yield* t.sweep(sense);
      const [plan] = yield* t.sweep(purge);
      assert.strictEqual(plan!.wanted, 1);
      assert.strictEqual(t.world.acts.length, 1);
      assert.deepStrictEqual(t.world.acts[0]!.sources, ["lint", "review"]);
      const [p] = yield* t.proposals();
      assert.strictEqual(p!.subject, `dup-code@${evidenceHash(["lint", "review"])}`);
      assert.deepStrictEqual(p!.cites, ["fake:tide/purge/dup", "replay:1"]);
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
      // Still covered after the apply: an accepted wave never re-fires on the same evidence.
      assert.strictEqual((yield* t.sweep(purge))[0]!.wanted, 0);
    }),
  );

  it.effect("a dismissal is keyed to the evidence set: the issue returns only when a source joins", () =>
    Effect.gen(function* () {
      const t = yield* boot();
      t.world.sensed.set("lint@s1", sensed(["todo"]));
      t.world.changes = { ref: "tide/purge/todo", head: "h2", summary: "" };
      yield* t.snapshot("s1");
      yield* t.sweep(sense, purge);
      const [p1] = yield* t.proposals();
      assert.isDefined(p1);
      yield* t.sim.command(
        Decide,
        { plant: "repo", loop: "purge", subject: p1!.subject, accept: false, text: "not now" },
        kai,
      );
      // The rate wobbles above the floor on the next snapshot; same evidence, still dismissed.
      t.world.sensed.set("lint@s2", sensed(["todo", "todo"]));
      yield* TestClock.adjust(DAY);
      yield* t.snapshot("s2");
      yield* t.sweep(sense, purge);
      assert.strictEqual((yield* t.proposals()).length, 1);
      // A second source joins: a new subject, a new wave, and the brief carries the rejection text.
      t.world.sensed.set("lint@s3", sensed(["todo"]));
      t.world.sensed.set("review@s3", sensed(["todo"]));
      yield* TestClock.adjust(DAY);
      yield* t.snapshot("s3");
      yield* t.sweep(sense, purge);
      const ps = yield* t.proposals();
      assert.strictEqual(ps.length, 2);
      assert.deepStrictEqual(ps[1]!.sources, ["lint", "review"]);
      assert.strictEqual(t.world.acts.at(-1)!.feedback, "not now");
    }),
  );

  it.effect("the budget caps the heavy day; the next day the second wave goes", () =>
    Effect.gen(function* () {
      const t = yield* boot();
      t.world.sensed.set("lint@s1", sensed(["a", "b"]));
      t.world.sensed.set("review@s1", {
        findings: [found("a"), found("b")],
        analyzed: 1,
        excluded: 0,
        failed: 0,
      });
      t.world.changes = { ref: "tide/purge/x", head: "h3", summary: "" };
      yield* t.snapshot("s1");
      yield* t.sweep(sense, purge);
      assert.strictEqual((yield* t.proposals()).length, 1);
      assert.strictEqual((yield* t.sweep(purge))[0]!.wanted, 0);
      yield* TestClock.adjust(DAY);
      yield* t.sweep(purge);
      assert.strictEqual((yield* t.proposals()).length, 2);
    }),
  );

  it.effect("a model fingerprint outside the vocabulary is `new:` and alone never clears the bar", () =>
    Effect.gen(function* () {
      const t = yield* boot();
      t.world.sensed.set("review@s1", sensed(["weird"]));
      yield* t.snapshot("s1");
      yield* t.sweep(sense, purge);
      const now = yield* Clock.currentTimeMillis;
      const issues = yield* issuesOf(t.sim.reader, "repo", now);
      assert.deepStrictEqual(
        issues.map((i) => i.fingerprint),
        ["new:weird"],
      );
      assert.strictEqual((yield* t.proposals()).length, 0);
    }),
  );

  it.effect("an actuator that changes nothing is a failed attempt, retried, never a proposal", () =>
    Effect.gen(function* () {
      const t = yield* boot();
      t.world.sensed.set("lint@s1", sensed(["dup-code"]));
      t.world.sensed.set("review@s1", sensed(["dup-code"]));
      t.world.changes = null;
      yield* t.snapshot("s1");
      yield* t.sweep(sense, purge);
      assert.strictEqual((yield* t.proposals()).length, 0);
      const receipts = yield* t.sim.reader.find(Kernel.Receipt, "by_key", { eq: [purge.id], limit: 10 });
      assert.deepStrictEqual(
        receipts.map((r) => r.outcome),
        ["failed"],
      );
      assert.include(receipts[0]!.error ?? "", "changed nothing");
    }),
  );

  it.effect("a verdict observed where the plant keeps its gate lands through the observe door", () =>
    Effect.gen(function* () {
      const t = yield* boot();
      t.world.sensed.set("lint@s1", sensed(["dup-code"]));
      t.world.sensed.set("review@s1", sensed(["dup-code"]));
      t.world.changes = { ref: "tide/purge/dup", head: "h4", summary: "" };
      yield* t.snapshot("s1");
      yield* t.sweep(sense, purge);
      const [p] = yield* t.proposals();
      yield* t.sim.entry(observe, {
        verdicts: [{ loop: "purge", subject: p!.subject, accept: true, text: "", cite: "https://pr/1" }],
      });
      const v = yield* t.sim.reader.find(Verdict, "by_key", { eq: ["repo", "purge", p!.subject], limit: 1 });
      assert.deepStrictEqual([v[0]!.accept, v[0]!.cite], [true, "https://pr/1"]);
      yield* t.sweep(apply);
      assert.strictEqual(t.world.applied.length, 1);
    }),
  );
});
