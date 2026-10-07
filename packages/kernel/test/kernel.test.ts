import { assert, describe, it } from "@effect/vitest";
import { Context, Effect, Layer, Schema } from "effect";
import { TestClock } from "effect/testing";
import {
  Command,
  DuplicateId,
  Fact,
  InvalidId,
  Kernel,
  Memory,
  Port,
  Rule,
  fnName,
  violation,
} from "../src/index.ts";

const meta = {
  owner: "test",
  audience: "dev",
  layer: "core",
  ruled: false,
  open: "test fixture",
  label: "fixture",
  plain: "A test fixture.",
} as const;

// A fake external system with a failure knob, so failures are scripted rather than random.
class Folders extends Context.Service<
  Folders,
  { readonly ensure: (key: string) => Effect.Effect<string, Error> }
>()("test/Folders") {}
const world = { folders: new Map<string, string>(), failNext: 0, killNext: 0, spans: [] as Array<string> };
const fake = Layer.succeed(Folders, {
  ensure: (key) =>
    Effect.gen(function* () {
      const span = yield* Effect.currentSpan;
      world.spans.push(span.traceId);
      if (world.failNext > 0) {
        world.failNext--;
        return yield* Effect.fail(new Error("drive 503"));
      }
      const id = world.folders.get(key) ?? `f${world.folders.size + 1}`;
      world.folders.set(key, id);
      if (world.killNext > 0) {
        world.killNext--;
        return yield* Effect.interrupt;
      }
      return id;
    }),
});
const FoldersPort = Port.make({ id: "test::folders", service: Folders, live: fake, fake, meta });

const Signed = Fact.make({
  id: "test::signed",
  class: "event",
  fields: { name: Schema.String },
  indexes: { by_at: ["at"] },
  meta,
});
const Linked = Fact.make({
  id: "test::linked",
  class: "inferred",
  fields: { signed: Schema.String, folder: Schema.String },
  meta,
});

const Sign = Command.make({
  id: "test::sign",
  role: "partner",
  args: { name: Schema.String },
  returns: Schema.String,
  writes: [Signed],
  run: ({ name }, { db }) =>
    name === "" ? Effect.fail(violation("named", "a signing needs a name")) : db.append(Signed, { name }),
  meta,
});
const Enable = Command.make({
  id: "test::enable",
  role: "partner",
  args: { rule: Schema.String },
  returns: Schema.String,
  writes: [Kernel.RuleEnabled],
  run: ({ rule }, { db }) => db.append(Kernel.RuleEnabled, { rule }),
  meta,
});

const FolderRule = Rule.make({
  id: "test::folder",
  reads: [Signed],
  uses: [FoldersPort],
  writes: [Linked],
  triggers: [Rule.onFact(Signed)],
  subject: Schema.Struct({ urn: Schema.String }),
  maxAttempts: 3,
  leaseMs: 60_000,
  want: (db, { enabledAt }) =>
    db
      .find(Signed, "by_at", { gte: enabledAt, limit: 100 })
      .pipe(Effect.map((rows) => rows.map((r) => ({ urn: `tc:${r._id}` })))),
  effect: ({ urn }) =>
    Effect.gen(function* () {
      const folders = yield* Folders;
      const folder = yield* folders.ensure(urn);
      return { result: `drive:${folder}`, append: [{ fact: Linked, draft: { signed: urn, folder } }] };
    }),
  meta,
});

const partner = { by: "person:p1", person: "p1", roles: new Set(["partner"]) };

describe("registry", () => {
  it("rejects a duplicate id at boot", () => {
    assert.throws(() => Fact.make({ id: "test::signed", class: "event", fields: {}, meta }), DuplicateId);
  });
  it("rejects an id that is not namespace::kebab", () => {
    assert.throws(() => Fact.make({ id: "Bad Id", class: "event", fields: {}, meta }), InvalidId);
  });
  it("derives the Convex function name from the id", () => {
    assert.strictEqual(fnName("time::submit-week"), "submitWeek");
  });
  it("marks a stub body as a stub", () => {
    assert.strictEqual(Sign.impl, "real");
  });
});

describe("level-triggered rules", () => {
  it.effect("a failed call leaves a failed receipt; the next sweep retries without a duplicate", () =>
    Effect.gen(function* () {
      Object.assign(world, { folders: new Map(), failNext: 1, killNext: 0, spans: [] });
      const sim = Memory.simulator();
      yield* sim.command(Enable, { rule: FolderRule.id }, partner);
      yield* TestClock.adjust(1000);
      yield* sim.command(Sign, { name: "2026-014" }, partner);

      const first = yield* sim.sweep(FolderRule);
      assert.strictEqual(first.pending.length, 1);
      // A second sweep before the effect runs sees the attempt in flight and schedules nothing.
      assert.strictEqual((yield* sim.sweep(FolderRule)).inflight, 1);
      yield* sim.drain;
      const receipts = () => sim.tables.get(Kernel.Receipt.table) ?? [];
      assert.deepStrictEqual(
        receipts().map((r) => r.outcome),
        ["failed"],
      );

      yield* sim.sweep(FolderRule);
      yield* sim.drain;
      assert.deepStrictEqual(
        receipts().map((r) => r.outcome),
        ["failed", "ok"],
      );
      assert.strictEqual(world.folders.size, 1);
      assert.strictEqual(sim.tables.get(Linked.table)?.length, 1);

      const third = yield* sim.sweep(FolderRule);
      assert.deepStrictEqual([third.done, third.pending.length], [1, 0]);
    }),
  );

  it.effect("a worker killed after Drive answered is retried after the lease, still one folder", () =>
    Effect.gen(function* () {
      Object.assign(world, { folders: new Map(), failNext: 0, killNext: 1, spans: [] });
      const sim = Memory.simulator();
      yield* sim.command(Enable, { rule: FolderRule.id }, partner);
      yield* TestClock.adjust(1000);
      yield* sim.command(Sign, { name: "2026-015" }, partner);
      yield* sim.sweep(FolderRule);
      yield* sim.drain;
      assert.strictEqual((sim.tables.get(Kernel.Receipt.table) ?? []).length, 0);
      assert.strictEqual((yield* sim.sweep(FolderRule)).inflight, 1);

      yield* TestClock.adjust(FolderRule.leaseMs + 1);
      yield* sim.sweep(FolderRule);
      yield* sim.drain;
      assert.deepStrictEqual(
        sim.tables.get(Kernel.Receipt.table)?.map((r) => r.outcome),
        ["ok"],
      );
      assert.strictEqual(world.folders.size, 1);
    }),
  );

  it.effect("after maxAttempts the subject is dead-lettered until a retry is granted", () =>
    Effect.gen(function* () {
      Object.assign(world, { folders: new Map(), failNext: 99, killNext: 0, spans: [] });
      const sim = Memory.simulator();
      yield* sim.command(Enable, { rule: FolderRule.id }, partner);
      yield* TestClock.adjust(1000);
      yield* sim.command(Sign, { name: "2026-016" }, partner);
      for (let i = 0; i < 4; i++) {
        yield* sim.sweep(FolderRule);
        yield* sim.drain;
      }
      const plan = yield* sim.sweep(FolderRule);
      assert.deepStrictEqual([plan.pending.length, plan.dead.length, plan.dead[0]?.failures], [0, 1, 3]);
      assert.include(plan.dead[0]?.error ?? "", "drive 503");
    }),
  );

  it.effect("imported history is inert: nothing before the enable fact is wanted", () =>
    Effect.gen(function* () {
      const sim = Memory.simulator();
      yield* sim.command(Sign, { name: "legacy" }, partner);
      assert.isFalse((yield* sim.sweep(FolderRule)).enabled);
      yield* TestClock.adjust(1000);
      yield* sim.command(Enable, { rule: FolderRule.id }, partner);
      assert.strictEqual((yield* sim.sweep(FolderRule)).wanted, 0);
    }),
  );

  it.effect("the traceparent crosses the scheduler hop: attempt, effect and receipt share one trace", () =>
    Effect.gen(function* () {
      Object.assign(world, { folders: new Map(), failNext: 0, killNext: 0, spans: [] });
      const sim = Memory.simulator();
      yield* sim.command(Enable, { rule: FolderRule.id }, partner);
      yield* TestClock.adjust(1000);
      yield* sim.command(Sign, { name: "2026-017" }, partner);
      yield* sim.sweep(FolderRule);
      yield* sim.drain;
      const traceOf = (tp: unknown) => String(tp).split("-")[1];
      const attempt = sim.tables.get(Kernel.Attempt.table)![0]!;
      const receipt = sim.tables.get(Kernel.Receipt.table)![0]!;
      assert.isDefined(attempt.trace);
      assert.strictEqual(traceOf(receipt.trace), traceOf(attempt.trace));
      assert.strictEqual(world.spans[0], traceOf(attempt.trace));
    }),
  );

  it.effect("a fact append kicks the rules that trigger on it", () =>
    Effect.gen(function* () {
      const sim = Memory.simulator();
      yield* sim.command(Sign, { name: "kick" }, partner);
      assert.include(sim.kicks, FolderRule.id);
    }),
  );

  it.effect("a command refuses a broken invariant with a tagged error", () =>
    Effect.gen(function* () {
      const sim = Memory.simulator();
      const err = yield* Effect.flip(sim.command(Sign, { name: "" }, partner));
      assert.strictEqual(err._tag, "InvariantViolation");
    }),
  );
});
