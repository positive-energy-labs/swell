import { execFileSync } from "node:child_process";
import { existsSync, mkdirSync, mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { assert, describe, it } from "@effect/vitest";
import { Context, Effect, Exit, Fiber, Layer, Scope } from "effect";
import { HttpServer } from "effect/http";
import { TestClock } from "effect/testing";
import { makeController } from "../src/controller.ts";
import { defineControl, fakePlant, fakeWorld, Measurement, Proposal, Verdict } from "../src/index.ts";

const git = (cwd: string, ...a: Array<string>) =>
  execFileSync("git", ["-C", cwd, "-c", "user.name=t", "-c", "user.email=t@t", ...a], {
    encoding: "utf8",
  }).trim();

/** A sensor that reports a TODO.md as one signal, and an actuator that deletes it. Both are plain Node scripts in the plant. */
const SENSOR = `const fs = require("node:fs");
process.stdout.write(JSON.stringify({ signals: fs.existsSync("TODO.md") ? [{ signature: "todo-file", mechanism: "todo", path: "TODO.md" }] : [], analyzed: 1, excluded: 0, failed: 0 }));`;
const ACTUATOR = `require("node:fs").unlinkSync("TODO.md");`;
/** A sensor that never returns, so the timeout is the only way out. */
const HANG = `setInterval(() => {}, 1000);`;
/** A sensor that prints something that is not a Measured. */
const GARBAGE = `process.stdout.write('{"signals":[{}],"analyzed":"3"}');`;

const setup = () => {
  const tmp = mkdtempSync(join(tmpdir(), "swell-"));
  const origin = join(tmp, "origin.git");
  const plant = join(tmp, "plant");
  execFileSync("git", ["init", "-q", "--bare", "-b", "main", origin]);
  execFileSync("git", ["init", "-q", "-b", "main", plant]);
  writeFileSync(join(plant, "TODO.md"), "later\n");
  writeFileSync(join(plant, "sensor.cjs"), SENSOR);
  writeFileSync(join(plant, "hang.cjs"), HANG);
  writeFileSync(join(plant, "garbage.cjs"), GARBAGE);
  writeFileSync(join(plant, "act.cjs"), ACTUATOR);
  git(plant, "add", "-A");
  git(plant, "commit", "-q", "-m", "seed");
  git(plant, "remote", "add", "origin", origin);
  git(plant, "push", "-q", "-u", "origin", "main");
  const work = join(tmp, "work");
  mkdirSync(work);
  return { tmp, origin, plant, work };
};

const TOKEN = "t0k";

/** Build the server layer in a scope and read its port back from the HttpServer service. */
const listening = (layer: Layer.Layer<HttpServer.HttpServer, unknown>) =>
  Effect.gen(function* () {
    const scope = yield* Scope.make();
    const ctx = yield* Layer.buildWithScope(layer, scope);
    const address = Context.get(ctx, HttpServer.HttpServer).address;
    const port = "port" in address ? address.port : 0;
    return { base: `http://127.0.0.1:${port}`, close: Scope.close(scope, Exit.void) };
  });

describe("controller over a git plant", () => {
  it("ticks: sample, measurement, move, auto verdict, squash-merge, push; the next tick is quiet; the door", async () => {
    const { tmp, origin, plant, work } = setup();
    const spec = defineControl({
      plant: { id: "p", kind: "git", root: plant, ref: "main", remote: "origin" },
      sensors: [
        { id: "todo", run: [process.execPath, "sensor.cjs"] },
        { id: "hang", run: [process.execPath, "hang.cjs"], timeoutMs: 300 },
        { id: "garbage", run: [process.execPath, "garbage.cjs"] },
      ],
      actuators: [{ id: "rm", run: [process.execPath, "act.cjs"] }],
      loops: [
        {
          id: "clean",
          inputs: ["todo"],
          actuator: "rm",
          mode: "auto",
          operator: "kai",
          threshold: (s) => s.hits > 0,
        },
      ],
    });
    const c = makeController({
      name: "test",
      db: join(tmp, "historian.sqlite"),
      work,
      specs: [spec],
      token: TOKEN,
    });
    try {
      await Effect.runPromise(c.tick);

      const measurements = (instrument: string) =>
        Effect.runPromise(
          c.sim.reader.find(Measurement, "by_instrument", { eq: ["p", instrument], gte: 0, limit: 10 }),
        );
      assert.deepStrictEqual(
        (await measurements("todo")).map((m) => m.signals.map((s) => s.signature)),
        [["todo-file"]],
      );
      // A hung sensor and a malformed one are failed measurements with a reason, never zeros and never a wedged tick.
      const hung = (await measurements("hang"))[0]!;
      assert.deepStrictEqual([hung.failed, hung.signals.length], [1, 0]);
      assert.include(hung.error ?? "", "timed out");
      assert.include((await measurements("garbage"))[0]!.error ?? "", "not a Measured");

      const [proposal] = await Effect.runPromise(
        c.sim.reader.find(Proposal, "by_loop", { eq: ["p", "clean"], gte: 0, limit: 10 }),
      );
      assert.isDefined(proposal);
      assert.match(proposal!.cites[0]!, /^git:swell\/clean\/todo-file-/);
      const [verdict] = await Effect.runPromise(
        c.sim.reader.find(Verdict, "by_plant", { eq: ["p"], gte: 0, limit: 10 }),
      );
      assert.deepStrictEqual([verdict!.accept, verdict!.text], [true, "auto: clean is in auto mode"]);

      // Applied: the plant's main moved by one squash commit, origin has it, and TODO.md is gone there.
      assert.match(git(plant, "log", "--oneline", "-1"), /swell: swell\/clean\/todo-file-/);
      assert.strictEqual(git(origin, "rev-parse", "main"), git(plant, "rev-parse", "HEAD"));
      assert.notInclude(git(origin, "ls-tree", "--name-only", "main"), "TODO.md");
      assert.isTrue(existsSync(join(work, "act", "p", "clean", "sensor.cjs")));

      // Second tick: new sample, a clean measurement, no second move.
      await Effect.runPromise(c.tick);
      const after = await Effect.runPromise(
        c.sim.reader.find(Proposal, "by_loop", { eq: ["p", "clean"], gte: 0, limit: 10 }),
      );
      assert.strictEqual(after.length, 1);
      const health = await Effect.runPromise(c.sim.health);
      // A plan is current wants, not history: the newest sample is measured, the signature is covered.
      assert.deepStrictEqual(
        health.map((h) => [h.rule, h.done, h.pending.length, h.dead.length]),
        [
          ["control::p-measure", 3, 0, 0],
          ["control::p-clean", 0, 0, 0],
          ["control::p-apply", 1, 0, 0],
        ],
      );

      // The HMI, the peer door, and the operator's decide verb, over a real socket.
      const { base, close } = await Effect.runPromise(listening(c.serve(0)));
      try {
        const view = (await (await fetch(`${base}/view?plant=p`)).json()) as {
          proposals: Array<{ verdict: { accept: boolean } | null }>;
        };
        assert.strictEqual(view.proposals[0]!.verdict?.accept, true);
        assert.include(await (await fetch(base)).text(), '<table id="proposals">');
        const auth = { authorization: `Bearer ${TOKEN}` };
        const facts = `${base}/facts/control_measurement`;
        assert.strictEqual((await fetch(`${facts}?index=by_instrument`)).status, 401);
        assert.strictEqual(
          (await fetch(`${facts}?index=by_instrument&limit=-5`, { headers: auth })).status,
          400,
        );
        assert.strictEqual((await fetch(`${facts}?index=nope`, { headers: auth })).status, 400);
        const rows = (await (
          await fetch(`${facts}?index=by_instrument&eq=${encodeURIComponent('["p","todo"]')}&limit=10`, {
            headers: auth,
          })
        ).json()) as Array<{ sample: string }>;
        assert.strictEqual(rows.length, 2);
        const decide = (body: unknown, headers: Record<string, string> = auth) =>
          fetch(`${base}/decide`, {
            method: "POST",
            headers: { ...headers, "content-type": "application/json" },
            body: JSON.stringify(body),
          });
        const yes = { plant: "p", loop: "clean", subject: "x", accept: true, text: "", operator: "kai" };
        assert.strictEqual((await decide(yes, {})).status, 401);
        assert.strictEqual((await decide({ ...yes, accept: "false" })).status, 400);
        assert.strictEqual((await decide(yes)).status, 422);
      } finally {
        await Effect.runPromise(close);
      }
    } finally {
      c.close();
    }
  }, 60_000);

  it.effect("a dead plant skips its tick and the loop keeps ticking", () =>
    Effect.gen(function* () {
      const world = fakeWorld();
      world.sampleFails = true;
      const spec = defineControl({
        plant: { id: "dead", kind: "git", root: "/nowhere", ref: "main" },
        sensors: [{ id: "s", run: ["x"] }],
        actuators: [{ id: "a", run: ["x"] }],
        loops: [],
      });
      const c = makeController({
        name: "t",
        db: ":memory:",
        work: "/nowhere",
        specs: [spec],
        token: TOKEN,
        plantLayer: fakePlant(world),
      });
      const fiber = yield* Effect.forkChild(c.tick.pipe(Effect.andThen(c.run(60_000))));
      yield* TestClock.adjust(1);
      yield* TestClock.adjust(180_000);
      yield* Fiber.interrupt(fiber);
      const health = yield* c.sim.health;
      // The first tick survived a failing sample; nothing was wanted because no sample landed.
      assert.deepStrictEqual(
        health.filter((h) => h.rule === "control::dead-measure").map((h) => [h.enabled, h.wanted]),
        [[true, 0]],
      );
      c.close();
    }),
  );
});
