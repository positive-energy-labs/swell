import { execFileSync } from "node:child_process";
import { existsSync, mkdirSync, mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { assert, describe, it } from "@effect/vitest";
import { Context, Effect, Exit, Fiber, Layer, Scope } from "effect";
import { HttpServer } from "effect/http";
import { TestClock } from "effect/testing";
import { makeHost } from "../src/host.ts";
import { defineTide, fakePlant, fakeWorld, Proposal, Reading, Verdict } from "../src/index.ts";

const git = (cwd: string, ...a: Array<string>) =>
  execFileSync("git", ["-C", cwd, "-c", "user.name=t", "-c", "user.email=t@t", ...a], {
    encoding: "utf8",
  }).trim();

/** A sensor that reports a TODO.md as one finding, and an actuator that deletes it. Both are plain Node scripts in the plant. */
const SENSOR = `const fs = require("node:fs");
process.stdout.write(JSON.stringify({ findings: fs.existsSync("TODO.md") ? [{ fingerprint: "todo-file", mechanism: "todo", path: "TODO.md" }] : [], analyzed: 1, excluded: 0, failed: 0 }));`;
const ACTUATOR = `require("node:fs").unlinkSync("TODO.md");`;
/** A sensor that never returns, so the timeout is the only way out. */
const HANG = `setInterval(() => {}, 1000);`;
/** A sensor that prints something that is not a Sensed. */
const GARBAGE = `process.stdout.write('{"findings":[{}],"analyzed":"3"}');`;

const setup = () => {
  const tmp = mkdtempSync(join(tmpdir(), "tide-"));
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

describe("host over a git plant", () => {
  it("ticks: snapshot, reading, wave, policy verdict, squash-merge, push; the next tick is quiet; the door", async () => {
    const { tmp, origin, plant, work } = setup();
    const tide = defineTide({
      plant: { id: "p", kind: "git", root: plant, ref: "main", remote: "origin" },
      sensors: [
        { id: "todo", kind: "measured", run: [process.execPath, "sensor.cjs"] },
        { id: "hang", kind: "measured", run: [process.execPath, "hang.cjs"], timeoutMs: 300 },
        { id: "garbage", kind: "measured", run: [process.execPath, "garbage.cjs"] },
      ],
      actuators: [{ id: "rm", run: [process.execPath, "act.cjs"] }],
      loops: [
        { id: "clean", sense: ["todo"], act: "rm", gate: "auto", person: "kai", when: (i) => i.hits > 0 },
      ],
    });
    const host = makeHost({ name: "test", db: join(tmp, "tide.sqlite"), work, tides: [tide], token: TOKEN });
    try {
      await Effect.runPromise(host.tick);

      const reading = (sensor: string) =>
        Effect.runPromise(
          host.sim.reader.find(Reading, "by_sensor", { eq: ["p", sensor], gte: 0, limit: 10 }),
        );
      assert.deepStrictEqual(
        (await reading("todo")).map((r) => r.findings.map((f) => f.fingerprint)),
        [["todo-file"]],
      );
      // A hung sensor and a malformed one are failed readings with a reason, never zeros and never a wedged tick.
      const hung = (await reading("hang"))[0]!;
      assert.deepStrictEqual([hung.failed, hung.findings.length], [1, 0]);
      assert.include(hung.error ?? "", "timed out");
      assert.include((await reading("garbage"))[0]!.error ?? "", "not a Sensed");

      const [proposal] = await Effect.runPromise(
        host.sim.reader.find(Proposal, "by_loop", { eq: ["p", "clean"], gte: 0, limit: 10 }),
      );
      assert.isDefined(proposal);
      assert.match(proposal!.cites[0]!, /^git:tide\/clean\/todo-file-/);
      const [verdict] = await Effect.runPromise(
        host.sim.reader.find(Verdict, "by_plant", { eq: ["p"], gte: 0, limit: 10 }),
      );
      assert.deepStrictEqual([verdict!.accept, verdict!.text], [true, "policy: clean is pre-approved"]);

      // Applied: the plant's main moved by one squash commit, origin has it, and TODO.md is gone there.
      assert.match(git(plant, "log", "--oneline", "-1"), /tide: tide\/clean\/todo-file-/);
      assert.strictEqual(git(origin, "rev-parse", "main"), git(plant, "rev-parse", "HEAD"));
      assert.notInclude(git(origin, "ls-tree", "--name-only", "main"), "TODO.md");
      assert.isTrue(existsSync(join(work, "act", "p", "clean", "sensor.cjs")));

      // Second tick: new snapshot, a clean reading, no second wave.
      await Effect.runPromise(host.tick);
      const after = await Effect.runPromise(
        host.sim.reader.find(Proposal, "by_loop", { eq: ["p", "clean"], gte: 0, limit: 10 }),
      );
      assert.strictEqual(after.length, 1);
      const health = await Effect.runPromise(host.sim.health);
      // A plan is current wants, not history: the newest snapshot is sensed, the fingerprint is covered.
      assert.deepStrictEqual(
        health.map((h) => [h.rule, h.done, h.pending.length, h.dead.length]),
        [
          ["tide::p-sense", 3, 0, 0],
          ["tide::p-clean", 0, 0, 0],
          ["tide::p-apply", 1, 0, 0],
        ],
      );

      // The page, the peer door, and the decide verb, over a real socket.
      const { base, close } = await Effect.runPromise(listening(host.serve(0)));
      try {
        const view = (await (await fetch(`${base}/view?plant=p`)).json()) as {
          proposals: Array<{ verdict: { accept: boolean } | null }>;
        };
        assert.strictEqual(view.proposals[0]!.verdict?.accept, true);
        assert.include(await (await fetch(base)).text(), '<table id="proposals">');
        const auth = { authorization: `Bearer ${TOKEN}` };
        assert.strictEqual((await fetch(`${base}/facts/tide_reading?index=by_sensor`)).status, 401);
        assert.strictEqual(
          (await fetch(`${base}/facts/tide_reading?index=by_sensor&limit=-5`, { headers: auth })).status,
          400,
        );
        assert.strictEqual(
          (await fetch(`${base}/facts/tide_reading?index=nope`, { headers: auth })).status,
          400,
        );
        const rows = (await (
          await fetch(
            `${base}/facts/tide_reading?index=by_sensor&eq=${encodeURIComponent('["p","todo"]')}&limit=10`,
            { headers: auth },
          )
        ).json()) as Array<{ snapshot: string }>;
        assert.strictEqual(rows.length, 2);
        const decide = (body: unknown, headers: Record<string, string> = auth) =>
          fetch(`${base}/decide`, {
            method: "POST",
            headers: { ...headers, "content-type": "application/json" },
            body: JSON.stringify(body),
          });
        assert.strictEqual(
          (
            await decide(
              { plant: "p", loop: "clean", subject: "x", accept: true, text: "", person: "kai" },
              {},
            )
          ).status,
          401,
        );
        assert.strictEqual(
          (
            await decide({
              plant: "p",
              loop: "clean",
              subject: "x",
              accept: "false",
              text: "",
              person: "kai",
            })
          ).status,
          400,
        );
        assert.strictEqual(
          (await decide({ plant: "p", loop: "clean", subject: "x", accept: true, text: "", person: "kai" }))
            .status,
          422,
        );
      } finally {
        await Effect.runPromise(close);
      }
    } finally {
      host.close();
    }
  }, 60_000);

  it.effect("a dead plant skips its tick and the loop keeps ticking", () =>
    Effect.gen(function* () {
      const world = fakeWorld();
      world.headFails = true;
      const tide = defineTide({
        plant: { id: "dead", kind: "git", root: "/nowhere", ref: "main" },
        sensors: [{ id: "s", kind: "measured", run: ["x"] }],
        actuators: [{ id: "a", run: ["x"] }],
        loops: [],
      });
      const host = makeHost({
        name: "t",
        db: ":memory:",
        work: "/nowhere",
        tides: [tide],
        token: TOKEN,
        plant: fakePlant(world),
      });
      const fiber = yield* Effect.forkChild(host.tick.pipe(Effect.andThen(host.run(60_000))));
      yield* TestClock.adjust(1);
      yield* TestClock.adjust(180_000);
      yield* Fiber.interrupt(fiber);
      const health = yield* host.sim.health;
      // The first tick survived a failing head; nothing was wanted because no snapshot landed.
      assert.deepStrictEqual(
        health.filter((h) => h.rule === "tide::dead-sense").map((h) => [h.enabled, h.wanted]),
        [[true, 0]],
      );
      host.close();
    }),
  );
});
