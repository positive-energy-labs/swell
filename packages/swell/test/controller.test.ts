import { execFileSync } from "node:child_process";
import { chmodSync, existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";
import { assert, describe, it } from "@effect/vitest";
import { Kernel } from "@swell/kernel";
import { Context, Effect, Exit, Fiber, Layer, Scope } from "effect";
import { HttpServer } from "effect/http";
import { TestClock } from "effect/testing";
import { makeController } from "../src/controller.ts";
import {
  type ControlSpec,
  defineControl,
  fakePlant,
  fakeWorld,
  Measurement,
  Proposal,
  type Signature,
  Verdict,
} from "../src/index.ts";

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

/** An actuator that does half its work, prints, and dies on its first run; on later runs it finishes. It records what each run saw outside the worktree. */
const flaky = (record: string) => `const fs = require("node:fs");
const record = ${JSON.stringify(record)};
const runs = fs.existsSync(record) ? JSON.parse(fs.readFileSync(record, "utf8")) : [];
const brief = JSON.parse(fs.readFileSync(process.env.SWELL_BRIEF, "utf8"));
runs.push({ partial: fs.existsSync("partial.txt"), previous: brief.previous });
fs.writeFileSync(record, JSON.stringify(runs));
if (runs.length === 1) {
  fs.writeFileSync("partial.txt", "half\\n");
  console.log("did half");
  console.error("then died");
  process.exit(1);
}
fs.writeFileSync("done.txt", "rest\\n");`;
/** An actuator that leaves a file, prints, and never exits. */
const STALL = `require("node:fs").writeFileSync("partial.txt", "x");
process.stdout.write("hello\\n");
setInterval(() => {}, 1000);`;
/** An actuator that counts its runs outside the worktree and makes a change. */
const counting = (counter: string) => `const fs = require("node:fs");
fs.appendFileSync(${JSON.stringify(counter)}, "x");
fs.writeFileSync("out.txt", "made\\n");`;

const setup = (extra: (tmp: string) => Record<string, string> = () => ({})) => {
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
  for (const [name, body] of Object.entries(extra(tmp))) writeFileSync(join(plant, name), body);
  git(plant, "add", "-A");
  git(plant, "commit", "-q", "-m", "seed");
  git(plant, "remote", "add", "origin", origin);
  git(plant, "push", "-q", "-u", "origin", "main");
  const work = join(tmp, "work");
  mkdirSync(work);
  return { tmp, origin, plant, work };
};

const TOKEN = "t0k";

/** A threshold that moves on the first signal, so a test need not wait three runs. */
const once = (s: Signature) => s.hits > 0;

const open = (tmp: string, work: string, spec: ControlSpec, gh = false) =>
  makeController({ name: "test", db: join(tmp, "historian.sqlite"), work, specs: [spec], token: TOKEN, gh });

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
    const seed = git(plant, "rev-parse", "HEAD");
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

      // Applied by push: origin's main moved by one squash commit and TODO.md is gone there, while the plant's own main and tree stay put.
      assert.match(git(origin, "log", "--oneline", "-1", "main"), /swell: swell\/clean\/todo-file-/);
      assert.strictEqual(git(origin, "rev-list", "--count", "main"), "2");
      assert.notInclude(git(origin, "ls-tree", "--name-only", "main"), "TODO.md");
      assert.strictEqual(git(plant, "rev-parse", "HEAD"), seed);
      assert.isTrue(existsSync(join(plant, "TODO.md")));
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
      rmSync(tmp, { recursive: true, force: true, maxRetries: 5 });
    }
  }, 60_000);

  it("apply is push-only: a plant root on another branch with staged, unstaged and untracked work is never touched", async () => {
    const { tmp, origin, plant, work } = setup();
    git(plant, "checkout", "-q", "-b", "side");
    writeFileSync(join(plant, "scratch.txt"), "untracked\n");
    writeFileSync(join(plant, "staged.txt"), "staged\n");
    git(plant, "add", "staged.txt");
    writeFileSync(join(plant, "sensor.cjs"), `${SENSOR}\n// edited, not committed\n`);
    const standing = () => ({
      head: git(plant, "rev-parse", "HEAD"),
      branch: git(plant, "rev-parse", "--abbrev-ref", "HEAD"),
      status: git(plant, "status", "--porcelain"),
      staged: git(plant, "diff", "--cached", "--name-only"),
      scratch: readFileSync(join(plant, "scratch.txt"), "utf8"),
      sensor: readFileSync(join(plant, "sensor.cjs"), "utf8"),
    });
    const before = standing();
    assert.strictEqual(before.branch, "side");
    const c = open(
      tmp,
      work,
      defineControl({
        plant: { id: "push", kind: "git", root: plant, ref: "main", remote: "origin" },
        sensors: [{ id: "todo", run: [process.execPath, "sensor.cjs"] }],
        actuators: [{ id: "rm", run: [process.execPath, "act.cjs"] }],
        loops: [
          { id: "clean", inputs: ["todo"], actuator: "rm", mode: "auto", operator: "kai", threshold: once },
        ],
      }),
    );
    try {
      await Effect.runPromise(c.tick);
      assert.match(git(origin, "log", "--oneline", "-1", "main"), /swell: swell\/clean\/todo-file-/);
      assert.notInclude(git(origin, "ls-tree", "--name-only", "main"), "TODO.md");
      assert.deepStrictEqual(standing(), before);
    } finally {
      c.close();
      rmSync(tmp, { recursive: true, force: true, maxRetries: 5 });
    }
  }, 60_000);

  it("a rejected push is a failed apply, and a later sweep applies against a fresh fetch", async () => {
    const { tmp, origin, plant, work } = setup();
    const reject = join(tmp, "reject").replaceAll("\\", "/");
    writeFileSync(join(reject), "");
    const hook = join(origin, "hooks", "pre-receive");
    writeFileSync(
      hook,
      `#!/bin/sh\nif [ -f "${reject}" ]; then echo "rejected by test hook" >&2; exit 1; fi\n`,
    );
    chmodSync(hook, 0o755);
    const c = open(
      tmp,
      work,
      defineControl({
        plant: { id: "reject", kind: "git", root: plant, ref: "main", remote: "origin" },
        sensors: [{ id: "todo", run: [process.execPath, "sensor.cjs"] }],
        actuators: [{ id: "rm", run: [process.execPath, "act.cjs"] }],
        loops: [
          { id: "clean", inputs: ["todo"], actuator: "rm", mode: "auto", operator: "kai", threshold: once },
        ],
      }),
    );
    try {
      const before = git(origin, "rev-parse", "main");
      await Effect.runPromise(c.tick);
      assert.strictEqual(git(origin, "rev-parse", "main"), before);
      const receipts = () =>
        Effect.runPromise(
          c.sim.reader.find(Kernel.Receipt, "by_key", { eq: ["control::reject-apply"], limit: 10 }),
        );
      const failed = await receipts();
      assert.deepStrictEqual(
        failed.map((r) => r.outcome),
        ["failed"],
      );
      assert.include(failed[0]!.error ?? "", "rejected by test hook");
      rmSync(join(tmp, "reject"));
      await Effect.runPromise(c.tick);
      assert.notStrictEqual(git(origin, "rev-parse", "main"), before);
      assert.notInclude(git(origin, "ls-tree", "--name-only", "main"), "TODO.md");
      assert.deepStrictEqual(
        (await receipts()).map((r) => r.outcome),
        ["failed", "ok"],
      );
    } finally {
      c.close();
      rmSync(tmp, { recursive: true, force: true, maxRetries: 5 });
    }
  }, 60_000);

  it("a failed actuator leaves its work on the move branch and a log; the next attempt resumes it and reads why it stopped", async () => {
    const { tmp, plant, work } = setup((t) => ({ "flaky.cjs": flaky(join(t, "record.json")) }));
    const c = open(
      tmp,
      work,
      defineControl({
        plant: { id: "flaky", kind: "git", root: plant, ref: "main", remote: "origin" },
        sensors: [{ id: "todo", run: [process.execPath, "sensor.cjs"] }],
        actuators: [{ id: "flaky", run: [process.execPath, "flaky.cjs"] }],
        loops: [
          {
            id: "clean",
            inputs: ["todo"],
            actuator: "flaky",
            mode: "manual",
            operator: "kai",
            threshold: once,
          },
        ],
      }),
    );
    try {
      await Effect.runPromise(c.tick);
      const receipts = () =>
        Effect.runPromise(
          c.sim.reader.find(Kernel.Receipt, "by_key", { eq: ["control::flaky-clean"], limit: 10 }),
        );
      const [first] = await receipts();
      assert.strictEqual(first!.outcome, "failed");
      const branch = /branch (swell\/clean\/todo-file-\w+)/.exec(first!.error ?? "")?.[1];
      const logPath = /log (\S+\.log)/.exec(first!.error ?? "")?.[1];
      assert.isDefined(branch);
      assert.isDefined(logPath);
      assert.strictEqual(
        git(plant, "log", "-1", "--format=%s", branch!),
        "swell: wip clean todo-file after exit 1",
      );
      assert.include(git(plant, "ls-tree", "-r", "--name-only", branch!), "partial.txt");
      assert.match(basename(logPath!), /^flaky-clean-todo-file-\w{7}-.+\.log$/);
      const log = readFileSync(logPath!, "utf8");
      assert.include(log, "did half");
      assert.include(log, "then died");
      assert.isBelow(Buffer.byteLength(log), 64 * 1024);

      await Effect.runPromise(c.tick);
      const runs = JSON.parse(readFileSync(join(tmp, "record.json"), "utf8")) as Array<{
        partial: boolean;
        previous: string;
      }>;
      assert.strictEqual(runs.length, 2);
      assert.deepStrictEqual([runs[0]!.partial, runs[0]!.previous], [false, ""]);
      assert.isTrue(runs[1]!.partial);
      assert.include(runs[1]!.previous, branch!);
      assert.include(runs[1]!.previous, logPath!);
      // The move is the resumed work plus what the second run added, finished as one completed commit.
      assert.match(git(plant, "log", "-1", "--format=%s", branch!), /^swell: clean todo-file at \w{7}$/);
      const tree = git(plant, "ls-tree", "-r", "--name-only", branch!);
      assert.include(tree, "partial.txt");
      assert.include(tree, "done.txt");
      const [proposal] = await Effect.runPromise(
        c.sim.reader.find(Proposal, "by_loop", { eq: ["flaky", "clean"], gte: 0, limit: 10 }),
      );
      assert.match(proposal!.cites[0]!, /^git:swell\/clean\/todo-file-/);
      assert.deepStrictEqual(
        (await receipts()).map((r) => r.outcome),
        ["failed", "ok"],
      );
    } finally {
      c.close();
      rmSync(tmp, { recursive: true, force: true, maxRetries: 5 });
    }
  }, 60_000);

  it("a timed-out actuator keeps the work it left and the output it printed", async () => {
    const { tmp, plant, work } = setup(() => ({ "stall.cjs": STALL }));
    const c = open(
      tmp,
      work,
      defineControl({
        plant: { id: "stall", kind: "git", root: plant, ref: "main", remote: "origin" },
        sensors: [{ id: "todo", run: [process.execPath, "sensor.cjs"] }],
        actuators: [{ id: "stall", run: [process.execPath, "stall.cjs"], timeoutMs: 3_000 }],
        loops: [
          {
            id: "clean",
            inputs: ["todo"],
            actuator: "stall",
            mode: "manual",
            operator: "kai",
            threshold: once,
          },
        ],
      }),
    );
    try {
      await Effect.runPromise(c.tick);
      const [r] = await Effect.runPromise(
        c.sim.reader.find(Kernel.Receipt, "by_key", { eq: ["control::stall-clean"], limit: 10 }),
      );
      assert.strictEqual(r!.outcome, "failed");
      assert.include(r!.error ?? "", "timed out after 3000 ms");
      const branch = /branch (swell\/clean\/todo-file-\w+)/.exec(r!.error ?? "")?.[1];
      const logPath = /log (\S+\.log)/.exec(r!.error ?? "")?.[1];
      assert.strictEqual(
        git(plant, "log", "-1", "--format=%s", branch!),
        "swell: wip clean todo-file after timeout",
      );
      assert.include(readFileSync(logPath!, "utf8"), "hello");
    } finally {
      c.close();
      rmSync(tmp, { recursive: true, force: true, maxRetries: 5 });
    }
  }, 60_000);

  it("an actuator that succeeded is never rerun when propose fails, however many attempts", async () => {
    const { tmp, plant, work } = setup((t) => ({ "count.cjs": counting(join(t, "count")) }));
    // Fetch works; every push fails, as when the host is unreachable. `gh` is never reached.
    git(plant, "remote", "set-url", "--push", "origin", join(tmp, "nowhere.git"));
    const c = open(
      tmp,
      work,
      defineControl({
        plant: { id: "count", kind: "git", root: plant, ref: "main", remote: "origin" },
        sensors: [{ id: "todo", run: [process.execPath, "sensor.cjs"] }],
        actuators: [{ id: "count", run: [process.execPath, "count.cjs"] }],
        loops: [
          {
            id: "clean",
            inputs: ["todo"],
            actuator: "count",
            mode: "manual",
            operator: "kai",
            threshold: once,
          },
        ],
      }),
      true,
    );
    try {
      await Effect.runPromise(c.tick);
      await Effect.runPromise(c.tick);
      assert.strictEqual(readFileSync(join(tmp, "count"), "utf8"), "x");
      // Both attempts failed at the push (the actuator ran once, counted outside the worktree), and no proposal was written.
      const receipts = await Effect.runPromise(
        c.sim.reader.find(Kernel.Receipt, "by_key", { eq: ["control::count-clean"], limit: 10 }),
      );
      assert.deepStrictEqual(
        receipts.map((r) => r.outcome),
        ["failed", "failed"],
      );
      for (const r of receipts) assert.include(r.error ?? "", "nowhere.git");
      assert.strictEqual(
        (
          await Effect.runPromise(
            c.sim.reader.find(Proposal, "by_loop", { eq: ["count", "clean"], gte: 0, limit: 10 }),
          )
        ).length,
        0,
      );
    } finally {
      c.close();
      rmSync(tmp, { recursive: true, force: true, maxRetries: 5 });
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
