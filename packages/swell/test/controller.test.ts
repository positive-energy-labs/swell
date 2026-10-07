import { execFileSync } from "node:child_process";
import { chmodSync, existsSync, readFileSync, statSync, writeFileSync } from "node:fs";
import { basename, delimiter, join } from "node:path";
import { NodeServices } from "@effect/platform-node";
import { assert, describe, it } from "@effect/vitest";
import { Kernel } from "@swell/kernel";
import { Cause, Context, Effect, Exit, Fiber, FileSystem, Layer, Path, Redacted, type Scope } from "effect";
import { HttpServer } from "effect/http";
import { TestClock } from "effect/testing";
import { controller, type ControllerOptions } from "../src/controller.ts";
import { Exec } from "../src/exec.ts";
import { Historian } from "../src/historian.ts";
import {
  type ControlSpec,
  defineControl,
  Measurement,
  peerHttp,
  Peer,
  Proposal,
  type Signature,
  Verdict,
} from "../src/index.ts";
import { fakePlant, fakeWorld } from "../src/testing.ts";

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
/** A sensor that reports what of the controller's environment it can see, as signatures. */
const NOSY = `const seen = ["SWELL_TOKEN", "GH_TOKEN", "SW_DECLARED", "SW_UNDECLARED"].filter((k) => process.env[k] !== undefined);
process.stdout.write(JSON.stringify({ signals: seen.map((k) => ({ signature: "env-" + k.toLowerCase(), mechanism: "env" })), analyzed: 1 }));`;

/** An actuator that does half its work, prints, and dies on its first run; on later runs it finishes. It records what each run saw outside the worktree. */
const flaky = (record: string) => `const fs = require("node:fs");
const record = ${JSON.stringify(record)};
const runs = fs.existsSync(record) ? JSON.parse(fs.readFileSync(record, "utf8")) : [];
const brief = JSON.parse(fs.readFileSync(process.env.SWELL_BRIEF, "utf8"));
runs.push({ partial: fs.existsSync("partial.txt"), previous: brief.previous, sample: brief.sample });
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
/** An actuator whose own child keeps writing outside the worktree; only a tree kill stops it. */
const forker = (out: string) => `const { spawn } = require("node:child_process");
spawn(process.execPath, ["-e", ${JSON.stringify(`setInterval(() => require("node:fs").appendFileSync(${JSON.stringify(out)}, "x"), 50)`)}], { stdio: "ignore" });
setInterval(() => {}, 1000);`;
/** An actuator that counts its runs outside the worktree and makes a change. */
const counting = (counter: string) => `const fs = require("node:fs");
fs.appendFileSync(${JSON.stringify(counter)}, "x");
fs.writeFileSync("out.txt", "made\\n");`;
/** An actuator that writes a line the plant's own hook refuses. */
const BAD = `require("node:fs").writeFileSync("out.txt", "BAD\\n");`;

/** A plant with a bare origin, built in a scoped temp dir that is removed afterwards. */
const setup = (extra: (tmp: string) => Record<string, string> = () => ({})) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const tmp = yield* fs.makeTempDirectoryScoped({ prefix: "swell-" });
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
    return { tmp, origin, plant, work: join(tmp, "work") };
  });

/** Someone else pushes to origin: the plant moves under the controller. */
const pushElsewhere = (tmp: string, origin: string, file: string, body: string) => {
  const other = join(tmp, `other-${basename(file)}-${Date.now()}`);
  execFileSync("git", ["clone", "-q", origin, other]);
  writeFileSync(join(other, file), body);
  git(other, "add", "-A");
  git(other, "commit", "-q", "-m", `elsewhere: ${file}`);
  git(other, "push", "-q", "origin", "main");
};

const PEER = Redacted.make("peer-t0k");
const KAI = Redacted.make("kai-t0k");

/** A threshold that moves on the first signal, so a test need not wait three runs. */
const once = (s: Signature) => s.hits > 0;

const services = Layer.mergeAll(NodeServices.layer, Exec.layer.pipe(Layer.provide(NodeServices.layer)));

/** A controller over a real historian file, for the scope of `body`, with peer and operator tokens set. */
const withController = <A, E, R>(
  tmp: string,
  opts: Omit<ControllerOptions, "name" | "work"> & { readonly work: string },
  body: (c: Effect.Success<ReturnType<typeof controller>>) => Effect.Effect<A, E, R>,
) =>
  Effect.gen(function* () {
    const c = yield* controller({
      name: "test",
      peerToken: PEER,
      operators: new Map([["kai", KAI]]),
      ...opts,
    });
    return yield* body(c);
  }).pipe(Effect.scoped, Effect.provide(Historian.layer(join(tmp, "historian.sqlite"))));

const auto = (
  id: string,
  plant: string,
  sensors: ControlSpec["sensors"],
  actuator: ControlSpec["actuators"][number],
) =>
  defineControl({
    plant: { id, kind: "git", root: plant, ref: "main", remote: "origin" },
    sensors,
    actuators: [actuator],
    loops: [
      {
        id: "clean",
        inputs: [sensors[0]!.id],
        actuator: actuator.id,
        mode: "auto",
        operator: "kai",
        threshold: once,
      },
    ],
  });
const manual = (id: string, plant: string, actuator: ControlSpec["actuators"][number]) =>
  defineControl({
    plant: { id, kind: "git", root: plant, ref: "main", remote: "origin" },
    sensors: [{ id: "todo", run: [process.execPath, "sensor.cjs"] }],
    actuators: [actuator],
    loops: [
      {
        id: "clean",
        inputs: ["todo"],
        actuator: actuator.id,
        mode: "manual",
        operator: "kai",
        threshold: once,
      },
    ],
  });
const todo = [{ id: "todo", run: [process.execPath, "sensor.cjs"] }];

const receiptsOf = (c: { reader: Effect.Success<ReturnType<typeof controller>>["reader"] }, rule: string) =>
  c.reader.find(Kernel.Receipt, "by_key", { eq: [rule], limit: 20 });

const SLOW = 60_000;

/** A real-clock test with Node services: a git plant needs real processes and real time. */
const live = <A, E>(
  name: string,
  body: () => Effect.Effect<A, E, Exec | FileSystem.FileSystem | Path.Path | Scope.Scope>,
): void => it.live(name, () => body().pipe(Effect.scoped, Effect.provide(services)), SLOW);

describe("controller over a git plant", () => {
  live(
    "ticks: sample, measurement, move, auto verdict, squash and push; the next tick is quiet; the door",
    () =>
      Effect.gen(function* () {
        const { tmp, origin, plant, work } = yield* setup();
        const seed = git(plant, "rev-parse", "HEAD");
        const spec = auto(
          "p",
          plant,
          [
            ...todo,
            { id: "hang", run: [process.execPath, "hang.cjs"], timeoutMs: 300 },
            { id: "garbage", run: [process.execPath, "garbage.cjs"] },
          ],
          { id: "rm", run: [process.execPath, "act.cjs"] },
        );
        yield* withController(tmp, { work, specs: [spec] }, (c) =>
          Effect.gen(function* () {
            yield* c.tick;
            const measurements = (instrument: string) =>
              c.reader.find(Measurement, "by_instrument", { eq: ["p", instrument], gte: 0, limit: 10 });
            assert.deepStrictEqual(
              (yield* measurements("todo")).map((m) => m.signals.map((s) => s.signature)),
              [["todo-file"]],
            );
            // A hung sensor and a malformed one are failed measurements with a reason, never zeros and never a wedged tick.
            const [hung] = yield* measurements("hang");
            assert.deepStrictEqual([hung!.failed, hung!.signals.length], [1, 0]);
            assert.include(hung!.error ?? "", "timed out");
            assert.include((yield* measurements("garbage"))[0]!.error ?? "", "not a Measured");

            const [proposal] = yield* c.reader.find(Proposal, "by_loop", {
              eq: ["p", "clean"],
              gte: 0,
              limit: 10,
            });
            assert.match(proposal!.cites[0]!, /^git:swell\/clean\/todo-file-[0-9a-f]{8}@/);
            assert.strictEqual(proposal!.via, "operator:kai");
            const [verdict] = yield* c.reader.find(Verdict, "by_plant", { eq: ["p"], gte: 0, limit: 10 });
            assert.deepStrictEqual([verdict!.accept, verdict!.text], [true, "auto: clean is in auto mode"]);

            // Applied by push: origin's main moved by one squash commit and TODO.md is gone there, while the plant's own main and tree stay put.
            assert.match(git(origin, "log", "--oneline", "-1", "main"), /swell: swell\/clean\/todo-file-/);
            assert.strictEqual(git(origin, "rev-list", "--count", "main"), "2");
            assert.notInclude(git(origin, "ls-tree", "--name-only", "main"), "TODO.md");
            assert.strictEqual(git(plant, "rev-parse", "HEAD"), seed);
            assert.isTrue(existsSync(join(plant, "TODO.md")));

            // Second tick: new sample, a clean measurement, no second move.
            yield* c.tick;
            const after = yield* c.reader.find(Proposal, "by_loop", {
              eq: ["p", "clean"],
              gte: 0,
              limit: 10,
            });
            assert.strictEqual(after.length, 1);
            const health = yield* c.health;
            // A plan is current wants, not history: the newest sample is measured, the signature is covered.
            assert.deepStrictEqual(
              health.map((h) => [h.rule, h.done, h.pending.length, h.dead.length]),
              [
                ["control::p-measure", 3, 0, 0],
                ["control::p-clean", 0, 0, 0],
                ["control::p-apply", 1, 0, 0],
              ],
            );

            // The HMI, the peer door, and the operator's verbs, over a real socket.
            const ctx = yield* Layer.build(c.serve(0));
            const address = Context.get(ctx, HttpServer.HttpServer).address;
            const base = `http://127.0.0.1:${"port" in address ? address.port : 0}`;
            const get = (path: string, token?: Redacted.Redacted) =>
              Effect.promise(() =>
                fetch(
                  `${base}${path}`,
                  token ? { headers: { authorization: `Bearer ${Redacted.value(token)}` } } : {},
                ),
              );
            const view = (yield* Effect.promise(async () =>
              (await fetch(`${base}/view?plant=p`)).json(),
            )) as {
              proposals: Array<{ verdict: { accept: boolean } | null }>;
            };
            assert.strictEqual(view.proposals[0]!.verdict?.accept, true);
            assert.strictEqual((yield* get("/view?plant=nope")).status, 400);
            const page = yield* Effect.promise(async () => (await fetch(base)).text());
            assert.include(page, '<table id="proposals">');
            assert.include(page, '<table id="dead">');
            const facts = "/facts/control_measurement";
            assert.strictEqual((yield* get(`${facts}?index=by_instrument`)).status, 401);
            // An operator's token is not a peer's: it opens the verbs, never the read door.
            assert.strictEqual((yield* get(`${facts}?index=by_instrument`, KAI)).status, 401);
            assert.strictEqual((yield* get(`${facts}?index=by_instrument&limit=-5`, PEER)).status, 400);
            assert.strictEqual((yield* get(`${facts}?index=nope`, PEER)).status, 400);

            // The peer client, derived from the same contract, reads the real door.
            const rows = yield* Effect.gen(function* () {
              const peer = yield* Peer;
              return yield* peer.find("control_measurement", "by_instrument", {
                eq: ["p", "todo"],
                limit: 10,
              });
            }).pipe(Effect.provide(peerHttp(base, PEER)));
            assert.strictEqual(rows.length, 2);
            const denied = yield* Effect.gen(function* () {
              const peer = yield* Peer;
              return yield* peer.find("control_measurement", "by_instrument", { limit: 10 });
            }).pipe(Effect.provide(peerHttp(base, KAI)), Effect.flip);
            assert.strictEqual(denied.reason, "NoToken");

            const decide = (body: unknown, token?: Redacted.Redacted) =>
              Effect.promise(() =>
                fetch(`${base}/decide`, {
                  method: "POST",
                  headers: {
                    "content-type": "application/json",
                    ...(token ? { authorization: `Bearer ${Redacted.value(token)}` } : {}),
                  },
                  body: JSON.stringify(body),
                }),
              );
            const yes = { plant: "p", loop: "clean", subject: "x", accept: true, text: "" };
            assert.strictEqual((yield* decide(yes)).status, 401);
            // A peer's token decides nothing, and the operator is whoever the token names, never the body.
            assert.strictEqual((yield* decide(yes, PEER)).status, 401);
            assert.strictEqual((yield* decide({ ...yes, accept: "false" }, KAI)).status, 400);
            assert.strictEqual((yield* decide(yes, KAI)).status, 422);
          }),
        );
      }),
  );

  live("a door with an empty or missing token refuses everyone", () =>
    Effect.gen(function* () {
      const { tmp, plant, work } = yield* setup();
      yield* withController(
        tmp,
        {
          work,
          specs: [manual("shut", plant, { id: "rm", run: [process.execPath, "act.cjs"] })],
          peerToken: Redacted.make(""),
          operators: new Map([["kai", Redacted.make("")]]),
        },
        (c) =>
          Effect.gen(function* () {
            const ctx = yield* Layer.build(c.serve(0));
            const address = Context.get(ctx, HttpServer.HttpServer).address;
            const base = `http://127.0.0.1:${"port" in address ? address.port : 0}`;
            const status = (path: string, init: RequestInit = {}) =>
              Effect.promise(async () => (await fetch(`${base}${path}`, init)).status);
            assert.strictEqual(yield* status("/facts/control_measurement?index=by_instrument"), 401);
            assert.strictEqual(
              yield* status("/facts/control_measurement?index=by_instrument", {
                headers: { authorization: "Bearer " },
              }),
              401,
            );
            assert.strictEqual(
              yield* status("/decide", {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({ plant: "shut", loop: "clean", subject: "x", accept: true, text: "" }),
              }),
              401,
            );
          }),
      );
    }),
  );

  live(
    "apply is push-only: a plant root on another branch with staged, unstaged and untracked work is never touched",
    () =>
      Effect.gen(function* () {
        const { tmp, origin, plant, work } = yield* setup();
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
        yield* withController(
          tmp,
          { work, specs: [auto("push", plant, todo, { id: "rm", run: [process.execPath, "act.cjs"] })] },
          (c) => c.tick,
        );
        assert.match(git(origin, "log", "--oneline", "-1", "main"), /swell: swell\/clean\/todo-file-/);
        assert.notInclude(git(origin, "ls-tree", "--name-only", "main"), "TODO.md");
        assert.deepStrictEqual(standing(), before);
      }),
  );

  live("a rejected push is a failed apply, and a later sweep applies against a fresh fetch", () =>
    Effect.gen(function* () {
      const { tmp, origin, plant, work } = yield* setup();
      const reject = join(tmp, "reject").replaceAll("\\", "/");
      writeFileSync(reject, "");
      const hook = join(origin, "hooks", "pre-receive");
      writeFileSync(
        hook,
        `#!/bin/sh\nif [ -f "${reject}" ]; then echo "rejected by test hook" >&2; exit 1; fi\n`,
      );
      chmodSync(hook, 0o755);
      yield* withController(
        tmp,
        { work, specs: [auto("reject", plant, todo, { id: "rm", run: [process.execPath, "act.cjs"] })] },
        (c) =>
          Effect.gen(function* () {
            const before = git(origin, "rev-parse", "main");
            yield* c.tick;
            assert.strictEqual(git(origin, "rev-parse", "main"), before);
            const failed = yield* receiptsOf(c, "control::reject-apply");
            assert.deepStrictEqual(
              failed.map((r) => r.outcome),
              ["failed"],
            );
            assert.include(failed[0]!.error ?? "", "rejected by test hook");
            yield* FileSystem.FileSystem.use((fs) => fs.remove(join(tmp, "reject")));
            yield* c.tick;
            assert.notStrictEqual(git(origin, "rev-parse", "main"), before);
            assert.notInclude(git(origin, "ls-tree", "--name-only", "main"), "TODO.md");
            assert.deepStrictEqual(
              (yield* receiptsOf(c, "control::reject-apply")).map((r) => r.outcome),
              ["failed", "ok"],
            );
          }),
      );
    }),
  );

  live("a squash conflict fails the apply naming the file, and never wedges the next one", () =>
    Effect.gen(function* () {
      const { tmp, origin, plant, work } = yield* setup((t) => ({
        "edit.cjs": `require("node:fs").writeFileSync("TODO.md", "mine\\n");`,
        ".keep": t,
      }));
      const spec = defineControl({
        plant: { id: "clash", kind: "git", root: plant, ref: "main", remote: "origin" },
        sensors: todo,
        actuators: [{ id: "edit", run: [process.execPath, "edit.cjs"] }],
        loops: [
          {
            id: "clean",
            inputs: ["todo"],
            actuator: "edit",
            mode: "manual",
            operator: "kai",
            threshold: once,
          },
        ],
      });
      yield* withController(tmp, { work, specs: [spec] }, (c) =>
        Effect.gen(function* () {
          yield* c.tick;
          const [p] = yield* c.reader.find(Proposal, "by_loop", {
            eq: ["clash", "clean"],
            gte: 0,
            limit: 10,
          });
          // While the move waits for its operator, someone edits the same line on main.
          pushElsewhere(tmp, origin, "TODO.md", "theirs\n");
          yield* c.decide(
            { plant: "clash", loop: "clean", subject: p!.subject, accept: true, text: "" },
            "kai",
          );
          yield* c.tick;
          const [r] = yield* receiptsOf(c, "control::clash-apply");
          assert.strictEqual(r!.outcome, "failed");
          assert.include(r!.error ?? "", "squash conflict");
          assert.include(r!.error ?? "", "TODO.md");
          // The conflicting edit is undone on main: the same apply worktree takes the next attempt cleanly.
          pushElsewhere(tmp, origin, "TODO.md", "later\n");
          yield* c.tick;
          assert.deepStrictEqual(
            (yield* receiptsOf(c, "control::clash-apply")).map((x) => x.outcome),
            ["failed", "ok"],
          );
          assert.strictEqual(git(origin, "show", "main:TODO.md"), "mine");
        }),
      );
    }),
  );

  live(
    "a failed actuator leaves its work on the move branch and a log; the next attempt resumes it, even after the plant moved",
    () =>
      Effect.gen(function* () {
        const { tmp, origin, plant, work } = yield* setup((t) => ({
          "flaky.cjs": flaky(join(t, "record.json")),
        }));
        yield* withController(
          tmp,
          { work, specs: [manual("flaky", plant, { id: "flaky", run: [process.execPath, "flaky.cjs"] })] },
          (c) =>
            Effect.gen(function* () {
              yield* c.tick;
              const [first] = yield* receiptsOf(c, "control::flaky-clean");
              assert.strictEqual(first!.outcome, "failed");
              const branch = /branch (swell\/clean\/todo-file-[0-9a-f]{8})/.exec(first!.error ?? "")?.[1];
              const logPath = /log (\S+\.log)/.exec(first!.error ?? "")?.[1];
              assert.isDefined(branch);
              assert.isDefined(logPath);
              assert.strictEqual(
                git(plant, "log", "-1", "--format=%s", branch!),
                "swell: wip clean todo-file after exit 1",
              );
              assert.include(git(plant, "ls-tree", "-r", "--name-only", branch!), "partial.txt");
              assert.match(basename(logPath!), /^flaky-clean-todo-file-[0-9a-f]{8}-.+\.log$/);
              const log = readFileSync(logPath!, "utf8");
              assert.include(log, "did half");
              assert.include(log, "then died");
              assert.isBelow(Buffer.byteLength(log), 64 * 1024);

              // The plant moves before the retry: the branch is named for the subject, so the retry still finds it.
              pushElsewhere(tmp, origin, "other.txt", "moved\n");
              yield* c.tick;
              const runs = JSON.parse(readFileSync(join(tmp, "record.json"), "utf8")) as Array<{
                partial: boolean;
                previous: string;
                sample: string;
              }>;
              assert.strictEqual(runs.length, 2);
              assert.deepStrictEqual([runs[0]!.partial, runs[0]!.previous], [false, ""]);
              assert.notStrictEqual(runs[1]!.sample, runs[0]!.sample);
              assert.isTrue(runs[1]!.partial);
              assert.include(runs[1]!.previous, branch!);
              assert.include(runs[1]!.previous, logPath!);
              // The move is the resumed work plus what the second run added, finished as one completed commit.
              assert.strictEqual(git(plant, "log", "-1", "--format=%s", branch!), "swell: clean todo-file");
              const tree = git(plant, "ls-tree", "-r", "--name-only", branch!);
              assert.include(tree, "partial.txt");
              assert.include(tree, "done.txt");
              const [proposal] = yield* c.reader.find(Proposal, "by_loop", {
                eq: ["flaky", "clean"],
                gte: 0,
                limit: 10,
              });
              assert.include(proposal!.cites[0]!, branch!);
              assert.deepStrictEqual(
                (yield* receiptsOf(c, "control::flaky-clean")).map((r) => r.outcome),
                ["failed", "ok"],
              );
            }),
        );
      }),
  );

  live("a timed-out actuator keeps the work it left and the output it printed", () =>
    Effect.gen(function* () {
      const { tmp, plant, work } = yield* setup(() => ({ "stall.cjs": STALL }));
      yield* withController(
        tmp,
        {
          work,
          specs: [
            manual("stall", plant, { id: "stall", run: [process.execPath, "stall.cjs"], timeoutMs: 3_000 }),
          ],
        },
        (c) =>
          Effect.gen(function* () {
            yield* c.tick;
            const [r] = yield* receiptsOf(c, "control::stall-clean");
            assert.strictEqual(r!.outcome, "failed");
            assert.include(r!.error ?? "", "timed out after 3000 ms");
            const branch = /branch (swell\/clean\/todo-file-[0-9a-f]{8})/.exec(r!.error ?? "")?.[1];
            const logPath = /log (\S+\.log)/.exec(r!.error ?? "")?.[1];
            assert.strictEqual(
              git(plant, "log", "-1", "--format=%s", branch!),
              "swell: wip clean todo-file after timeout",
            );
            assert.include(readFileSync(logPath!, "utf8"), "hello");
          }),
      );
    }),
  );

  live("a timed-out actuator's whole tree is killed: its own child stops writing", () =>
    Effect.gen(function* () {
      const { tmp, plant, work } = yield* setup((t) => ({ "fork.cjs": forker(join(t, "grandchild.txt")) }));
      const out = join(tmp, "grandchild.txt");
      yield* withController(
        tmp,
        {
          work,
          specs: [
            manual("fork", plant, { id: "fork", run: [process.execPath, "fork.cjs"], timeoutMs: 1_500 }),
          ],
        },
        (c) => c.tick,
      );
      assert.isTrue(existsSync(out));
      const size = () => statSync(out).size;
      const a = size();
      yield* Effect.sleep("600 millis");
      assert.strictEqual(size(), a, "the grandchild is still writing");
    }),
  );

  live("an actuator that succeeded is never rerun when propose fails, however many attempts", () =>
    Effect.gen(function* () {
      const { tmp, plant, work } = yield* setup((t) => ({ "count.cjs": counting(join(t, "count")) }));
      // Fetch works; every push fails, as when the host is unreachable. `gh` is never reached.
      git(plant, "remote", "set-url", "--push", "origin", join(tmp, "nowhere.git"));
      yield* withController(
        tmp,
        {
          work,
          gh: true,
          specs: [manual("count", plant, { id: "count", run: [process.execPath, "count.cjs"] })],
        },
        (c) =>
          Effect.gen(function* () {
            yield* c.tick;
            yield* c.tick;
            assert.strictEqual(readFileSync(join(tmp, "count"), "utf8"), "x");
            const receipts = yield* receiptsOf(c, "control::count-clean");
            assert.deepStrictEqual(
              receipts.map((r) => r.outcome),
              ["failed", "failed"],
            );
            for (const r of receipts) assert.include(r.error ?? "", "nowhere.git");
            assert.strictEqual(
              (yield* c.reader.find(Proposal, "by_loop", { eq: ["count", "clean"], gte: 0, limit: 10 }))
                .length,
              0,
            );
          }),
      );
    }),
  );

  live(
    "the plant's own commit hook judges the move: a refusal fails the attempt, keeps the work and says why",
    () =>
      Effect.gen(function* () {
        const { tmp, plant, work } = yield* setup(() => ({ "bad.cjs": BAD }));
        const hook = join(plant, ".git", "hooks", "pre-commit");
        writeFileSync(
          hook,
          `#!/bin/sh\nif git diff --cached | grep -q BAD; then echo "hook: no BAD lines" >&2; exit 1; fi\n`,
        );
        chmodSync(hook, 0o755);
        yield* withController(
          tmp,
          { work, specs: [manual("hooked", plant, { id: "bad", run: [process.execPath, "bad.cjs"] })] },
          (c) =>
            Effect.gen(function* () {
              yield* c.tick;
              const [r] = yield* receiptsOf(c, "control::hooked-clean");
              assert.strictEqual(r!.outcome, "failed");
              assert.include(r!.error ?? "", "commit hook");
              const branch = /branch (swell\/clean\/todo-file-[0-9a-f]{8})/.exec(r!.error ?? "")?.[1];
              const logPath = /log (\S+\.log)/.exec(r!.error ?? "")?.[1];
              assert.include(readFileSync(logPath!, "utf8"), "hook: no BAD lines");
              // The save skips hooks: it is the controller's bookkeeping, not a move.
              assert.strictEqual(
                git(plant, "log", "-1", "--format=%s", branch!),
                "swell: wip clean todo-file after hook",
              );
              assert.include(git(plant, "ls-tree", "-r", "--name-only", branch!), "out.txt");
            }),
        );
      }),
  );

  live("an instrument sees only the base environment and what it declares, never the door's token", () =>
    Effect.gen(function* () {
      const { tmp, plant, work } = yield* setup(() => ({ "nosy.cjs": NOSY }));
      const saved = { ...process.env };
      Object.assign(process.env, {
        SWELL_TOKEN: "secret",
        GH_TOKEN: "gh",
        SW_DECLARED: "1",
        SW_UNDECLARED: "1",
      });
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          for (const k of ["SWELL_TOKEN", "GH_TOKEN", "SW_DECLARED", "SW_UNDECLARED"]) delete process.env[k];
          Object.assign(process.env, saved);
        }),
      );
      const spec = defineControl({
        plant: { id: "nosy", kind: "git", root: plant, ref: "main", remote: "origin" },
        sensors: [{ id: "nosy", run: [process.execPath, "nosy.cjs"], env: ["SW_DECLARED"] }],
        actuators: [{ id: "rm", run: [process.execPath, "act.cjs"] }],
        loops: [],
      });
      yield* withController(tmp, { work, specs: [spec] }, (c) =>
        Effect.gen(function* () {
          yield* c.tick;
          const [m] = yield* c.reader.find(Measurement, "by_instrument", {
            eq: ["nosy", "nosy"],
            gte: 0,
            limit: 1,
          });
          assert.deepStrictEqual(
            m!.signals.map((s) => s.signature),
            ["env-sw_declared"],
          );
        }),
      );
    }),
  );

  live("one controller per work dir: a second one refuses to start", () =>
    Effect.gen(function* () {
      const { tmp, plant, work } = yield* setup();
      const spec = manual("locked", plant, { id: "rm", run: [process.execPath, "act.cjs"] });
      yield* withController(tmp, { work, specs: [spec] }, () =>
        Effect.gen(function* () {
          const second = yield* Effect.exit(
            controller({ name: "second", work, specs: [] }).pipe(
              Effect.scoped,
              Effect.provide(Historian.layer(join(tmp, "second.sqlite"))),
            ),
          );
          assert.isTrue(Exit.isFailure(second) && Cause.pretty(second.cause).includes("holds"));
        }),
      );
      // Released with the scope: the next one starts.
      yield* withController(tmp, { work, specs: [spec] }, () => Effect.void);
    }),
  );

  live("--gh: a yes on the HMI merges the PR; a merge on GitHub is read back as the verdict", () =>
    Effect.gen(function* () {
      const { tmp, plant, work } = yield* setup();
      // A fake `gh` on PATH: a .cmd shim on Windows, so the shim path is proven too.
      const bin = join(tmp, "bin");
      const state = join(tmp, "gh-state.json");
      writeFileSync(state, "{}");
      yield* FileSystem.FileSystem.use((fs) => fs.makeDirectory(bin));
      writeFileSync(
        join(bin, "gh.cjs"),
        `const fs = require("node:fs"); const at = ${JSON.stringify(state)};
const s = JSON.parse(fs.readFileSync(at, "utf8")); const [, , a, b, ref] = process.argv;
const save = () => fs.writeFileSync(at, JSON.stringify(s));
if (a === "pr" && b === "create") { const head = process.argv[process.argv.indexOf("--head") + 1]; const n = Object.keys(s).length + 1; s["https://gh/pr/" + n] = { state: "OPEN", head }; save(); console.log("https://gh/pr/" + n); }
else if (a === "pr" && b === "view") { const pr = s[ref] ?? Object.entries(s).find(([, v]) => v.head === ref)?.[1]; if (!pr) { console.error("no pull requests found"); process.exit(1); }
  const url = s[ref] ? ref : Object.entries(s).find(([, v]) => v.head === ref)[0];
  console.log(JSON.stringify({ state: pr.state, url, mergeCommit: pr.state === "MERGED" ? { oid: "merged-" + url.split("/").pop() } : null, comments: [] })); }
else if (a === "pr" && b === "merge") { s[ref].state = "MERGED"; save(); }
else { console.error("fake gh: " + process.argv.slice(2).join(" ")); process.exit(2); }`,
      );
      if (process.platform === "win32")
        writeFileSync(join(bin, "gh.cmd"), `@"${process.execPath}" "%~dp0gh.cjs" %*\r\n`);
      else {
        writeFileSync(
          join(bin, "gh"),
          `#!/bin/sh\nexec "${process.execPath}" "$(dirname "$0")/gh.cjs" "$@"\n`,
        );
        chmodSync(join(bin, "gh"), 0o755);
      }
      const path = process.env.PATH;
      process.env.PATH = `${bin}${delimiter}${path}`;
      yield* Effect.addFinalizer(() => Effect.sync(() => void (process.env.PATH = path)));
      const spec = defineControl({
        plant: { id: "gh", kind: "git", root: plant, ref: "main", remote: "origin" },
        sensors: todo,
        actuators: [
          { id: "rm", run: [process.execPath, "act.cjs"] },
          { id: "rm2", run: [process.execPath, "act.cjs"] },
        ],
        loops: [
          { id: "hmi", inputs: ["todo"], actuator: "rm", mode: "manual", operator: "kai", threshold: once },
          { id: "web", inputs: ["todo"], actuator: "rm2", mode: "manual", operator: "kai", threshold: once },
        ],
      });
      yield* withController(tmp, { work, gh: true, specs: [spec] }, (c) =>
        Effect.gen(function* () {
          yield* c.tick;
          const pr = (loop: string) =>
            c.reader
              .find(Proposal, "by_loop", { eq: ["gh", loop], gte: 0, limit: 10 })
              .pipe(Effect.map((r) => r[0]!));
          const viaHmi = yield* pr("hmi");
          const viaWeb = yield* pr("web");
          assert.match(viaHmi.cites[0]!, /^https:\/\/gh\/pr\/\d$/);
          // The operator says yes on the HMI: the apply merges the PR, and the receipt holds GitHub's merge commit.
          yield* c.decide(
            { plant: "gh", loop: "hmi", subject: viaHmi.subject, accept: true, text: "" },
            "kai",
          );
          // Someone merges the other PR on GitHub: the feedback path reads it back as the verdict.
          const s = JSON.parse(readFileSync(state, "utf8")) as Record<string, { state: string }>;
          s[viaWeb.cites[0]!]!.state = "MERGED";
          writeFileSync(state, JSON.stringify(s));
          yield* c.tick;
          const verdict = yield* c.reader.find(Verdict, "by_key", {
            eq: ["gh", "web", viaWeb.subject],
            limit: 1,
          });
          assert.deepStrictEqual([verdict[0]!.accept, verdict[0]!.cite], [true, viaWeb.cites[0]]);
          const applied = (yield* receiptsOf(c, "control::gh-apply")).map((r) => [r.outcome, r.result]);
          assert.sameDeepMembers(applied, [
            ["ok", `merged-${viaHmi.cites[0]!.split("/").pop()}`],
            ["ok", `merged-${viaWeb.cites[0]!.split("/").pop()}`],
          ]);
          assert.strictEqual(
            (JSON.parse(readFileSync(state, "utf8")) as Record<string, { state: string }>)[viaHmi.cites[0]!]!
              .state,
            "MERGED",
          );
        }),
      );
    }),
  );

  it.effect("one plant's stuck actuator never holds another plant's sampling", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const work = yield* fs.makeTempDirectoryScoped({ prefix: "swell-two-" });
      const world = fakeWorld();
      world.changes = { ref: "r", head: "h", summary: "" };
      world.holdAct = Effect.never;
      world.measured.set("busy-sensor@s0", {
        signals: [{ signature: "x", mechanism: "m" }],
        analyzed: 1,
        excluded: 0,
        failed: 0,
      });
      const plant = (id: string, loops: ControlSpec["loops"]) =>
        defineControl({
          plant: { id, kind: "git", root: "/nowhere", ref: "main" },
          sensors: [{ id: `${id}-sensor`, run: ["x"] }],
          actuators: [{ id: "a", run: ["x"] }],
          loops,
        });
      const specs = [
        plant("busy", [
          {
            id: "l",
            inputs: ["busy-sensor"],
            actuator: "a",
            mode: "manual",
            operator: "kai",
            threshold: once,
          },
        ]),
        plant("idle", []),
      ];
      const c = yield* controller({ name: "t", work, specs, plantLayer: fakePlant(world) });
      const fiber = yield* Effect.forkChild(c.run(60_000));
      for (let i = 0; i < 3; i++) yield* TestClock.adjust(60_000);
      // busy's actuator started and never returned; idle kept its period all the while.
      assert.strictEqual(world.acts.length, 1);
      assert.strictEqual(world.sampledBy.get("busy"), 1);
      assert.isAtLeast(world.sampledBy.get("idle") ?? 0, 4);
      yield* Fiber.interrupt(fiber);
    }).pipe(Effect.scoped, Effect.provide(Historian.layer(":memory:")), Effect.provide(services)),
  );

  it.effect("a dead plant skips its feedback and the loop keeps ticking", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const work = yield* fs.makeTempDirectoryScoped({ prefix: "swell-dead-" });
      const world = fakeWorld();
      world.sampleFails = true;
      const spec = defineControl({
        plant: { id: "dead", kind: "git", root: "/nowhere", ref: "main" },
        sensors: [{ id: "s", run: ["x"] }],
        actuators: [{ id: "a", run: ["x"] }],
        loops: [],
      });
      const c = yield* controller({ name: "t", work, specs: [spec], plantLayer: fakePlant(world) });
      const fiber = yield* Effect.forkChild(c.run(60_000));
      for (let i = 0; i < 4; i++) yield* TestClock.adjust(60_000);
      yield* Fiber.interrupt(fiber);
      // The tick survived every failing sample and kept its period: four periods, five samples.
      assert.isAtLeast(world.sampled, 5);
      const health = yield* c.health;
      assert.deepStrictEqual(
        health.filter((h) => h.rule === "control::dead-measure").map((h) => [h.enabled, h.wanted]),
        [[true, 0]],
      );
    }).pipe(Effect.scoped, Effect.provide(Historian.layer(":memory:")), Effect.provide(services)),
  );
});
