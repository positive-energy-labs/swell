import { execFileSync } from "node:child_process";
import { existsSync, mkdirSync, mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { assert, describe, it } from "@effect/vitest";
import { Effect } from "effect";
import { makeHost } from "../src/host.ts";
import { defineTide, Proposal, Reading, Verdict } from "../src/index.ts";

const git = (cwd: string, ...a: Array<string>) =>
  execFileSync("git", ["-C", cwd, "-c", "user.name=t", "-c", "user.email=t@t", ...a], {
    encoding: "utf8",
  }).trim();

/** A sensor that reports a TODO.md as one finding, and an actuator that deletes it. Both are plain Node scripts in the plant. */
const SENSOR = `const fs = require("node:fs");
process.stdout.write(JSON.stringify({ findings: fs.existsSync("TODO.md") ? [{ fingerprint: "todo-file", mechanism: "todo", path: "TODO.md" }] : [], analyzed: 1, excluded: 0, failed: 0 }));`;
const ACTUATOR = `require("node:fs").unlinkSync("TODO.md");`;

const setup = () => {
  const tmp = mkdtempSync(join(tmpdir(), "tide-"));
  const origin = join(tmp, "origin.git");
  const plant = join(tmp, "plant");
  execFileSync("git", ["init", "-q", "--bare", "-b", "main", origin]);
  execFileSync("git", ["init", "-q", "-b", "main", plant]);
  writeFileSync(join(plant, "TODO.md"), "later\n");
  writeFileSync(join(plant, "sensor.cjs"), SENSOR);
  writeFileSync(join(plant, "act.cjs"), ACTUATOR);
  git(plant, "add", "-A");
  git(plant, "commit", "-q", "-m", "seed");
  git(plant, "remote", "add", "origin", origin);
  git(plant, "push", "-q", "-u", "origin", "main");
  return { tmp, origin, plant };
};

describe("host over a git plant", () => {
  it("ticks: snapshot, reading, wave, policy verdict, squash-merge, push; the next tick is quiet", async () => {
    const { tmp, origin, plant } = setup();
    const tide = defineTide({
      plant: { id: "p", kind: "git", root: plant, ref: "main", remote: "origin" },
      sensors: [{ id: "todo", kind: "measured", run: [process.execPath, "sensor.cjs"] }],
      actuators: [{ id: "rm", run: [process.execPath, "act.cjs"] }],
      loops: [
        { id: "clean", sense: ["todo"], act: "rm", gate: "auto", person: "kai", when: (i) => i.hits > 0 },
      ],
    });
    const work = join(tmp, "work");
    mkdirSync(work);
    const host = makeHost({ name: "test", db: join(tmp, "tide.sqlite"), work, tides: [tide], token: "t0k" });
    try {
      await Effect.runPromise(host.tick);

      const readings = await Effect.runPromise(
        host.sim.reader.find(Reading, "by_sensor", { eq: ["p", "todo"], gte: 0, limit: 10 }),
      );
      assert.deepStrictEqual(
        readings.map((r) => r.findings.map((f) => f.fingerprint)),
        [["todo-file"]],
      );
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
      // The plant root itself was never a worktree for the actuator.
      assert.isTrue(existsSync(join(work, "act", "p", "clean", "sensor.cjs")));

      // Second tick: new snapshot, a clean reading, no second wave.
      await Effect.runPromise(host.tick);
      const after = await Effect.runPromise(
        host.sim.reader.find(Proposal, "by_loop", { eq: ["p", "clean"], gte: 0, limit: 10 }),
      );
      assert.strictEqual(after.length, 1);
      const health = await Effect.runPromise(host.sim.health);
      assert.deepStrictEqual(
        health.map((h) => [h.rule, h.done, h.pending.length, h.dead.length]),
        [
          // A plan is current wants, not history: the newest snapshot is sensed, the fingerprint is covered.
          ["tide::p-sense", 1, 0, 0],
          ["tide::p-clean", 0, 0, 0],
          ["tide::p-apply", 1, 0, 0],
        ],
      );

      // The page and the peer door.
      const server = host.serve(0);
      await new Promise((r) => server.once("listening", r));
      const port = (server.address() as { port: number }).port;
      const base = `http://127.0.0.1:${port}`;
      try {
        const view = (await (await fetch(`${base}/view?plant=p`)).json()) as {
          issues: Array<unknown>;
          proposals: Array<{ verdict: { accept: boolean } | null }>;
        };
        assert.strictEqual(view.proposals.length, 1);
        assert.strictEqual(view.proposals[0]!.verdict?.accept, true);
        const html = await (await fetch(base)).text();
        assert.include(html, '<table id="proposals">');
        assert.strictEqual((await fetch(`${base}/facts/tide_reading?index=by_sensor`)).status, 401);
        const peer = (await (
          await fetch(
            `${base}/facts/tide_reading?index=by_sensor&eq=${encodeURIComponent('["p","todo"]')}&limit=10`,
            {
              headers: { authorization: "Bearer t0k" },
            },
          )
        ).json()) as Array<{ snapshot: string }>;
        assert.strictEqual(peer.length, 2);
      } finally {
        server.close();
      }
    } finally {
      host.close();
    }
  }, 60_000);
});
