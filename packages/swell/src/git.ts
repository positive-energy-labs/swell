import { execFile, execFileSync } from "node:child_process";
import { existsSync, mkdirSync, rmSync, writeFileSync } from "node:fs";
import { join, normalize, resolve } from "node:path";
import { Effect, Layer, Schema } from "effect";
import {
  type Brief,
  type Changes,
  type Decision,
  type Measured,
  MeasuredJson,
  measureFailed,
  Plant,
  PlantError,
  type PlantSpec,
} from "./plant.ts";

// ponytail: 8 MiB makes chatty commands explicit; stream to artifacts if an instrument outgrows it.
const maxBuffer = 8 * 1024 * 1024;

/** Every commit the controller makes is the controller's, whatever identity the machine has or lacks. */
const identity = ["-c", "user.name=swell", "-c", "user.email=swell@localhost"];

/** Local git plumbing is bounded, so it stays synchronous. */
const git = (cwd: string, ...args: ReadonlyArray<string>) =>
  execFileSync("git", ["-C", cwd, ...identity, ...args], { encoding: "utf8", maxBuffer }).trim();

const fail = (op: string, cause: unknown) =>
  new PlantError({ op, message: cause instanceof Error ? cause.message : String(cause), cause });

const sync = <A>(op: string, f: () => A) => Effect.try({ try: f, catch: (cause) => fail(op, cause) });

interface Run {
  readonly status: number | null;
  readonly stdout: string;
  readonly stderr: string;
}

/**
 * Anything that can hang or talk to the network runs asynchronously: the event loop stays free for the HMI
 * and the peer door, a timeout kills the child, and the kernel's interrupt path can reach it.
 */
const run = (
  op: string,
  argv: ReadonlyArray<string>,
  o: { cwd: string; env?: NodeJS.ProcessEnv; timeoutMs: number },
) =>
  Effect.callback<Run, PlantError>((resume, signal) => {
    execFile(
      argv[0]!,
      argv.slice(1),
      { cwd: o.cwd, env: o.env ?? process.env, signal, encoding: "utf8", maxBuffer },
      (err, stdout, stderr) => {
        if (err === null) return resume(Effect.succeed({ status: 0, stdout, stderr }));
        // A child that ran and exited non-zero is a result; one that could not start or was killed is a failure.
        if (typeof err.code === "number") return resume(Effect.succeed({ status: err.code, stdout, stderr }));
        resume(Effect.fail(fail(op, err)));
      },
    );
  }).pipe(
    Effect.timeoutOrElse({
      duration: o.timeoutMs,
      orElse: () => Effect.fail(new PlantError({ op, message: `timed out after ${o.timeoutMs} ms` })),
    }),
  );

/** A shell-out that must succeed: non-zero exit is a plant error carrying stderr. */
const must = (
  op: string,
  argv: ReadonlyArray<string>,
  o: { cwd: string; env?: NodeJS.ProcessEnv; timeoutMs: number },
) =>
  run(op, argv, o).pipe(
    Effect.flatMap((r) =>
      r.status === 0
        ? Effect.succeed(r.stdout.trim())
        : Effect.fail(new PlantError({ op, message: r.stderr.trim() || `exited ${r.status}` })),
    ),
  );

const NET_MS = 120_000;

const samePath = (a: string, b: string) =>
  normalize(resolve(a)).toLowerCase() === normalize(resolve(b)).toLowerCase();

/** A run worktree must prove its own top level before any reset, add or commit; a bare leftover directory resolves to the controller's tree. */
const isOwnTop = (worktree: string) => {
  try {
    return samePath(git(worktree, "rev-parse", "--show-toplevel"), worktree);
  } catch {
    return false;
  }
};

const worktreeAt = (root: string, path: string, branch: string, sha: string) => {
  const reuse = existsSync(path) && isOwnTop(path);
  if (existsSync(path) && !reuse) {
    rmSync(path, { recursive: true, force: true });
    git(root, "worktree", "prune");
  }
  if (reuse) {
    git(path, "checkout", "-q", "-B", branch, sha);
    git(path, "reset", "-q", "--hard", sha);
    git(path, "clean", "-fdq");
  } else {
    mkdirSync(resolve(path, ".."), { recursive: true });
    git(root, "worktree", "add", "-q", "-B", branch, path, sha);
  }
  if (!isOwnTop(path)) throw new Error(`worktree escaped its path: ${path}`);
  return path;
};

const scrubbed = (extra: Record<string, string>): NodeJS.ProcessEnv => {
  const env: NodeJS.ProcessEnv = { ...process.env, ...extra };
  for (const k of Object.keys(env)) if (/(_API_KEY|_AUTH_TOKEN|_SECRET)$/.test(k)) delete env[k];
  return env;
};

const churnOf = (root: string, from: string, to: string) => {
  const stat = git(root, "diff", "--shortstat", `${from}..${to}`);
  return [...stat.matchAll(/(\d+) (?:insertion|deletion)/g)].reduce((n, m) => n + Number(m[1]), 0);
};

const slug = (s: string) =>
  s
    .replace(/[^a-z0-9]+/gi, "-")
    .replace(/^-|-$/g, "")
    .slice(0, 40)
    .toLowerCase();

type Apply = { readonly ref: string; readonly head: string; readonly pr?: string };
const target = (plant: PlantSpec) =>
  plant.remote === undefined ? plant.ref : `${plant.remote}/${plant.ref}`;
const fetch = (plant: PlantSpec) =>
  plant.remote === undefined
    ? Effect.void
    : must(`fetch ${plant.id}`, ["git", "-C", plant.root, "fetch", "-q", plant.remote], {
        cwd: plant.root,
        timeoutMs: NET_MS,
      });

/**
 * Git as a plant. The controller's own tree is never the plant: every instrument and actuator runs in a
 * worktree under `work`, and only `apply` with no PR touches the ref checked out at the plant root.
 */
export const gitPlant = (opts: { readonly work: string; readonly gh: boolean }): Layer.Layer<Plant> =>
  Layer.succeed(Plant, {
    sample: (plant, parent) =>
      Effect.gen(function* () {
        yield* fetch(plant);
        return yield* sync(`sample ${plant.id}`, () => {
          const sample = git(plant.root, "rev-parse", target(plant));
          const commits =
            parent === undefined ? 0 : Number(git(plant.root, "rev-list", "--count", `${parent}..${sample}`));
          const churn = parent === undefined ? 0 : churnOf(plant.root, parent, sample);
          return { sample, commits, churn, ...(parent === undefined ? {} : { parent }) };
        });
      }),

    measure: (plant, instrument, sample) =>
      Effect.gen(function* () {
        const op = `measure ${instrument.id}@${sample.slice(0, 7)}`;
        const cwd = yield* sync(op, () =>
          worktreeAt(plant.root, join(opts.work, "measure", plant.id), `swell/measure/${plant.id}`, sample),
        );
        const env = scrubbed({ SWELL_PLANT: plant.id, SWELL_SAMPLE: sample, SWELL_ROOT: plant.root });
        // An instrument that cannot run is a failed measurement, never a failed attempt: the row says what broke.
        const r = yield* run(op, instrument.run, { cwd, env, timeoutMs: instrument.timeoutMs }).pipe(
          Effect.catchTag("PlantError", (e) =>
            Effect.succeed<Run>({ status: null, stdout: "", stderr: e.message }),
          ),
        );
        if (r.status !== 0) return measureFailed(r.stderr.trim() || `exited ${r.status}`);
        return yield* Schema.decodeUnknownEffect(MeasuredJson)(r.stdout).pipe(
          Effect.catch((e) =>
            Effect.succeed<Measured>(measureFailed(`instrument output is not a Measured: ${e.message}`)),
          ),
        );
      }),

    act: (plant, actuator, brief: Brief) =>
      Effect.gen(function* () {
        const op = `act ${actuator.id}`;
        const sha7 = brief.sample.slice(0, 7);
        const branch = `swell/${brief.loop}/${slug(brief.signature)}-${sha7}`;
        const { cwd, briefPath } = yield* sync(op, () => {
          const cwd = worktreeAt(
            plant.root,
            join(opts.work, "act", plant.id, brief.loop),
            branch,
            brief.sample,
          );
          const briefPath = join(
            opts.work,
            "briefs",
            `${plant.id}-${brief.loop}-${slug(brief.signature)}-${sha7}.json`,
          );
          mkdirSync(resolve(briefPath, ".."), { recursive: true });
          writeFileSync(briefPath, JSON.stringify(brief, null, 2));
          return { cwd, briefPath };
        });
        const env = scrubbed({
          SWELL_PLANT: plant.id,
          SWELL_SAMPLE: brief.sample,
          SWELL_ROOT: plant.root,
          SWELL_BRIEF: briefPath,
        });
        yield* must(op, actuator.run, { cwd, env, timeoutMs: actuator.timeoutMs ?? 3_600_000 });
        return yield* sync(op, (): Changes | null => {
          if (!isOwnTop(cwd)) throw new Error("actuator moved the worktree");
          if (git(cwd, "status", "--porcelain") === "") return null;
          git(cwd, "add", "-A");
          git(cwd, "commit", "-q", "-m", `swell: ${brief.loop} ${brief.signature} at ${sha7}`);
          const head = git(cwd, "rev-parse", "HEAD");
          return { ref: branch, head, summary: git(cwd, "diff", "--shortstat", `${brief.sample}..${head}`) };
        });
      }),

    propose: (plant, changes, text, mode) =>
      Effect.gen(function* () {
        const op = `propose ${changes.ref}`;
        const local = {
          apply: JSON.stringify({ ref: changes.ref, head: changes.head } satisfies Apply),
          cite: `git:${changes.ref}@${changes.head.slice(0, 7)}`,
        };
        if (mode === "auto" || !opts.gh || plant.remote === undefined) return local;
        yield* must(
          op,
          [
            "git",
            "-C",
            plant.root,
            "push",
            "-q",
            "-f",
            plant.remote,
            `${changes.head}:refs/heads/${changes.ref}`,
          ],
          {
            cwd: plant.root,
            timeoutMs: NET_MS,
          },
        );
        const [title, ...rest] = text.split("\n");
        const body = `${rest.join("\n")}\n\n${changes.summary}`;
        const pr = yield* must(
          op,
          [
            "gh",
            "pr",
            "create",
            "--head",
            changes.ref,
            "--base",
            plant.ref,
            "--title",
            title!.slice(0, 70),
            "--body",
            body,
            "--label",
            "swell",
          ],
          { cwd: plant.root, timeoutMs: NET_MS },
        );
        return {
          apply: JSON.stringify({ ref: changes.ref, head: changes.head, pr } satisfies Apply),
          cite: pr,
        };
      }),

    decisions: (plant, applies) =>
      Effect.gen(function* () {
        const out: Array<Decision> = [];
        for (const apply of applies) {
          const a = JSON.parse(apply) as Apply;
          if (a.pr === undefined || !opts.gh) continue;
          const raw = yield* must(
            `decisions ${plant.id}`,
            ["gh", "pr", "view", a.pr, "--json", "state,comments"],
            {
              cwd: plant.root,
              timeoutMs: NET_MS,
            },
          );
          const view = JSON.parse(raw) as { state: string; comments: Array<{ body: string }> };
          if (view.state === "MERGED") out.push({ apply, accept: true, text: "", cite: a.pr });
          if (view.state === "CLOSED")
            out.push({ apply, accept: false, text: view.comments.at(-1)?.body ?? "", cite: a.pr });
        }
        return out;
      }),

    apply: (plant, apply) =>
      Effect.gen(function* () {
        const op = `apply ${apply}`;
        const a = JSON.parse(apply) as Apply;
        if (a.pr !== undefined) {
          // GitHub merged it; the plant ref already holds the change.
          yield* fetch(plant);
          return yield* sync(op, () => git(plant.root, "rev-parse", target(plant)));
        }
        // The controller merges only into a ref checked out in the plant's own tree.
        const sha = yield* sync(op, () => {
          const current = git(plant.root, "rev-parse", "--abbrev-ref", "HEAD");
          if (current !== plant.ref)
            throw new Error(`plant root has ${current} checked out, not ${plant.ref}`);
          if (git(plant.root, "status", "--porcelain") !== "") throw new Error("plant root is dirty");
          git(plant.root, "merge", "-q", "--squash", a.head);
          git(plant.root, "commit", "-q", "-m", `swell: ${a.ref}`);
          return git(plant.root, "rev-parse", "HEAD");
        });
        if (plant.remote !== undefined) {
          yield* must(op, ["git", "-C", plant.root, "push", "-q", plant.remote, plant.ref], {
            cwd: plant.root,
            timeoutMs: NET_MS,
          });
        }
        return sha;
      }),
  });
