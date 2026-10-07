import { execFile, execFileSync, spawn } from "node:child_process";
import { existsSync, mkdirSync, rmSync, writeFileSync } from "node:fs";
import { join, normalize, resolve } from "node:path";
import { Clock, Effect, Layer, Schema } from "effect";
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

/** The actuator's log keeps this much of the end of each stream, so a file never outgrows 64 KiB. */
const STREAM_TAIL = 32 * 1024 - 64;

const clip = (s: string) =>
  Buffer.byteLength(s) > STREAM_TAIL ? Buffer.from(s).subarray(-STREAM_TAIL).toString("utf8") : s;

interface Actuated {
  /** The exit code; null when the child never ran to an exit (timed out, could not start, was killed). */
  readonly status: number | null;
  /** Why it failed, in words, or empty. */
  readonly why: string;
  /** Why it failed, in a commit subject's words. */
  readonly short: string;
  readonly stdout: string;
  readonly stderr: string;
}

/**
 * Run an actuator and keep the end of its output. Unlike `run`, a timeout or a crash is a result carrying whatever
 * the child printed first, because the caller salvages the work before it fails.
 */
const actuate = (
  argv: ReadonlyArray<string>,
  o: { cwd: string; env: NodeJS.ProcessEnv; timeoutMs: number },
) => {
  const tail = { stdout: "", stderr: "" };
  const done = (status: number | null, why: string, short: string): Actuated => ({
    status,
    why,
    short,
    ...tail,
  });
  return Effect.callback<Actuated>((resume, signal) => {
    const child = spawn(argv[0]!, argv.slice(1), {
      cwd: o.cwd,
      env: o.env,
      signal,
      stdio: ["ignore", "pipe", "pipe"],
    });
    child.stdout.setEncoding("utf8").on("data", (c: string) => (tail.stdout = clip(tail.stdout + c)));
    child.stderr.setEncoding("utf8").on("data", (c: string) => (tail.stderr = clip(tail.stderr + c)));
    child.on("error", (e) => resume(Effect.succeed(done(null, `could not run: ${e.message}`, "error"))));
    child.on("close", (code, sig) =>
      resume(
        Effect.succeed(
          code === 0
            ? done(0, "", "")
            : code === null
              ? done(null, `killed by ${sig}`, "kill")
              : done(code, `exited ${code}`, `exit ${code}`),
        ),
      ),
    );
  }).pipe(
    Effect.timeoutOrElse({
      duration: o.timeoutMs,
      orElse: () => Effect.sync(() => done(null, `timed out after ${o.timeoutMs} ms`, "timeout")),
    }),
  );
};

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

/** A git read that may honestly find nothing. */
const probe = (cwd: string, ...args: ReadonlyArray<string>) => {
  try {
    return git(cwd, ...args);
  } catch {
    return undefined;
  }
};

/** The commit a finished move ends in. The body names the evidence set, so a move for other evidence is never mistaken for this one. */
const doneMessage = (brief: Brief) => {
  const sha7 = brief.sample.slice(0, 7);
  return {
    subject: `swell: ${brief.loop} ${brief.signature} at ${sha7}`,
    body: `sources: ${[...brief.sources].sort().join(", ")}`,
  };
};

const WIP = "swell: wip ";

/**
 * What an earlier attempt left on the move branch, judged from its head: a finished move for this very evidence
 * (skip the actuator), unfinished work (resume it), or nothing usable (start from the sample).
 */
type Prior =
  | { readonly kind: "done"; readonly head: string }
  | { readonly kind: "resume"; readonly head: string }
  | { readonly kind: "fresh" };

const priorWork = (root: string, branch: string, brief: Brief): Prior => {
  const head = probe(root, "rev-parse", "--verify", "-q", `refs/heads/${branch}`);
  if (head === undefined) return { kind: "fresh" };
  if (probe(root, "merge-base", "--is-ancestor", brief.sample, head) === undefined) return { kind: "fresh" };
  const subject = git(root, "log", "-1", "--format=%s", head);
  if (subject.startsWith(WIP)) return { kind: "resume", head };
  const done = doneMessage(brief);
  if (subject === done.subject && git(root, "log", "-1", "--format=%b", head).includes(done.body))
    return { kind: "done", head };
  return { kind: "fresh" };
};

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
 * Git as a plant. The controller's own tree is never the plant: every instrument, actuator and apply runs in a
 * worktree under `work`, and nothing here touches the plant root's working tree, HEAD or index.
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
        const stem = `${plant.id}-${brief.loop}-${slug(brief.signature)}-${sha7}`;
        const changesAt = (head: string): Changes => ({
          ref: branch,
          head,
          summary: git(plant.root, "diff", "--shortstat", `${brief.sample}..${head}`),
        });
        // A finished move for this evidence is never made twice: a retry after a failed propose skips the agent.
        const prior = yield* sync(op, () => priorWork(plant.root, branch, brief));
        if (prior.kind === "done") return yield* sync(op, () => changesAt(prior.head));

        const { cwd, briefPath } = yield* sync(op, () => {
          // Resume what an earlier attempt left: the branch stays where it is, and the brief says why it stopped.
          const cwd = worktreeAt(
            plant.root,
            join(opts.work, "act", plant.id, brief.loop),
            branch,
            prior.kind === "resume" ? prior.head : brief.sample,
          );
          const briefPath = join(opts.work, "briefs", `${stem}.json`);
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
        const startedAt = yield* Clock.currentTimeMillis;
        const ran = yield* actuate(actuator.run, { cwd, env, timeoutMs: actuator.timeoutMs ?? 3_600_000 });
        if (ran.status !== 0) {
          // The agent's work cost money: keep what it left on the branch and its output in a log, then fail naming both.
          const message = yield* sync(op, () => {
            if (!isOwnTop(cwd)) throw new Error("actuator moved the worktree");
            const log = join(
              opts.work,
              "logs",
              `${stem}-${new Date(startedAt).toISOString().replace(/[:.]/g, "-")}.log`,
            );
            mkdirSync(resolve(log, ".."), { recursive: true });
            writeFileSync(
              log,
              `# actuator ${actuator.id} ${ran.why}\n== stdout ==\n${ran.stdout}\n== stderr ==\n${ran.stderr}\n`,
            );
            const kept = git(cwd, "status", "--porcelain") !== "";
            if (kept) {
              git(cwd, "add", "-A");
              git(cwd, "commit", "-q", "-m", `${WIP}${brief.loop} ${brief.signature} after ${ran.short}`);
            }
            return `actuator ${actuator.id} ${ran.why}; ${kept ? "work kept" : "nothing to keep"} on branch ${branch}; log ${log}`;
          });
          return yield* Effect.fail(new PlantError({ op, message }));
        }
        return yield* sync(op, (): Changes | null => {
          if (!isOwnTop(cwd)) throw new Error("actuator moved the worktree");
          const dirty = git(cwd, "status", "--porcelain") !== "";
          // Resumed work counts even when this run added nothing: the branch is already ahead of the sample.
          if (!dirty && git(cwd, "rev-parse", "HEAD") === brief.sample) return null;
          if (dirty) git(cwd, "add", "-A");
          const done = doneMessage(brief);
          git(cwd, "commit", "-q", "--allow-empty", "-m", done.subject, "-m", done.body);
          return changesAt(git(cwd, "rev-parse", "HEAD"));
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
        // Push-only: squash in a worktree of the controller's own, onto the freshly fetched remote ref, and push
        // without force. The plant root's tree, HEAD and index are never touched. A rejected push (the ref moved)
        // is a failed apply, and the kernel retries it on a later sweep against a new fetch.
        const remote = plant.remote;
        if (remote === undefined)
          return yield* Effect.fail(
            new PlantError({ op, message: "apply is push-only and the plant has no remote" }),
          );
        yield* fetch(plant);
        const squashed = yield* sync(op, () => {
          const cwd = worktreeAt(
            plant.root,
            join(opts.work, "apply", plant.id),
            `swell/apply/${plant.id}`,
            git(plant.root, "rev-parse", target(plant)),
          );
          git(cwd, "merge", "-q", "--squash", a.head);
          // Nothing staged: the change is already in the remote ref, so there is nothing to push.
          if (probe(cwd, "diff", "--cached", "--quiet") !== undefined) return { cwd, push: false };
          git(cwd, "commit", "-q", "-m", `swell: ${a.ref}`);
          return { cwd, push: true };
        });
        if (squashed.push)
          yield* must(op, ["git", "-C", squashed.cwd, "push", "-q", remote, `HEAD:refs/heads/${plant.ref}`], {
            cwd: squashed.cwd,
            timeoutMs: NET_MS,
          });
        return yield* sync(op, () => git(squashed.cwd, "rev-parse", "HEAD"));
      }),
  });
