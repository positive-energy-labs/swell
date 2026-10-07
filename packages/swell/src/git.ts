import { createHash } from "node:crypto";
import { realpath } from "node:fs/promises";
import { Clock, Duration, Effect, FileSystem, Layer, Option, Path, Schema } from "effect";
import { childEnv, Exec, type Ran, toolEnv } from "./exec.ts";
import {
  type Brief,
  type Changes,
  type Decision,
  type Measured,
  MeasuredJson,
  measureFailed,
  Plant,
  PlantError,
} from "./plant.ts";
import type { PlantSpec } from "./spec.ts";

/** Every commit the controller makes is the controller's, whatever identity the machine has or lacks. */
const identity = ["-c", "user.name=swell", "-c", "user.email=swell@localhost"];

/** Plumbing with no hook behind it. */
const PLUMBING = Duration.minutes(2);
/**
 * A commit, a checkout or a push runs the plant's own hooks (a typecheck, a lint, a secrets scan): the plant's policy
 * holds for the controller's moves too. Hooks get this long; a hung one fails the step, never the controller.
 */
const HOOKED = Duration.minutes(10);
const NET = Duration.minutes(2);

const WIP = "swell: wip ";

/** Paths compare case-insensitively where the filesystem does. */
const canonical = (p: string) =>
  process.platform === "win32" || process.platform === "darwin" ? p.toLowerCase() : p;

const slug = (s: string) =>
  s
    .replace(/[^a-z0-9]+/gi, "-")
    .replace(/^-|-$/g, "")
    .slice(0, 40)
    .toLowerCase();

/** A move's names derive from its subject, never from a sample: a retry finds the branch however far the plant moved. */
const moveOf = (plant: PlantSpec, brief: Brief) => {
  const h = createHash("sha256").update(brief.subject).digest("hex").slice(0, 8);
  const tag = `${slug(brief.signature)}-${h}`;
  return { branch: `swell/${brief.loop}/${tag}`, stem: `${plant.id}-${brief.loop}-${tag}` };
};

/** The commit a finished move ends in. Its body names the subject, so a move for other evidence is never mistaken for this one. */
const doneMessage = (brief: Brief) => ({
  subject: `swell: ${brief.loop} ${brief.signature}`,
  body: `subject: ${brief.subject}\nsample: ${brief.sample}`,
});

type Prior =
  | { readonly kind: "done"; readonly head: string }
  | { readonly kind: "resume"; readonly head: string }
  | { readonly kind: "fresh" };

const Apply = Schema.Struct({
  ref: Schema.String,
  head: Schema.String,
  pr: Schema.optionalKey(Schema.String),
});
type Apply = typeof Apply.Type;
const ApplyJson = Schema.fromJsonString(Apply);

const PrView = Schema.fromJsonString(
  Schema.Struct({
    state: Schema.String,
    url: Schema.optionalKey(Schema.String),
    mergeCommit: Schema.optionalKey(Schema.NullOr(Schema.Struct({ oid: Schema.String }))),
    comments: Schema.optionalKey(Schema.Array(Schema.Struct({ body: Schema.String }))),
  }),
);

const target = (plant: PlantSpec) =>
  plant.remote === undefined ? plant.ref : `${plant.remote}/${plant.ref}`;

/**
 * Git as a plant. The controller's own tree is never the plant: every instrument, actuator and apply runs in a
 * worktree under `work`, and nothing here touches the plant root's working tree, HEAD or index. Every git call
 * runs off the event loop with a timeout, so a slow hook never freezes the HMI or another plant.
 */
export const gitPlant = (opts: {
  readonly work: string;
  readonly gh: boolean;
}): Layer.Layer<Plant, never, Exec | FileSystem.FileSystem | Path.Path> =>
  Layer.effect(
    Plant,
    Effect.gen(function* () {
      const exec = yield* Exec;
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const tools = toolEnv();
      const fail = (op: string, message: string) => new PlantError({ op, message });
      const io = (op: string) => Effect.mapError((e: { readonly message: string }) => fail(op, e.message));

      /** A tool call that must succeed: a non-zero exit is a plant error carrying what the tool said. */
      const must = (op: string, argv: ReadonlyArray<string>, cwd: string, timeout: Duration.Input) =>
        exec
          .run(argv, { cwd, env: tools, timeout })
          .pipe(
            Effect.flatMap((r) =>
              r.status === 0
                ? Effect.succeed(r.stdout.trim())
                : Effect.fail(
                    fail(op, [r.why, r.stderr.trim() || r.stdout.trim()].filter(Boolean).join(": ")),
                  ),
            ),
          );
      const git = (
        op: string,
        cwd: string,
        args: ReadonlyArray<string>,
        timeout: Duration.Input = PLUMBING,
      ) => must(op, ["git", "-C", cwd, ...identity, ...args], cwd, timeout);
      /** A git read that may honestly find nothing. */
      const probe = (cwd: string, ...args: ReadonlyArray<string>) =>
        git("probe", cwd, args).pipe(Effect.option);

      /**
       * Same directory, by the OS's own canonical path: native realpath expands 8.3 short names and resolves
       * symlinks. Not by inode: Effect's `stat` reports no `ino` past 2^53, and NTFS file ids routinely are.
       */
      const sameFile = (a: string, b: string) =>
        Effect.tryPromise(() => Promise.all([realpath(a), realpath(b)])).pipe(
          Effect.map(([x, y]) => canonical(x) === canonical(y)),
          Effect.orElseSucceed(() => false),
        );

      /** A run worktree must prove its own top level before any reset, add or commit; a bare leftover directory resolves to the controller's tree. */
      const isOwnTop = (worktree: string) =>
        probe(worktree, "rev-parse", "--show-toplevel").pipe(
          Effect.flatMap(
            Option.match({ onNone: () => Effect.succeed(false), onSome: (top) => sameFile(top, worktree) }),
          ),
        );

      const worktreeAt = Effect.fn("swell/git/worktree")(function* (
        op: string,
        root: string,
        dir: string,
        branch: string,
        sha: string,
      ) {
        const exists = yield* fs.exists(dir).pipe(io(op));
        const reuse = exists && (yield* isOwnTop(dir));
        if (exists && !reuse) {
          yield* fs.remove(dir, { recursive: true, force: true }).pipe(io(op));
          yield* git(op, root, ["worktree", "prune"]);
        }
        if (reuse) {
          // A failed squash leaves unmerged entries that refuse every later checkout: clear the index first.
          yield* git(op, dir, ["reset", "-q", "--hard"]);
          yield* git(op, dir, ["checkout", "-q", "-f", "-B", branch, sha], HOOKED);
          yield* git(op, dir, ["reset", "-q", "--hard", sha]);
          yield* git(op, dir, ["clean", "-fdq"]);
        } else {
          yield* fs.makeDirectory(path.dirname(dir), { recursive: true }).pipe(io(op));
          yield* git(op, root, ["worktree", "add", "-q", "-f", "-B", branch, dir, sha], HOOKED);
        }
        if (!(yield* isOwnTop(dir))) return yield* fail(op, `worktree escaped its path: ${dir}`);
        return dir;
      });

      const fetch = (plant: PlantSpec) =>
        plant.remote === undefined
          ? Effect.void
          : git(`fetch ${plant.id}`, plant.root, ["fetch", "-q", plant.remote], NET).pipe(Effect.asVoid);

      const churnOf = (root: string, from: string, to: string) =>
        git("churn", root, ["diff", "--shortstat", `${from}..${to}`]).pipe(
          Effect.map((stat) =>
            [...stat.matchAll(/(\d+) (?:insertion|deletion)/g)].reduce((n, m) => n + Number(m[1]), 0),
          ),
        );

      /**
       * What an earlier attempt left on the move branch, judged from its head: a finished move for this very subject
       * (skip the actuator), unfinished work (resume it, wherever the plant has moved since), or nothing usable.
       */
      const priorWork = (root: string, branch: string, brief: Brief) =>
        Effect.gen(function* () {
          const head = yield* probe(root, "rev-parse", "--verify", "-q", `refs/heads/${branch}`);
          if (Option.isNone(head)) return { kind: "fresh" } satisfies Prior;
          const subject = yield* git("prior", root, ["log", "-1", "--format=%s", head.value]);
          if (subject.startsWith(WIP)) return { kind: "resume", head: head.value } satisfies Prior;
          const done = doneMessage(brief);
          const body = yield* git("prior", root, ["log", "-1", "--format=%b", head.value]);
          if (subject === done.subject && body.includes(`subject: ${brief.subject}`))
            return { kind: "done", head: head.value } satisfies Prior;
          return { kind: "fresh" } satisfies Prior;
        });

      const writeFile = (op: string, at: string, text: string) =>
        fs
          .makeDirectory(path.dirname(at), { recursive: true })
          .pipe(Effect.andThen(fs.writeFileString(at, text)), io(op));

      /** Gh reads go through a schema: a changed JSON shape is a plant error on one plant, never a defect. */
      const prView = (op: string, plant: PlantSpec, pr: string, fields: string) =>
        must(op, ["gh", "pr", "view", pr, "--json", fields], plant.root, NET).pipe(
          Effect.flatMap((raw) => Schema.decodeUnknownEffect(PrView)(raw)),
          Effect.mapError((e) => (e._tag === "PlantError" ? e : fail(op, `gh pr view: ${e.message}`))),
        );

      const decodeApply = (op: string, apply: string) =>
        Schema.decodeUnknownEffect(ApplyJson)(apply).pipe(Effect.mapError((e) => fail(op, e.message)));

      return Plant.of({
        sample: Effect.fn("swell/git/sample")(function* (plant, parent) {
          const op = `sample ${plant.id}`;
          yield* fetch(plant);
          const sample = yield* git(op, plant.root, ["rev-parse", target(plant)]);
          if (parent === undefined) return { sample, commits: 0, churn: 0 };
          const commits = Number(yield* git(op, plant.root, ["rev-list", "--count", `${parent}..${sample}`]));
          return { sample, commits, churn: yield* churnOf(plant.root, parent, sample), parent };
        }),

        measure: Effect.fn("swell/git/measure")(function* (plant, instrument, sample) {
          const op = `measure ${instrument.id}@${sample.slice(0, 7)}`;
          const cwd = yield* worktreeAt(
            op,
            plant.root,
            path.join(opts.work, "measure", plant.id),
            `swell/measure/${plant.id}`,
            sample,
          );
          const env = childEnv(instrument.env, {
            SWELL_PLANT: plant.id,
            SWELL_SAMPLE: sample,
            SWELL_ROOT: plant.root,
          });
          // An instrument that cannot run is a failed measurement, never a failed attempt: the row says what broke.
          const r = yield* exec.run(instrument.run, { cwd, env, timeout: instrument.timeoutMs });
          if (r.status !== 0) return measureFailed([r.why, r.stderr.trim()].filter(Boolean).join(": "));
          return yield* Schema.decodeUnknownEffect(MeasuredJson)(r.stdout).pipe(
            Effect.catch((e) =>
              Effect.succeed<Measured>(measureFailed(`instrument output is not a Measured: ${e.message}`)),
            ),
          );
        }),

        act: Effect.fn("swell/git/act")(function* (plant, actuator, brief) {
          const op = `act ${actuator.id}`;
          const { branch, stem } = moveOf(plant, brief);
          const changesAt = (head: string) =>
            git(op, plant.root, ["diff", "--shortstat", `${brief.sample}...${head}`]).pipe(
              Effect.map((summary): Changes => ({ ref: branch, head, summary })),
            );
          // A finished move for this subject is never made twice: a retry after a failed propose skips the agent.
          const prior = yield* priorWork(plant.root, branch, brief);
          if (prior.kind === "done") return yield* changesAt(prior.head);

          // Resume what an earlier attempt left: the branch stays where it is, and the brief says why it stopped.
          const cwd = yield* worktreeAt(
            op,
            plant.root,
            path.join(opts.work, "act", plant.id, brief.loop),
            branch,
            prior.kind === "resume" ? prior.head : brief.sample,
          );
          const briefPath = path.join(opts.work, "briefs", `${stem}.json`);
          yield* writeFile(op, briefPath, JSON.stringify(brief, null, 2));
          const env = childEnv(actuator.env ?? [], {
            SWELL_PLANT: plant.id,
            SWELL_SAMPLE: brief.sample,
            SWELL_ROOT: plant.root,
            SWELL_BRIEF: briefPath,
          });
          const startedAt = yield* Clock.currentTimeMillis;

          /**
           * The agent's work cost money: keep whatever it left as a wip commit (hooks skipped: a save, not a move)
           * and its output in a log, then say where both are. A wip head is what the next attempt resumes, so even
           * an agent's own commits survive.
           */
          const salvage = (ran: Pick<Ran, "why" | "short" | "stdout" | "stderr">) =>
            Effect.gen(function* () {
              if (!(yield* isOwnTop(cwd))) return `actuator ${actuator.id} ${ran.why}; it moved the worktree`;
              const stamp = new Date(startedAt).toISOString().replace(/[:.]/g, "-");
              const log = path.join(opts.work, "logs", `${stem}-${stamp}.log`);
              yield* writeFile(
                op,
                log,
                `# actuator ${actuator.id} ${ran.why}\n== stdout ==\n${ran.stdout}\n== stderr ==\n${ran.stderr}\n`,
              );
              const dirty = (yield* git(op, cwd, ["status", "--porcelain"])) !== "";
              if (dirty) yield* git(op, cwd, ["add", "-A"]);
              const moved = (yield* git(op, cwd, ["rev-parse", "HEAD"])) !== brief.sample;
              const kept = dirty || moved;
              yield* git(op, cwd, [
                "commit",
                "-q",
                "--no-verify",
                "--allow-empty",
                "-m",
                `${WIP}${brief.loop} ${brief.signature} after ${ran.short}`,
              ]);
              return `actuator ${actuator.id} ${ran.why}; ${kept ? "work kept" : "nothing to keep"} on branch ${branch}; log ${log}`;
            });

          const ran = yield* exec
            .run(actuator.run, { cwd, env, timeout: actuator.timeoutMs ?? 3_600_000 })
            .pipe(
              // A controller shutdown mid-run: the tree is already killed; save what it left before the fiber ends.
              Effect.onInterrupt(() =>
                salvage({ why: "interrupted", short: "interrupt", stdout: "", stderr: "" }).pipe(
                  Effect.ignore,
                ),
              ),
            );
          if (ran.status !== 0) return yield* fail(op, yield* salvage(ran));

          if (!(yield* isOwnTop(cwd))) return yield* fail(op, "actuator moved the worktree");
          const dirty = (yield* git(op, cwd, ["status", "--porcelain"])) !== "";
          // Resumed work counts even when this run added nothing: the branch is already ahead of the sample.
          if (!dirty && (yield* git(op, cwd, ["rev-parse", "HEAD"])) === brief.sample) return null;
          if (dirty) yield* git(op, cwd, ["add", "-A"]);
          const done = doneMessage(brief);
          // The plant's hooks judge the move: a refusal fails the attempt with what the hook said, and the work is
          // saved for the next attempt, whose brief carries the refusal as `previous`.
          const committed = yield* exec.run(
            [
              "git",
              "-C",
              cwd,
              ...identity,
              "commit",
              "-q",
              "--allow-empty",
              "-m",
              done.subject,
              "-m",
              done.body,
            ],
            { cwd, env: tools, timeout: HOOKED },
          );
          if (committed.status !== 0)
            return yield* fail(
              op,
              yield* salvage({
                why: `was refused by the plant's commit hook (${committed.why})`,
                short: "hook",
                stdout: committed.stdout,
                stderr: committed.stderr,
              }),
            );
          return yield* changesAt(yield* git(op, cwd, ["rev-parse", "HEAD"]));
        }),

        propose: Effect.fn("swell/git/propose")(function* (plant, changes, text, mode) {
          const op = `propose ${changes.ref}`;
          const local = {
            apply: JSON.stringify({ ref: changes.ref, head: changes.head } satisfies Apply),
            cite: `git:${changes.ref}@${changes.head.slice(0, 7)}`,
          };
          if (mode === "auto" || !opts.gh || plant.remote === undefined) return local;
          yield* git(
            op,
            plant.root,
            ["push", "-q", "-f", plant.remote, `${changes.head}:refs/heads/${changes.ref}`],
            HOOKED,
          );
          // A retry after `gh pr create` succeeded but the attempt failed later reuses the open PR.
          const open = yield* prView(op, plant, changes.ref, "state,url").pipe(Effect.option);
          const reuse = Option.filter(open, (v) => v.state === "OPEN" && v.url !== undefined);
          const create = Effect.gen(function* () {
            const [title, ...rest] = text.split("\n");
            // The body goes by file: no argv length limit, and no newline for a Windows shim to cut.
            const body = path.join(opts.work, "prs", `${slug(changes.ref)}.md`);
            yield* writeFile(op, body, `${rest.join("\n")}\n\n${changes.summary}`);
            return yield* must(
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
              ].concat(["--body-file", body, "--label", "swell"]),
              plant.root,
              NET,
            );
          });
          const pr = Option.isSome(reuse) ? reuse.value.url! : yield* create;
          return {
            apply: JSON.stringify({ ref: changes.ref, head: changes.head, pr } satisfies Apply),
            cite: pr,
          };
        }),

        decisions: Effect.fn("swell/git/decisions")(function* (plant, applies) {
          const out: Array<Decision> = [];
          if (!opts.gh) return out;
          const op = `decisions ${plant.id}`;
          for (const apply of applies) {
            const a = yield* decodeApply(op, apply);
            if (a.pr === undefined) continue;
            const view = yield* prView(op, plant, a.pr, "state,comments");
            if (view.state === "MERGED") out.push({ apply, accept: true, text: "", cite: a.pr });
            if (view.state === "CLOSED")
              out.push({ apply, accept: false, text: view.comments?.at(-1)?.body ?? "", cite: a.pr });
          }
          return out;
        }),

        apply: Effect.fn("swell/git/apply")(function* (plant, apply) {
          const op = `apply ${plant.id}`;
          const a = yield* decodeApply(op, apply);
          if (a.pr !== undefined) {
            // A PR move: merged on GitHub already, or the operator's yes merges it here. GitHub's own rules (reviews,
            // checks) still hold: a refusal fails the apply and the kernel retries it on a later sweep.
            if (!opts.gh)
              return yield* fail(op, `${a.pr} is a PR move; run the controller with --gh to apply it`);
            const before = yield* prView(op, plant, a.pr, "state,mergeCommit");
            if (before.state === "CLOSED")
              return yield* fail(op, `${a.pr} was closed on GitHub without merging`);
            if (before.state === "OPEN")
              yield* must(op, ["gh", "pr", "merge", a.pr, "--squash"], plant.root, NET);
            const after =
              before.state === "MERGED" ? before : yield* prView(op, plant, a.pr, "state,mergeCommit");
            const oid = after.mergeCommit?.oid;
            if (after.state !== "MERGED" || oid === undefined)
              return yield* fail(op, `${a.pr} is ${after.state} on GitHub, not merged yet`);
            yield* fetch(plant);
            return oid;
          }
          // Push-only: squash in a worktree of the controller's own, onto the freshly fetched remote ref, and push
          // without force. The plant root's tree, HEAD and index are never touched. A rejected push (the ref moved)
          // is a failed apply, and the kernel retries it on a later sweep against a new fetch.
          const remote = plant.remote;
          if (remote === undefined) return yield* fail(op, "apply is push-only and the plant has no remote");
          yield* fetch(plant);
          const base = yield* git(op, plant.root, ["rev-parse", target(plant)]);
          const cwd = yield* worktreeAt(
            op,
            plant.root,
            path.join(opts.work, "apply", plant.id),
            `swell/apply/${plant.id}`,
            base,
          );
          const merged = yield* exec.run(["git", "-C", cwd, ...identity, "merge", "-q", "--squash", a.head], {
            cwd,
            env: tools,
            timeout: PLUMBING,
          });
          if (merged.status !== 0) {
            const files = yield* probe(cwd, "diff", "--name-only", "--diff-filter=U");
            yield* git(op, cwd, ["reset", "-q", "--hard"]);
            return yield* fail(
              op,
              `squash conflict onto ${target(plant)}: ${
                Option.getOrElse(files, () => "")
                  .split("\n")
                  .filter(Boolean)
                  .join(", ") || merged.stderr.trim()
              }`,
            );
          }
          // Nothing staged: the change is already in the remote ref, so there is nothing to push.
          if (Option.isSome(yield* probe(cwd, "diff", "--cached", "--quiet"))) return base;
          yield* git(op, cwd, ["commit", "-q", "-m", `swell: ${a.ref}`], HOOKED);
          yield* git(op, cwd, ["push", "-q", remote, `HEAD:refs/heads/${plant.ref}`], HOOKED);
          return yield* git(op, cwd, ["rev-parse", "HEAD"]);
        }),
      });
    }),
  );
