import { execFileSync, spawnSync } from "node:child_process";
import { existsSync, mkdirSync, rmSync, writeFileSync } from "node:fs";
import { join, normalize, resolve } from "node:path";
import { Effect, Layer } from "effect";
import { type Changes, type Decision, Plant, type Sensed } from "./plant.ts";

// ponytail: 8 MiB makes chatty commands explicit; stream to artifacts if a sensor outgrows it.
const maxBuffer = 8 * 1024 * 1024;

const git = (cwd: string, ...args: ReadonlyArray<string>) =>
  execFileSync("git", ["-C", cwd, ...args], { encoding: "utf8", maxBuffer }).trim();
const gh = (cwd: string, ...args: ReadonlyArray<string>) =>
  execFileSync("gh", args, { cwd, encoding: "utf8", maxBuffer }).trim();

const samePath = (a: string, b: string) =>
  normalize(resolve(a)).toLowerCase() === normalize(resolve(b)).toLowerCase();

/** A run worktree must prove its own top level before any reset, add or commit; a bare leftover directory resolves to the host's tree. */
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

/**
 * Git as a plant. The host's own tree is never the plant: every sensor and actuator runs in a worktree under
 * `work`, and only `apply` with no PR touches the ref checked out at the plant root.
 */
export const gitPlant = (opts: { readonly work: string; readonly gh: boolean }): Layer.Layer<Plant> =>
  Layer.succeed(Plant, {
    head: (plant, parent) =>
      Effect.try({
        try: () => {
          if (plant.remote !== undefined) git(plant.root, "fetch", "-q", plant.remote);
          const target = plant.remote === undefined ? plant.ref : `${plant.remote}/${plant.ref}`;
          const snapshot = git(plant.root, "rev-parse", target);
          const commits =
            parent === undefined
              ? 0
              : Number(git(plant.root, "rev-list", "--count", `${parent}..${snapshot}`));
          const churn = parent === undefined ? 0 : churnOf(plant.root, parent, snapshot);
          return { snapshot, commits, churn, ...(parent === undefined ? {} : { parent }) };
        },
        catch: (e) => new Error(`head of ${plant.id}: ${String(e)}`),
      }),

    sense: (plant, sensor, snapshot) =>
      Effect.try({
        try: (): Sensed => {
          const cwd = worktreeAt(
            plant.root,
            join(opts.work, "sense", plant.id),
            `tide/sense/${plant.id}`,
            snapshot,
          );
          const env = scrubbed({ TIDE_PLANT: plant.id, TIDE_SNAPSHOT: snapshot, TIDE_ROOT: plant.root });
          const r = spawnSync(sensor.run[0]!, sensor.run.slice(1), { cwd, env, encoding: "utf8", maxBuffer });
          const failed = (error: string): Sensed => ({
            findings: [],
            analyzed: 0,
            excluded: 0,
            failed: 1,
            error,
          });
          if (r.error !== undefined) return failed(r.error.message);
          if (r.status !== 0) return failed(r.stderr.trim() || `exited ${r.status}`);
          const out = JSON.parse(r.stdout) as Partial<Sensed>;
          if (!Array.isArray(out.findings)) return failed("sensor printed no findings array");
          return {
            findings: out.findings,
            analyzed: out.analyzed ?? 0,
            excluded: out.excluded ?? 0,
            failed: out.failed ?? 0,
          };
        },
        catch: (e) => new Error(`sense ${sensor.id}@${snapshot.slice(0, 7)}: ${String(e)}`),
      }),

    act: (plant, actuator, brief) =>
      Effect.try({
        try: (): Changes | null => {
          const sha7 = brief.snapshot.slice(0, 7);
          const branch = `tide/${brief.loop}/${slug(brief.fingerprint)}-${sha7}`;
          const cwd = worktreeAt(
            plant.root,
            join(opts.work, "act", plant.id, brief.loop),
            branch,
            brief.snapshot,
          );
          const briefPath = join(
            opts.work,
            "briefs",
            `${plant.id}-${brief.loop}-${slug(brief.fingerprint)}-${sha7}.json`,
          );
          mkdirSync(resolve(briefPath, ".."), { recursive: true });
          writeFileSync(briefPath, JSON.stringify(brief, null, 2));
          const env = scrubbed({
            TIDE_PLANT: plant.id,
            TIDE_SNAPSHOT: brief.snapshot,
            TIDE_ROOT: plant.root,
            TIDE_BRIEF: briefPath,
          });
          const r = spawnSync(actuator.run[0]!, actuator.run.slice(1), {
            cwd,
            env,
            encoding: "utf8",
            maxBuffer,
          });
          if (r.error !== undefined) throw r.error;
          if (r.status !== 0) throw new Error(r.stderr.trim() || `actuator exited ${r.status}`);
          if (!isOwnTop(cwd)) throw new Error("actuator moved the worktree");
          if (git(cwd, "status", "--porcelain") === "") return null;
          git(cwd, "add", "-A");
          git(cwd, "commit", "-q", "-m", `tide: ${brief.loop} ${brief.fingerprint} at ${sha7}`);
          const head = git(cwd, "rev-parse", "HEAD");
          return {
            ref: branch,
            head,
            summary: git(cwd, "diff", "--shortstat", `${brief.snapshot}..${head}`),
          };
        },
        catch: (e) => new Error(`act ${actuator.id}: ${e instanceof Error ? e.message : String(e)}`),
      }),

    propose: (plant, changes, text, gate) =>
      Effect.try({
        try: () => {
          const local = {
            apply: JSON.stringify({ ref: changes.ref, head: changes.head } satisfies Apply),
            cite: `git:${changes.ref}@${changes.head.slice(0, 7)}`,
          };
          if (gate === "auto" || !opts.gh || plant.remote === undefined) return local;
          git(plant.root, "push", "-q", "-f", plant.remote, `${changes.head}:refs/heads/${changes.ref}`);
          const [title, ...rest] = text.split("\n");
          const pr = gh(
            plant.root,
            "pr",
            "create",
            "--head",
            changes.ref,
            "--base",
            plant.ref,
            "--title",
            title!.slice(0, 70),
            "--body",
            `${rest.join("\n")}\n\n${changes.summary}`,
            "--label",
            "tide",
          );
          return {
            apply: JSON.stringify({ ref: changes.ref, head: changes.head, pr } satisfies Apply),
            cite: pr,
          };
        },
        catch: (e) => new Error(`propose ${changes.ref}: ${e instanceof Error ? e.message : String(e)}`),
      }),

    decisions: (plant, applies) =>
      Effect.try({
        try: () =>
          applies.flatMap((apply): Array<Decision> => {
            const a = JSON.parse(apply) as Apply;
            if (a.pr === undefined || !opts.gh) return [];
            const view = JSON.parse(gh(plant.root, "pr", "view", a.pr, "--json", "state,comments")) as {
              state: string;
              comments: Array<{ body: string }>;
            };
            if (view.state === "MERGED") return [{ apply, accept: true, text: "", cite: a.pr }];
            if (view.state === "CLOSED")
              return [{ apply, accept: false, text: view.comments.at(-1)?.body ?? "", cite: a.pr }];
            return [];
          }),
        catch: (e) => new Error(`decisions of ${plant.id}: ${String(e)}`),
      }),

    apply: (plant, apply) =>
      Effect.try({
        try: () => {
          const a = JSON.parse(apply) as Apply;
          if (a.pr !== undefined) {
            // GitHub merged it; the plant ref already holds the change.
            if (plant.remote !== undefined) git(plant.root, "fetch", "-q", plant.remote);
            return git(
              plant.root,
              "rev-parse",
              plant.remote === undefined ? plant.ref : `${plant.remote}/${plant.ref}`,
            );
          }
          // The host merges only into a ref checked out in the plant's own tree.
          const current = git(plant.root, "rev-parse", "--abbrev-ref", "HEAD");
          if (current !== plant.ref)
            throw new Error(`plant root has ${current} checked out, not ${plant.ref}`);
          if (git(plant.root, "status", "--porcelain") !== "") throw new Error("plant root is dirty");
          git(plant.root, "merge", "-q", "--squash", a.head);
          git(plant.root, "commit", "-q", "-m", `tide: ${a.ref}`);
          const sha = git(plant.root, "rev-parse", "HEAD");
          if (plant.remote !== undefined) git(plant.root, "push", "-q", plant.remote, plant.ref);
          return sha;
        },
        catch: (e) => new Error(`apply ${apply}: ${e instanceof Error ? e.message : String(e)}`),
      }),
  });
