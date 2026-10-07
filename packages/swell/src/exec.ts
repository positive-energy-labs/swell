import { access } from "node:fs/promises";
import { delimiter, extname, join, resolve } from "node:path";
import { Context, Duration, Effect, Layer, Stream } from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/process";

/** How a child ended, with the end of what it printed. Never a failure: the caller decides what an exit means. */
export interface Ran {
  /** The exit code; null when the child never ran to an exit (timed out, could not start, was killed). */
  readonly status: number | null;
  /** Why it failed, in words, or empty. */
  readonly why: string;
  /** Why it failed, in a commit subject's words. */
  readonly short: string;
  readonly stdout: string;
  readonly stderr: string;
}

export interface RunOptions {
  readonly cwd: string;
  /** The child's whole environment: nothing is inherited that is not named here. */
  readonly env: Readonly<Record<string, string>>;
  readonly timeout: Duration.Input;
}

export interface ExecService {
  /**
   * Run argv off the event loop. A timeout or an interrupt kills the child's whole tree (`taskkill /T` on Windows,
   * the process group elsewhere) and waits for it before returning, so nothing is still writing when the caller
   * salvages. Whatever the child printed before it ended is kept, clipped to its tail.
   */
  readonly run: (argv: ReadonlyArray<string>, options: RunOptions) => Effect.Effect<Ran>;
}

/** Each stream keeps this much of its end, so a log built from both stays under 64 KiB. */
const TAIL = 32 * 1024 - 64;
const clip = (s: string) =>
  Buffer.byteLength(s) > TAIL ? Buffer.from(s).subarray(-TAIL).toString("utf8") : s;

const lookup = (env: Readonly<Record<string, string>>, name: string) =>
  Object.entries(env).find(([k]) => k.toLowerCase() === name.toLowerCase())?.[1];

/** cmd.exe quoting for one argument: a `.cmd` shim can only run under a shell on Windows, so its argv is quoted here. */
const cmdQuote = (s: string) => (/^[\w./:=@\\-]+$/.test(s) ? s : `"${s.replaceAll('"', '""')}"`);

/**
 * On Windows a bare name like `claude` may be a `.cmd` shim, which Node refuses to start without a shell. Resolve it
 * through PATH and PATHEXT; a shim runs under cmd.exe with its arguments quoted, anything else runs as given.
 */
const resolveWin = async (
  argv: ReadonlyArray<string>,
  cwd: string,
  env: Readonly<Record<string, string>>,
) => {
  const [cmd, ...args] = argv as [string, ...Array<string>];
  const exts = (lookup(env, "PATHEXT") ?? ".COM;.EXE;.BAT;.CMD").split(";").filter(Boolean);
  const named = (base: string) => (extname(base) === "" ? exts.map((e) => base + e) : [base]);
  const candidates = /[\\/]/.test(cmd)
    ? named(resolve(cwd, cmd))
    : (lookup(env, "PATH") ?? "")
        .split(delimiter)
        .filter(Boolean)
        .flatMap((dir) => named(join(dir, cmd)));
  for (const c of candidates) {
    const ok = await access(c).then(
      () => true,
      () => false,
    );
    if (!ok) continue;
    return /\.(cmd|bat)$/i.test(c)
      ? { cmd: [cmdQuote(c), ...args.map(cmdQuote)].join(" "), args: [], shell: true }
      : { cmd, args, shell: false };
  }
  return { cmd, args, shell: false };
};

export class Exec extends Context.Service<Exec, ExecService>()("swell/Exec") {
  /** The live service over Effect's child-process spawner; `NodeServices.layer` provides the Node one. */
  static readonly layer = Layer.effect(
    Exec,
    Effect.gen(function* () {
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const run = Effect.fn("swell/exec")(function* (argv: ReadonlyArray<string>, o: RunOptions) {
        yield* Effect.annotateCurrentSpan({ "swell.argv0": argv[0] ?? "", "swell.cwd": o.cwd });
        const tail = { stdout: "", stderr: "" };
        const ran = (status: number | null, why: string, short: string): Ran => ({
          status,
          why,
          short,
          ...tail,
        });
        const drain = <E>(s: Stream.Stream<Uint8Array, E>, k: "stdout" | "stderr") =>
          s.pipe(
            Stream.decodeText(),
            Stream.runForEach((c) => Effect.sync(() => void (tail[k] = clip(tail[k] + c)))),
          );
        const command =
          process.platform === "win32"
            ? yield* Effect.promise(() => resolveWin(argv, o.cwd, o.env))
            : { cmd: argv[0]!, args: argv.slice(1), shell: false };
        const child = Effect.gen(function* () {
          const h = yield* spawner.spawn(
            ChildProcess.make(command.cmd, command.args, {
              cwd: o.cwd,
              env: o.env,
              extendEnv: false,
              shell: command.shell,
              stdin: "ignore",
              forceKillAfter: Duration.seconds(5),
            }),
          );
          yield* Effect.all([drain(h.stdout, "stdout"), drain(h.stderr, "stderr")], {
            concurrency: 2,
            discard: true,
          });
          return Number(yield* h.exitCode);
        }).pipe(Effect.scoped);
        const ms = Duration.toMillis(Duration.fromInputUnsafe(o.timeout));
        return yield* child.pipe(
          Effect.map((code) => (code === 0 ? ran(0, "", "") : ran(code, `exited ${code}`, `exit ${code}`))),
          // Could not start, or ended by a signal: a result with a reason, never a defect.
          Effect.catch((e) =>
            Effect.succeed(
              /signal/i.test(e.message)
                ? ran(null, `killed: ${e.message}`, "kill")
                : ran(null, `could not run: ${e.message}`, "error"),
            ),
          ),
          Effect.timeoutOrElse({
            duration: o.timeout,
            orElse: () => Effect.succeed(ran(null, `timed out after ${ms} ms`, "timeout")),
          }),
        );
      });
      return Exec.of({ run });
    }),
  );
}

/**
 * What an instrument or actuator may see of the controller's environment: the base a program needs to start and
 * find its tools, plus the names its declaration lists. Never the door's token or a credential nobody declared.
 */
const BASE = [
  "PATH",
  "PATHEXT",
  "HOME",
  "USER",
  "USERNAME",
  "LOGNAME",
  "SHELL",
  "LANG",
  "LC_ALL",
  "LC_CTYPE",
  "TERM",
  "TZ",
  "TMPDIR",
  "TEMP",
  "TMP",
  "XDG_CONFIG_HOME",
  "XDG_CACHE_HOME",
  "XDG_DATA_HOME",
  "XDG_STATE_HOME",
  "XDG_RUNTIME_DIR",
  "USERPROFILE",
  "HOMEDRIVE",
  "HOMEPATH",
  "APPDATA",
  "LOCALAPPDATA",
  "SYSTEMROOT",
  "SYSTEMDRIVE",
  "WINDIR",
  "COMSPEC",
  "PROGRAMDATA",
  "PROGRAMFILES",
  "PROGRAMFILES(X86)",
  "COMMONPROGRAMFILES",
  "NUMBER_OF_PROCESSORS",
  "PROCESSOR_ARCHITECTURE",
  "OS",
];

const environ = (): Record<string, string> =>
  Object.fromEntries(
    Object.entries(process.env).filter((e): e is [string, string] => typeof e[1] === "string"),
  );

/** A child's environment: the base set and the declared names, from the controller's own, plus `extra`. */
export const childEnv = (
  declared: ReadonlyArray<string>,
  extra: Readonly<Record<string, string>>,
  from: Readonly<Record<string, string>> = environ(),
): Record<string, string> => {
  const allowed = new Set([...BASE, ...declared].map((n) => n.toUpperCase()));
  const kept = Object.entries(from).filter(([k]) => allowed.has(k.toUpperCase()) && !/^SWELL_/i.test(k));
  return { ...Object.fromEntries(kept), ...extra };
};

/**
 * The controller's own tools (git, gh) see everything it has, so ssh agents and credential helpers work, but never
 * its own secrets: a plant's git hooks are plant code, like its instruments.
 */
export const toolEnv = (from: Readonly<Record<string, string>> = environ()): Record<string, string> =>
  Object.fromEntries(Object.entries(from).filter(([k]) => !/^SWELL_/i.test(k)));
