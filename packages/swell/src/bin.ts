#!/usr/bin/env node
import { homedir, hostname } from "node:os";
import { pathToFileURL } from "node:url";
import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Config, Console, Context, Effect, FileSystem, Layer, Option, Path, Redacted } from "effect";
import { Command, Flag } from "effect/cli";
import { Controller } from "./controller.ts";
import { Exec } from "./exec.ts";
import { Historian } from "./historian.ts";
import { decodeControl } from "./spec.ts";

const nonEmpty = (r: Redacted.Redacted) => Redacted.value(r).trim() !== "";

const shared = {
  config: Flag.String("config").pipe(
    Flag.atLeast(0),
    Flag.withDescription("A plant's swell.config.ts; repeat for several plants. Default ./swell.config.ts."),
  ),
  work: Flag.String("work").pipe(
    Flag.optional,
    Flag.withDescription("Where worktrees, briefs, logs and the lock go. Default ~/.swell/<name>."),
  ),
  db: Flag.String("db").pipe(
    Flag.optional,
    Flag.withDescription("The historian's SQLite file. Default <work>/historian.sqlite."),
  ),
  name: Flag.String("name").pipe(
    Flag.optional,
    Flag.withDescription("This controller's name, the authority in its URNs. Default the hostname."),
  ),
  gh: Flag.Boolean("gh").pipe(
    Flag.withDefault(false),
    Flag.withDescription("Manual moves open a PR; a merge on GitHub or a yes on the HMI applies it."),
  ),
  /** Off argv and shell history when it comes from the environment; redacted when logged either way. */
  token: Flag.Redacted("token").pipe(
    Flag.withFallbackConfig(Config.Redacted("SWELL_TOKEN")),
    Flag.filter(nonEmpty, () => "an empty token would open the door"),
    Flag.optional,
    Flag.withDescription(
      "A peer controller's bearer for the read door (or SWELL_TOKEN). None: peers are refused.",
    ),
  ),
  operators: Flag.Redacted("operators").pipe(
    Flag.withFallbackConfig(Config.Redacted("SWELL_OPERATORS")),
    Flag.optional,
    Flag.withDescription(
      "Operator bearers as name=token[,name=token] (or SWELL_OPERATORS). Each token decides as its name.",
    ),
  ),
};
type Shared = { [K in keyof typeof shared]: (typeof shared)[K] extends Flag.Flag<infer A> ? A : never };

/** `kai=t0k,bob=t1k`: each operator's token, refused when a name or a token is empty. */
const parseOperators = (raw: Option.Option<Redacted.Redacted>) =>
  Effect.gen(function* () {
    const out = new Map<string, Redacted.Redacted>();
    if (Option.isNone(raw)) return out;
    for (const pair of Redacted.value(raw.value)
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean)) {
      const at = pair.indexOf("=");
      const name = pair.slice(0, at).trim();
      const token = pair.slice(at + 1).trim();
      if (at <= 0 || token === "" || !/^[a-z][a-z0-9-]*$/.test(name))
        return yield* Effect.fail(new Error("--operators: each entry is name=token, with a kebab-case name"));
      out.set(name, Redacted.make(token));
    }
    return out;
  });

/** A config needs no runtime import of swell: a type-only import is erased, and the controller decodes it here. */
const load = Effect.fn("swell/load")(function* (file: string) {
  const path = yield* Path.Path;
  const abs = path.resolve(file);
  const mod = yield* Effect.tryPromise({
    try: () => import(pathToFileURL(abs).href) as Promise<{ readonly default?: unknown }>,
    catch: (e) => new Error(`load ${file}: ${e instanceof Error ? e.message : String(e)}`),
  });
  const spec = yield* decodeControl(mod.default).pipe(
    Effect.mapError((e) => new Error(`load ${file}: ${e.message}`)),
  );
  return { ...spec, plant: { ...spec.plant, root: path.resolve(path.dirname(abs), spec.plant.root) } };
});

/** The controller for one command's lifetime: the historian and the lock close with the scope, on exit or interrupt. */
const open = (c: Shared) =>
  Effect.gen(function* () {
    const path = yield* Path.Path;
    const fs = yield* FileSystem.FileSystem;
    const specs = yield* Effect.forEach(c.config.length === 0 ? ["swell.config.ts"] : c.config, load);
    const name = Option.getOrElse(c.name, () => hostname().toLowerCase());
    const work = path.resolve(Option.getOrElse(c.work, () => path.join(homedir(), ".swell", name)));
    const db = path.resolve(Option.getOrElse(c.db, () => path.join(work, "historian.sqlite")));
    // A first run on a fresh machine: SQLite opens a file, never a directory that is not there yet.
    yield* fs.makeDirectory(path.dirname(db), { recursive: true });
    const layer = Controller.layer({
      name,
      work,
      specs,
      gh: c.gh,
      peerToken: Option.getOrUndefined(c.token),
      operators: yield* parseOperators(c.operators),
    }).pipe(Layer.provide(Layer.mergeAll(Historian.layer(db), Exec.layer)));
    return Context.get(yield* Layer.build(layer), Controller);
  });

const once = Command.make("once", shared, (c) =>
  Effect.gen(function* () {
    const h = yield* open(c);
    yield* h.tick;
    yield* Console.log(JSON.stringify(yield* h.health, null, 2));
  }).pipe(Effect.scoped),
);

const view = Command.make(
  "view",
  {
    ...shared,
    plant: Flag.String("plant").pipe(Flag.optional, Flag.withDescription("Default the first plant.")),
  },
  (c) =>
    Effect.gen(function* () {
      const h = yield* open(c);
      const plant = Option.getOrElse(c.plant, () => h.plants[0]!.plant.id);
      yield* Console.log(JSON.stringify(yield* h.view(plant), null, 2));
    }).pipe(Effect.scoped),
);

const serve = Command.make(
  "serve",
  {
    ...shared,
    port: Flag.Int("port").pipe(
      Flag.withDefault(4747),
      Flag.filter(
        (n) => n >= 0 && n <= 65535,
        (n) => `port ${n} is not 0..65535`,
      ),
      Flag.withDescription("The HMI and the door, on 127.0.0.1."),
    ),
    period: Flag.Int("period").pipe(
      Flag.withDefault(60),
      Flag.filter(
        (n) => n >= 1,
        (n) => `a period of ${n}s would spin`,
      ),
      Flag.withDescription("Seconds between samples of each plant."),
    ),
  },
  (c) =>
    Effect.gen(function* () {
      const h = yield* open(c);
      yield* Layer.build(h.serve(c.port));
      yield* Console.log(
        `swell ${h.plants.map((p) => p.plant.id).join(", ")} on http://127.0.0.1:${c.port}, sampling every ${c.period}s`,
      );
      yield* h.run(c.period * 1000);
    }).pipe(Effect.scoped),
);

const main = Command.make("swell").pipe(Command.withSubcommands([once, view, serve]));

Command.runWith(main, { version: "0.0.0" })(process.argv.slice(2)).pipe(
  Effect.provide(NodeServices.layer),
  NodeRuntime.runMain,
);
