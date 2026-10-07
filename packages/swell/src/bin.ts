#!/usr/bin/env node
import { homedir, hostname } from "node:os";
import { dirname, join, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Config, Console, Effect, Layer, Option, Redacted } from "effect";
import { Command, Flag } from "effect/cli";
import { makeController } from "./controller.ts";
import { type ControlSpec, defineControl } from "./loop.ts";

const shared = {
  config: Flag.String("config").pipe(Flag.atLeast(0)),
  work: Flag.String("work").pipe(Flag.optional),
  db: Flag.String("db").pipe(Flag.optional),
  name: Flag.String("name").pipe(Flag.optional),
  gh: Flag.Boolean("gh").pipe(Flag.withDefault(false)),
  /** Off argv and shell history when it comes from `SWELL_TOKEN`; redacted when logged either way. */
  token: Flag.Redacted("token").pipe(Flag.withFallbackConfig(Config.Redacted("SWELL_TOKEN"))),
};
type Shared = { [K in keyof typeof shared]: (typeof shared)[K] extends Flag.Flag<infer A> ? A : never };

/** A config needs no runtime import of swell: a type-only import is erased, and the controller validates here. */
const load = (path: string) =>
  Effect.tryPromise({
    try: async () => {
      const abs = resolve(path);
      const mod = (await import(pathToFileURL(abs).href)) as { default: ControlSpec };
      const spec = defineControl(mod.default);
      return { ...spec, plant: { ...spec.plant, root: resolve(dirname(abs), spec.plant.root) } };
    },
    catch: (e) => new Error(`load ${path}: ${e instanceof Error ? e.message : String(e)}`),
  });

const open = (c: Shared) =>
  Effect.gen(function* () {
    const configs = c.config.length === 0 ? ["swell.config.ts"] : c.config;
    const specs = yield* Effect.forEach(configs, load);
    const name = Option.getOrElse(c.name, () => hostname().toLowerCase());
    const work = resolve(Option.getOrElse(c.work, () => join(homedir(), ".swell", name)));
    return makeController({
      name,
      work,
      db: Option.getOrElse(c.db, () => join(work, "historian.sqlite")),
      specs,
      gh: c.gh,
      token: Redacted.value(c.token),
    });
  });

/** The controller for one command's lifetime: the historian closes when the scope does, on exit or interrupt. */
const controller = (c: Shared) => Effect.acquireRelease(open(c), (h) => Effect.sync(() => h.close()));

const once = Command.make("once", shared, (c) =>
  Effect.gen(function* () {
    const h = yield* controller(c);
    yield* h.tick;
    yield* Console.log(JSON.stringify(yield* h.sim.health, null, 2));
  }).pipe(Effect.scoped),
);

const view = Command.make("view", { ...shared, plant: Flag.String("plant").pipe(Flag.optional) }, (c) =>
  Effect.gen(function* () {
    const h = yield* controller(c);
    const plant = Option.getOrElse(c.plant, () => h.plants[0]!.spec.plant.id);
    yield* Console.log(JSON.stringify(yield* h.view(plant), null, 2));
  }).pipe(Effect.scoped),
);

const serve = Command.make(
  "serve",
  {
    ...shared,
    port: Flag.Int("port").pipe(Flag.withDefault(4747)),
    /** The sample period, in seconds. */
    period: Flag.Int("period").pipe(Flag.withDefault(60)),
  },
  (c) =>
    Effect.gen(function* () {
      const h = yield* controller(c);
      yield* Layer.build(h.serve(c.port));
      yield* Console.log(
        `swell ${h.plants.map((p) => p.spec.plant.id).join(", ")} on http://127.0.0.1:${c.port}, sampling every ${c.period}s`,
      );
      yield* h.run(c.period * 1000);
    }).pipe(Effect.scoped),
);

const main = Command.make("swell").pipe(Command.withSubcommands([once, view, serve]));

Command.runWith(main, { version: "0.0.0" })(process.argv.slice(2)).pipe(
  Effect.provide(NodeServices.layer),
  NodeRuntime.runMain,
);
