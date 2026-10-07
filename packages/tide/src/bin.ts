#!/usr/bin/env node
import { homedir, hostname } from "node:os";
import { dirname, join, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Config, Console, Effect, Layer, Option, Redacted } from "effect";
import { Command, Flag } from "effect/cli";
import { makeHost } from "./host.ts";
import { defineTide, type TideSpec } from "./loop.ts";

const shared = {
  config: Flag.String("config").pipe(Flag.atLeast(0)),
  work: Flag.String("work").pipe(Flag.optional),
  db: Flag.String("db").pipe(Flag.optional),
  name: Flag.String("name").pipe(Flag.optional),
  gh: Flag.Boolean("gh").pipe(Flag.withDefault(false)),
  /** Off argv and shell history when it comes from `TIDE_TOKEN`; redacted when logged either way. */
  token: Flag.Redacted("token").pipe(Flag.withFallbackConfig(Config.Redacted("TIDE_TOKEN"))),
};
type Shared = { [K in keyof typeof shared]: (typeof shared)[K] extends Flag.Flag<infer A> ? A : never };

/** A config needs no runtime import of @tc/tide: a type-only import is erased, and the host validates here. */
const load = (path: string) =>
  Effect.tryPromise({
    try: async () => {
      const abs = resolve(path);
      const mod = (await import(pathToFileURL(abs).href)) as { default: TideSpec };
      const spec = defineTide(mod.default);
      return { ...spec, plant: { ...spec.plant, root: resolve(dirname(abs), spec.plant.root) } };
    },
    catch: (e) => new Error(`load ${path}: ${e instanceof Error ? e.message : String(e)}`),
  });

const open = (c: Shared) =>
  Effect.gen(function* () {
    const configs = c.config.length === 0 ? ["tide.config.ts"] : c.config;
    const tides = yield* Effect.forEach(configs, load);
    const name = Option.getOrElse(c.name, () => hostname().toLowerCase());
    const work = resolve(Option.getOrElse(c.work, () => join(homedir(), ".tide", name)));
    return makeHost({
      name,
      work,
      db: Option.getOrElse(c.db, () => join(work, "tide.sqlite")),
      tides,
      gh: c.gh,
      token: Redacted.value(c.token),
    });
  });

/** The host for one command's lifetime: the store closes when the scope does, on exit or interrupt. */
const host = (c: Shared) => Effect.acquireRelease(open(c), (h) => Effect.sync(() => h.close()));

const once = Command.make("once", shared, (c) =>
  Effect.gen(function* () {
    const h = yield* host(c);
    yield* h.tick;
    yield* Console.log(JSON.stringify(yield* h.sim.health, null, 2));
  }).pipe(Effect.scoped),
);

const view = Command.make("view", { ...shared, plant: Flag.String("plant").pipe(Flag.optional) }, (c) =>
  Effect.gen(function* () {
    const h = yield* host(c);
    const plant = Option.getOrElse(c.plant, () => h.tides[0]!.tide.plant.id);
    yield* Console.log(JSON.stringify(yield* h.view(plant), null, 2));
  }).pipe(Effect.scoped),
);

const serve = Command.make(
  "serve",
  {
    ...shared,
    port: Flag.Int("port").pipe(Flag.withDefault(4747)),
    every: Flag.Int("every").pipe(Flag.withDefault(60)),
  },
  (c) =>
    Effect.gen(function* () {
      const h = yield* host(c);
      yield* Layer.build(h.serve(c.port));
      yield* Console.log(
        `tide ${h.tides.map((t) => t.tide.plant.id).join(", ")} on http://127.0.0.1:${c.port}, tick every ${c.every}s`,
      );
      yield* h.run(c.every * 1000);
    }).pipe(Effect.scoped),
);

const main = Command.make("tide").pipe(Command.withSubcommands([once, view, serve]));

Command.runWith(main, { version: "0.0.0" })(process.argv.slice(2)).pipe(
  Effect.provide(NodeServices.layer),
  NodeRuntime.runMain,
);
