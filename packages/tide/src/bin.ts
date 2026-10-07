#!/usr/bin/env node
import { hostname } from "node:os";
import { dirname, join, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { Effect } from "effect";
import { makeHost } from "./host.ts";
import { defineTide, type TideSpec } from "./loop.ts";

const usage = `tide once|serve|view [--config <tide.config.ts>]... [--work <dir>] [--db <file>] [--port <n>] [--name <host>] [--gh] [--token <t>]
  once   observe, sweep, drain, exit
  serve  tick every --every seconds (default 60) and serve the page on --port (default 4747)
  view   print the view JSON for --plant`;

const args = process.argv.slice(2);
const verb = args[0];
const flag = (name: string): string | undefined => {
  const i = args.indexOf(`--${name}`);
  return i === -1 ? undefined : args[i + 1];
};
const flags = (name: string): Array<string> =>
  args.flatMap((a, i) => (a === `--${name}` ? [args[i + 1]!] : []));
const has = (name: string) => args.includes(`--${name}`);

if (verb === undefined || !["once", "serve", "view"].includes(verb)) {
  console.error(usage);
  process.exit(2);
}

const configs = flags("config");
if (configs.length === 0) configs.push(resolve("tide.config.ts"));
const tides: Array<TideSpec> = [];
for (const c of configs) {
  const path = resolve(c);
  const mod = (await import(pathToFileURL(path).href)) as { default: TideSpec };
  // A config needs no runtime import of @tc/tide: a type-only import is erased, and the host validates here.
  const spec = defineTide(mod.default);
  tides.push({ ...spec, plant: { ...spec.plant, root: resolve(dirname(path), spec.plant.root) } });
}

const name = flag("name") ?? hostname().toLowerCase();
const work = resolve(flag("work") ?? join(process.env.HOME ?? process.env.USERPROFILE ?? ".", ".tide", name));
const host = makeHost({
  name,
  work,
  db: flag("db") ?? join(work, "tide.sqlite"),
  tides,
  gh: has("gh"),
  ...(flag("token") === undefined ? {} : { token: flag("token")! }),
});

if (verb === "view") {
  console.log(
    JSON.stringify(await Effect.runPromise(host.view(flag("plant") ?? tides[0]!.plant.id)), null, 2),
  );
  host.close();
} else if (verb === "once") {
  await Effect.runPromise(host.tick);
  console.log(JSON.stringify(await Effect.runPromise(host.sim.health), null, 2));
  host.close();
} else {
  const port = Number(flag("port") ?? 4747);
  const every = Number(flag("every") ?? 60) * 1000;
  host.serve(port);
  console.log(
    `tide ${name}: ${tides.map((t) => t.plant.id).join(", ")} on http://127.0.0.1:${port}, tick every ${every / 1000}s`,
  );
  let ticking = false;
  const tick = async () => {
    if (ticking) return;
    ticking = true;
    try {
      await Effect.runPromise(host.tick);
    } catch (e) {
      console.error(`tick failed: ${e instanceof Error ? e.message : String(e)}`);
    } finally {
      ticking = false;
    }
  };
  await tick();
  setInterval(() => void tick(), every);
}
