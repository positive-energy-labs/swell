import { createServer } from "node:http";
import { NodeHttpServer } from "@effect/platform-node";
import { type AnyPort, Kernel, Memory, type Store, transact } from "@tc/kernel";
import { Clock, Effect, Layer, Redacted, Schedule } from "effect";
import { HttpRouter, HttpServerResponse } from "effect/http";
import { HttpApiBuilder } from "effect/http-api";
import { BadRead, NoPeerToken, PeerAuth, TideApi } from "./door.ts";
import { issuesOf, Proposal, Snapshot, Verdict } from "./facts.ts";
import { gitPlant } from "./git.ts";
import { Decide, type ObservePayload, rulesOf, type TideSpec } from "./loop.ts";
import { fieldsOf } from "./peer.ts";
import { Plant, PlantPort } from "./plant.ts";
import { sqliteStore } from "./sqlite.ts";

export { gitPlant, sqliteStore };

export interface HostOptions {
  /** This host's name: the minting half of every URN it writes. */
  readonly name: string;
  /** SQLite file, or `:memory:`. */
  readonly db: string;
  /** Where worktrees and briefs go. Never inside a plant. */
  readonly work: string;
  readonly tides: ReadonlyArray<TideSpec>;
  readonly gh?: boolean;
  /** Bearer token for the peer door and the page's decide verb. */
  readonly token: string;
  /** Override the plant layer, for a host under test. */
  readonly plant?: Layer.Layer<Plant>;
}

const WINDOW_MS = 35 * 86_400_000;

/**
 * One process over one store with many plants: observe each plant, sweep every rule, drain, serve a read-only
 * page and the peer door. The simulator with SQLite and live ports.
 */
export const makeHost = (opts: HostOptions) => {
  const sqlite = sqliteStore(opts.db);
  const store: Store = sqlite.store;
  const plantLayer = opts.plant ?? gitPlant({ work: opts.work, gh: opts.gh ?? false });
  const ports = (p: AnyPort) => (p.id === PlantPort.id ? plantLayer : (p.live as Layer.Layer<any>));
  const sim = Memory.simulator(ports, { store }, `host:${opts.name}`);
  const tides = opts.tides.map((tide) => ({ tide, ...rulesOf(tide) }));
  const by = `host:${opts.name}`;

  const enable = Effect.gen(function* () {
    const now = yield* Clock.currentTimeMillis;
    for (const t of tides) {
      for (const rule of t.rules) {
        const on = yield* sim.reader.find(Kernel.RuleEnabled, "by_key", { eq: [rule.id], limit: 1 });
        if (on.length > 0) continue;
        yield* transact(store, { by, now, trace: undefined }, (db) =>
          db.append(Kernel.RuleEnabled, { rule: rule.id }),
        );
      }
    }
  });

  const observe = (t: (typeof tides)[number]) =>
    Effect.gen(function* () {
      const plant = t.tide.plant;
      const now = yield* Clock.currentTimeMillis;
      const last = (yield* sim.reader.find(Snapshot, "by_plant", {
        eq: [plant.id],
        order: "desc",
        limit: 1,
      }))[0];
      const p = yield* Plant;
      const head = yield* p.head(plant, last?.snapshot);
      const verdicts: Array<ObservePayload["verdicts"][number]> = [];
      const decided = new Set(
        (yield* sim.reader.find(Verdict, "by_plant", {
          eq: [plant.id],
          gte: now - WINDOW_MS,
          limit: 2048,
        })).map((v) => `${v.loop}|${v.subject}`),
      );
      for (const loop of t.tide.loops) {
        const open = (yield* sim.reader.find(Proposal, "by_loop", {
          eq: [plant.id, loop.id],
          gte: now - WINDOW_MS,
          limit: 2048,
        })).filter((x) => !decided.has(`${x.loop}|${x.subject}`));
        if (open.length === 0) continue;
        for (const d of yield* p.decisions(
          plant,
          open.map((x) => x.apply),
        )) {
          const x = open.find((o) => o.apply === d.apply);
          if (x !== undefined)
            verdicts.push({
              loop: loop.id,
              subject: x.subject,
              accept: d.accept,
              text: d.text,
              cite: d.cite,
            });
        }
      }
      const payload: ObservePayload = { verdicts, ...(head.snapshot === last?.snapshot ? {} : { head }) };
      yield* sim.entry(t.observe, payload);
    }).pipe(
      Effect.provide(plantLayer),
      // A dead remote skips its plant this tick; the other plants and the sweep still run.
      Effect.catchTag("PlantError", (e) =>
        Effect.logWarning(`plant ${t.tide.plant.id}: ${e.op}: ${e.message}`),
      ),
    );

  const tick = Effect.gen(function* () {
    yield* enable;
    for (const t of tides) {
      yield* observe(t);
      // Drain after each sweep: a loop reads the readings the sense rule just wrote.
      for (const rule of t.rules) {
        yield* sim.sweep(rule);
        yield* sim.drain;
      }
    }
  });

  /** Tick forever. Sequential by construction, a failed tick is logged and the next one runs, interrupt stops it. */
  const run = (everyMs: number) =>
    tick.pipe(
      Effect.catchCause((cause) => Effect.logError("tick failed", cause)),
      Effect.repeat(Schedule.spaced(everyMs)),
    );

  const decide = (
    args: { plant: string; loop: string; subject: string; accept: boolean; text: string },
    person: string,
  ) => sim.command(Decide, args, { by: `person:${person}`, person, roles: new Set(["owner"]) });

  const view = (plant: string) =>
    Effect.gen(function* () {
      const now = yield* Clock.currentTimeMillis;
      const issues = yield* issuesOf(sim.reader, plant, now);
      const rows = [];
      const t = tides.find((x) => x.tide.plant.id === plant);
      for (const loop of t?.tide.loops ?? []) {
        for (const p of yield* sim.reader.find(Proposal, "by_loop", {
          eq: [plant, loop.id],
          gte: now - WINDOW_MS,
          limit: 2048,
        })) {
          const v = (yield* sim.reader.find(Verdict, "by_key", {
            eq: [plant, loop.id, p.subject],
            limit: 1,
          }))[0];
          rows.push({
            ...p,
            verdict:
              v === undefined ? null : { accept: v.accept, text: v.text ?? "", cite: v.cite ?? "", at: v.at },
          });
        }
      }
      return { plant, now, issues, proposals: rows, health: yield* sim.health };
    });

  const PeerLive = HttpApiBuilder.group(TideApi, "peer", (h) =>
    Effect.succeed(
      h.handleAll({
        facts: ({ params, query: { index = "by_key", limit = 256, eq, gte, lt, order } }) =>
          fieldsOf(params.table, index).pipe(
            Effect.mapError((e) => new BadRead({ message: e.message })),
            Effect.flatMap((fields) =>
              store.find(params.table, index, fields, {
                limit,
                ...(eq === undefined ? {} : { eq }),
                ...(gte === undefined ? {} : { gte }),
                ...(lt === undefined ? {} : { lt }),
                ...(order === undefined ? {} : { order }),
              }),
            ),
          ),
        tallies: ({ params, query }) =>
          store.tally.range(params.id, query.gte ?? "", query.lt ?? "~", query.limit ?? 256),
      }),
    ),
  );
  const PageLive = HttpApiBuilder.group(TideApi, "page", (h) =>
    Effect.succeed(
      h.handleAll({
        view: ({ query }) => view(query.plant ?? tides[0]?.tide.plant.id ?? ""),
        health: () => sim.health,
      }),
    ),
  );
  const ActLive = HttpApiBuilder.group(TideApi, "act", (h) =>
    Effect.succeed(
      h.handle("decide", ({ payload: { person, ...args } }) =>
        decide(args, person).pipe(Effect.map((verdict) => ({ verdict }))),
      ),
    ),
  );
  const AuthLive = Layer.succeed(
    PeerAuth,
    PeerAuth.of({
      bearer: (app, { credential }) =>
        Redacted.value(credential) === opts.token
          ? app
          : Effect.fail(new NoPeerToken({ message: "bad token" })),
    }),
  );
  const Page = HttpRouter.add(
    "GET",
    "/",
    Effect.sync(() => HttpServerResponse.html(page(tides.map((t) => t.tide.plant.id)))),
  );

  /** The server as a scoped layer: build it to listen, close the scope to stop. `HttpServer.HttpServer` carries the address. */
  const serve = (port: number) =>
    HttpRouter.serve(Layer.mergeAll(HttpApiBuilder.layer(TideApi), Page), {
      disableLogger: true,
      disableListenLog: true,
    }).pipe(
      Layer.provide(Layer.mergeAll(PeerLive, PageLive, ActLive)),
      // provideMerge, not provide: the router resolves PeerAuth too, and plain provide compiles then dies at runtime.
      Layer.provideMerge(AuthLive),
      Layer.provideMerge(NodeHttpServer.layer(createServer, { port, host: "127.0.0.1" })),
    );

  return { store, sim, tides, enable, tick, run, decide, view, serve, close: () => sqlite.close() };
};

/** A read-only gauge with one verb. The test checks the template, not a DOM: the script has never run under proof. */
const page = (plants: ReadonlyArray<string>) => `<!doctype html>
<meta charset="utf-8"><title>tide</title>
<style>body{font:14px system-ui;margin:2rem;max-width:72rem}table{border-collapse:collapse;width:100%}td,th{border-bottom:1px solid #ddd;padding:.3rem .5rem;text-align:left;vertical-align:top}code{font-size:12px}button{margin-right:.3rem}</style>
<h1>tide</h1>
<label>plant <select id="plant">${plants.map((p) => `<option>${p}</option>`).join("")}</select></label>
<label>person <input id="person" value="kai" size="8"></label>
<label>token <input id="token" type="password" size="12"></label>
<h2>issues</h2><table id="issues"><thead><tr><th>fingerprint</th><th>sources</th><th>hits</th><th>runs</th><th>rate</th></tr></thead><tbody></tbody></table>
<h2>proposals</h2><table id="proposals"><thead><tr><th>loop</th><th>subject</th><th>text</th><th>cites</th><th>verdict</th></tr></thead><tbody></tbody></table>
<script>
const $=s=>document.querySelector(s);
const esc=s=>String(s).replace(/[&<>"]/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c]));
$("#token").value=localStorage.getItem("tide.token")||"";$("#token").onchange=()=>localStorage.setItem("tide.token",$("#token").value);
async function load(){
  const v=await (await fetch("/view?plant="+encodeURIComponent($("#plant").value))).json();
  $("#issues tbody").innerHTML=v.issues.map(i=>\`<tr><td><code>\${esc(i.fingerprint)}</code></td><td>\${esc(i.sources.join(", "))}</td><td>\${i.hits}</td><td>\${i.runs}</td><td>\${i.rate.toFixed(2)}</td></tr>\`).join("");
  $("#proposals tbody").innerHTML=v.proposals.map(p=>\`<tr><td>\${esc(p.loop)}</td><td><code>\${esc(p.subject)}</code></td><td>\${esc(p.text)}</td><td>\${p.cites.map(c=>/^https?:/.test(c)?\`<a href="\${esc(c)}">\${esc(c)}</a>\`:esc(c)).join("<br>")}</td><td>\${p.verdict?(p.verdict.accept?"yes":"no")+" "+esc(p.verdict.text):\`<button data-a="1" data-l="\${esc(p.loop)}" data-s="\${esc(p.subject)}">yes</button><button data-a="0" data-l="\${esc(p.loop)}" data-s="\${esc(p.subject)}">no</button>\`}</td></tr>\`).join("");
}
document.addEventListener("click",async e=>{const b=e.target.closest("button[data-a]");if(!b)return;
  const accept=b.dataset.a==="1";const text=accept?"":prompt("why not?")||"";if(!accept&&!text)return;
  const r=await fetch("/decide",{method:"POST",headers:{"content-type":"application/json",authorization:"Bearer "+$("#token").value},body:JSON.stringify({plant:$("#plant").value,loop:b.dataset.l,subject:b.dataset.s,accept,text,person:$("#person").value})});
  if(!r.ok)alert(r.status+" "+await r.text());load();});
$("#plant").onchange=load;load();setInterval(load,5000);
</script>`;
