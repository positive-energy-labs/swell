import { createServer } from "node:http";
import { NodeHttpServer } from "@effect/platform-node";
import { type AnyPort, Kernel, Memory, type Store, transact } from "@swell/kernel";
import { Clock, Effect, Layer, Redacted, Schedule } from "effect";
import { HttpRouter, HttpServerResponse } from "effect/http";
import { HttpApiBuilder } from "effect/http-api";
import { BadRead, ControllerApi, DoorAuth, NoToken } from "./door.ts";
import { Proposal, Sample, signaturesOf, Verdict } from "./facts.ts";
import { gitPlant } from "./git.ts";
import { historian } from "./historian.ts";
import { type ControlSpec, Decide, type FeedbackPayload, rulesOf } from "./loop.ts";
import { fieldsOf } from "./peer.ts";
import { Plant, PlantPort } from "./plant.ts";

export { gitPlant, historian };

export interface ControllerOptions {
  /** This controller's name: the authority in every URN it writes. */
  readonly name: string;
  /** The historian's SQLite file, or `:memory:`. */
  readonly db: string;
  /** Where worktrees and briefs go. Never inside a plant. */
  readonly work: string;
  readonly specs: ReadonlyArray<ControlSpec>;
  readonly gh?: boolean;
  /** Bearer token for the peer door and the operator's decide verb. */
  readonly token: string;
  /** Override the plant layer, for a controller under test. */
  readonly plantLayer?: Layer.Layer<Plant>;
}

const WINDOW_MS = 35 * 86_400_000;

/**
 * One process over one historian with many plants: sample each plant, sweep every rule, drain, serve the HMI
 * and the peer door. The kernel's simulator with SQLite and live ports.
 */
export const makeController = (opts: ControllerOptions) => {
  const hist = historian(opts.db);
  const store: Store = hist.store;
  const plantLayer = opts.plantLayer ?? gitPlant({ work: opts.work, gh: opts.gh ?? false });
  const ports = (p: AnyPort) => (p.id === PlantPort.id ? plantLayer : (p.live as Layer.Layer<any>));
  const by = `controller:${opts.name}`;
  const sim = Memory.simulator(ports, { store }, by);
  const plants = opts.specs.map((spec) => ({ spec, ...rulesOf(spec) }));

  const enable = Effect.gen(function* () {
    const now = yield* Clock.currentTimeMillis;
    for (const p of plants) {
      for (const rule of p.rules) {
        const on = yield* sim.reader.find(Kernel.RuleEnabled, "by_key", { eq: [rule.id], limit: 1 });
        if (on.length > 0) continue;
        yield* transact(store, { by, now, trace: undefined }, (db) =>
          db.append(Kernel.RuleEnabled, { rule: rule.id }),
        );
      }
    }
  });

  /** The feedback path: sample the plant, read decisions made where it keeps its operator, write both. */
  const feedback = (p: (typeof plants)[number]) =>
    Effect.gen(function* () {
      const plant = p.spec.plant;
      const now = yield* Clock.currentTimeMillis;
      const last = (yield* sim.reader.find(Sample, "by_plant", {
        eq: [plant.id],
        order: "desc",
        limit: 1,
      }))[0];
      const svc = yield* Plant;
      const sampled = yield* svc.sample(plant, last?.sample);
      const verdicts: Array<FeedbackPayload["verdicts"][number]> = [];
      const decided = new Set(
        (yield* sim.reader.find(Verdict, "by_plant", {
          eq: [plant.id],
          gte: now - WINDOW_MS,
          limit: 2048,
        })).map((v) => `${v.loop}|${v.subject}`),
      );
      for (const loop of p.spec.loops) {
        const open = (yield* sim.reader.find(Proposal, "by_loop", {
          eq: [plant.id, loop.id],
          gte: now - WINDOW_MS,
          limit: 2048,
        })).filter((x) => !decided.has(`${x.loop}|${x.subject}`));
        if (open.length === 0) continue;
        for (const d of yield* svc.decisions(
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
      const payload: FeedbackPayload = {
        verdicts,
        ...(sampled.sample === last?.sample ? {} : { sample: sampled }),
      };
      yield* sim.entry(p.feedback, payload);
    }).pipe(
      Effect.provide(plantLayer),
      // A dead remote skips its plant this tick; the other plants and the sweep still run.
      Effect.catchTag("PlantError", (e) =>
        Effect.logWarning(`plant ${p.spec.plant.id}: ${e.op}: ${e.message}`),
      ),
    );

  const tick = Effect.gen(function* () {
    yield* enable;
    for (const p of plants) {
      yield* feedback(p);
      // Drain after each sweep: a loop reads the measurements the measure rule just wrote.
      for (const rule of p.rules) {
        yield* sim.sweep(rule);
        yield* sim.drain;
      }
    }
  });

  /** Tick forever at a fixed sample period. Sequential by construction; a failed tick is logged and the next runs; interrupt stops it. */
  const run = (periodMs: number) =>
    tick.pipe(
      Effect.catchCause((cause) => Effect.logError("tick failed", cause)),
      Effect.repeat(Schedule.spaced(periodMs)),
    );

  const decide = (
    args: { plant: string; loop: string; subject: string; accept: boolean; text: string },
    operator: string,
  ) =>
    sim.command(Decide, args, { by: `operator:${operator}`, person: operator, roles: new Set(["operator"]) });

  const view = (plant: string) =>
    Effect.gen(function* () {
      const now = yield* Clock.currentTimeMillis;
      const signatures = yield* signaturesOf(sim.reader, plant, now);
      const rows = [];
      const p = plants.find((x) => x.spec.plant.id === plant);
      for (const loop of p?.spec.loops ?? []) {
        for (const x of yield* sim.reader.find(Proposal, "by_loop", {
          eq: [plant, loop.id],
          gte: now - WINDOW_MS,
          limit: 2048,
        })) {
          const v = (yield* sim.reader.find(Verdict, "by_key", {
            eq: [plant, loop.id, x.subject],
            limit: 1,
          }))[0];
          rows.push({
            ...x,
            verdict:
              v === undefined ? null : { accept: v.accept, text: v.text ?? "", cite: v.cite ?? "", at: v.at },
          });
        }
      }
      return { plant, now, signatures, proposals: rows, health: yield* sim.health };
    });

  const PeerLive = HttpApiBuilder.group(ControllerApi, "peer", (h) =>
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
  const HmiLive = HttpApiBuilder.group(ControllerApi, "hmi", (h) =>
    Effect.succeed(
      h.handleAll({
        view: ({ query }) => view(query.plant ?? plants[0]?.spec.plant.id ?? ""),
        health: () => sim.health,
      }),
    ),
  );
  const OperatorLive = HttpApiBuilder.group(ControllerApi, "operator", (h) =>
    Effect.succeed(
      h.handle("decide", ({ payload: { operator, ...args } }) =>
        decide(args, operator).pipe(Effect.map((verdict) => ({ verdict }))),
      ),
    ),
  );
  const AuthLive = Layer.succeed(
    DoorAuth,
    DoorAuth.of({
      bearer: (app, { credential }) =>
        Redacted.value(credential) === opts.token ? app : Effect.fail(new NoToken({ message: "bad token" })),
    }),
  );
  const Screen = HttpRouter.add(
    "GET",
    "/",
    Effect.sync(() => HttpServerResponse.html(hmi(plants.map((p) => p.spec.plant.id)))),
  );

  /** The server as a scoped layer: build it to listen, close the scope to stop. `HttpServer.HttpServer` carries the address. */
  const serve = (port: number) =>
    HttpRouter.serve(Layer.mergeAll(HttpApiBuilder.layer(ControllerApi), Screen), {
      disableLogger: true,
      disableListenLog: true,
    }).pipe(
      Layer.provide(Layer.mergeAll(PeerLive, HmiLive, OperatorLive)),
      // provideMerge, not provide: the router resolves DoorAuth too, and plain provide compiles then dies at runtime.
      Layer.provideMerge(AuthLive),
      Layer.provideMerge(NodeHttpServer.layer(createServer, { port, host: "127.0.0.1" })),
    );

  return { store, sim, plants, enable, tick, run, decide, view, serve, close: () => hist.close() };
};

/** The HMI: a read-only gauge with one verb. The test checks the template, not a DOM: the script has never run under proof. */
const hmi = (plants: ReadonlyArray<string>) => `<!doctype html>
<meta charset="utf-8"><title>swell</title>
<style>body{font:14px system-ui;margin:2rem;max-width:72rem}table{border-collapse:collapse;width:100%}td,th{border-bottom:1px solid #ddd;padding:.3rem .5rem;text-align:left;vertical-align:top}code{font-size:12px}button{margin-right:.3rem}</style>
<h1>swell</h1>
<label>plant <select id="plant">${plants.map((p) => `<option>${p}</option>`).join("")}</select></label>
<label>operator <input id="operator" value="kai" size="8"></label>
<label>token <input id="token" type="password" size="12"></label>
<h2>signatures</h2><table id="signatures"><thead><tr><th>signature</th><th>sources</th><th>hits</th><th>runs</th><th>rate</th></tr></thead><tbody></tbody></table>
<h2>proposals</h2><table id="proposals"><thead><tr><th>loop</th><th>subject</th><th>text</th><th>cites</th><th>verdict</th></tr></thead><tbody></tbody></table>
<script>
const $=s=>document.querySelector(s);
const esc=s=>String(s).replace(/[&<>"]/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c]));
$("#token").value=localStorage.getItem("swell.token")||"";$("#token").onchange=()=>localStorage.setItem("swell.token",$("#token").value);
async function load(){
  const v=await (await fetch("/view?plant="+encodeURIComponent($("#plant").value))).json();
  $("#signatures tbody").innerHTML=v.signatures.map(i=>\`<tr><td><code>\${esc(i.signature)}</code></td><td>\${esc(i.sources.join(", "))}</td><td>\${i.hits}</td><td>\${i.runs}</td><td>\${i.rate.toFixed(2)}</td></tr>\`).join("");
  $("#proposals tbody").innerHTML=v.proposals.map(p=>\`<tr><td>\${esc(p.loop)}</td><td><code>\${esc(p.subject)}</code></td><td>\${esc(p.text)}</td><td>\${p.cites.map(c=>/^https?:/.test(c)?\`<a href="\${esc(c)}">\${esc(c)}</a>\`:esc(c)).join("<br>")}</td><td>\${p.verdict?(p.verdict.accept?"yes":"no")+" "+esc(p.verdict.text):\`<button data-a="1" data-l="\${esc(p.loop)}" data-s="\${esc(p.subject)}">yes</button><button data-a="0" data-l="\${esc(p.loop)}" data-s="\${esc(p.subject)}">no</button>\`}</td></tr>\`).join("");
}
document.addEventListener("click",async e=>{const b=e.target.closest("button[data-a]");if(!b)return;
  const accept=b.dataset.a==="1";const text=accept?"":prompt("why not?")||"";if(!accept&&!text)return;
  const r=await fetch("/decide",{method:"POST",headers:{"content-type":"application/json",authorization:"Bearer "+$("#token").value},body:JSON.stringify({plant:$("#plant").value,loop:b.dataset.l,subject:b.dataset.s,accept,text,operator:$("#operator").value})});
  if(!r.ok)alert(r.status+" "+await r.text());load();});
$("#plant").onchange=load;load();setInterval(load,5000);
</script>`;
