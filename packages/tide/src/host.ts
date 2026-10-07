import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { type AnyPort, Kernel, Memory, type Store, transact } from "@tc/kernel";
import { Clock, Effect, Layer } from "effect";
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
  /** Bearer token peers present to read this host. No token, no peer door. */
  readonly token?: string;
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
    }).pipe(Effect.provide(plantLayer));

  const tick = Effect.gen(function* () {
    yield* enable;
    for (const t of tides) {
      yield* observe(t);
      for (const rule of t.rules) yield* sim.sweep(rule);
      yield* sim.drain;
    }
  });

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

  const serve = (port: number) => {
    const json = (res: ServerResponse, body: unknown, status = 200) => {
      res.writeHead(status, { "content-type": "application/json" });
      res.end(JSON.stringify(body));
    };
    const body = (req: IncomingMessage) =>
      new Promise<string>((ok) => {
        let s = "";
        req.on("data", (c: Buffer) => (s += c.toString("utf8")));
        req.on("end", () => ok(s));
      });
    const server = createServer((req, res) => {
      void (async () => {
        const url = new URL(req.url ?? "/", "http://x");
        const q = (k: string) => url.searchParams.get(k) ?? undefined;
        try {
          if (url.pathname === "/" && req.method === "GET") {
            res.writeHead(200, { "content-type": "text/html" });
            return res.end(page(tides.map((t) => t.tide.plant.id)));
          }
          if (url.pathname === "/health") return json(res, await Effect.runPromise(sim.health));
          if (url.pathname === "/view")
            return json(res, await Effect.runPromise(view(q("plant") ?? tides[0]?.tide.plant.id ?? "")));
          if (url.pathname === "/decide" && req.method === "POST") {
            const b = JSON.parse(await body(req)) as {
              plant: string;
              loop: string;
              subject: string;
              accept: boolean;
              text: string;
              person: string;
            };
            return json(res, { verdict: await Effect.runPromise(decide(b, b.person)) });
          }
          // The peer door: bounded reads only, under a token.
          const peer = url.pathname.match(/^\/(facts|tallies)\/([a-z0-9_:-]+)$/);
          if (peer !== null) {
            if (opts.token === undefined || req.headers.authorization !== `Bearer ${opts.token}`)
              return json(res, { error: "no peer token" }, 401);
            const limit = Math.min(Number(q("limit") ?? 256), 2048);
            if (peer[1] === "tallies") {
              return json(
                res,
                await Effect.runPromise(store.tally.range(peer[2]!, q("gte") ?? "", q("lt") ?? "~", limit)),
              );
            }
            const index = q("index") ?? "by_key";
            const eq = q("eq");
            const order = q("order");
            return json(
              res,
              await Effect.runPromise(
                store.find(peer[2]!, index, fieldsOf(peer[2]!, index), {
                  ...(eq === undefined ? {} : { eq: JSON.parse(eq) as Array<string | number | boolean> }),
                  ...(q("gte") === undefined ? {} : { gte: q("gte")! }),
                  ...(q("lt") === undefined ? {} : { lt: q("lt")! }),
                  ...(order === "desc" ? { order: "desc" as const } : {}),
                  limit,
                }),
              ),
            );
          }
          json(res, { error: "not found" }, 404);
        } catch (e) {
          json(res, { error: e instanceof Error ? e.message : String(e) }, 500);
        }
      })();
    });
    server.listen(port, "127.0.0.1");
    return server;
  };

  return { store, sim, tides, enable, tick, decide, view, serve, close: () => sqlite.close() };
};

/** A read-only gauge with one verb. Proven by a DOM assertion, never by a description. */
const page = (plants: ReadonlyArray<string>) => `<!doctype html>
<meta charset="utf-8"><title>tide</title>
<style>body{font:14px system-ui;margin:2rem;max-width:72rem}table{border-collapse:collapse;width:100%}td,th{border-bottom:1px solid #ddd;padding:.3rem .5rem;text-align:left;vertical-align:top}code{font-size:12px}button{margin-right:.3rem}</style>
<h1>tide</h1>
<label>plant <select id="plant">${plants.map((p) => `<option>${p}</option>`).join("")}</select></label>
<label>person <input id="person" value="kai" size="8"></label>
<h2>issues</h2><table id="issues"><thead><tr><th>fingerprint</th><th>sources</th><th>hits</th><th>runs</th><th>rate</th></tr></thead><tbody></tbody></table>
<h2>proposals</h2><table id="proposals"><thead><tr><th>loop</th><th>subject</th><th>text</th><th>cites</th><th>verdict</th></tr></thead><tbody></tbody></table>
<script>
const $=s=>document.querySelector(s);
const esc=s=>String(s).replace(/[&<>"]/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c]));
async function load(){
  const v=await (await fetch("/view?plant="+encodeURIComponent($("#plant").value))).json();
  $("#issues tbody").innerHTML=v.issues.map(i=>\`<tr><td><code>\${esc(i.fingerprint)}</code></td><td>\${esc(i.sources.join(", "))}</td><td>\${i.hits}</td><td>\${i.runs}</td><td>\${i.rate.toFixed(2)}</td></tr>\`).join("");
  $("#proposals tbody").innerHTML=v.proposals.map(p=>\`<tr><td>\${esc(p.loop)}</td><td><code>\${esc(p.subject)}</code></td><td>\${esc(p.text)}</td><td>\${p.cites.map(c=>/^https?:/.test(c)?\`<a href="\${esc(c)}">\${esc(c)}</a>\`:esc(c)).join("<br>")}</td><td>\${p.verdict?(p.verdict.accept?"yes":"no")+" "+esc(p.verdict.text):\`<button data-a="1" data-l="\${esc(p.loop)}" data-s="\${esc(p.subject)}">yes</button><button data-a="0" data-l="\${esc(p.loop)}" data-s="\${esc(p.subject)}">no</button>\`}</td></tr>\`).join("");
}
document.addEventListener("click",async e=>{const b=e.target.closest("button[data-a]");if(!b)return;
  const accept=b.dataset.a==="1";const text=accept?"":prompt("why not?")||"";if(!accept&&!text)return;
  await fetch("/decide",{method:"POST",body:JSON.stringify({plant:$("#plant").value,loop:b.dataset.l,subject:b.dataset.s,accept,text,person:$("#person").value})});load();});
$("#plant").onchange=load;load();setInterval(load,5000);
</script>`;
