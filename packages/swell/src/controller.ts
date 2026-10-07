import { timingSafeEqual } from "node:crypto";
import { createServer } from "node:http";
import { NodeHttpServer } from "@effect/platform-node";
import { type AnyPort, Kernel, Memory, scoped, transact } from "@swell/kernel";
import {
  Clock,
  Context,
  Effect,
  FileSystem,
  Layer,
  Option,
  Path,
  Redacted,
  Schedule,
  type Scope,
} from "effect";
import { HttpRouter, HttpServerResponse } from "effect/http";
import { HttpApiBuilder } from "effect/http-api";
import { BadRead, ControllerApi, NoToken, Operator, OperatorAuth, PeerAuth } from "./door.ts";
import type { Exec } from "./exec.ts";
import { Proposal, Sample, signaturesOf, Verdict } from "./facts.ts";
import { gitPlant } from "./git.ts";
import { Historian } from "./historian.ts";
import { hmi } from "./hmi.ts";
import { Decide, type FeedbackPayload, Retry, rulesOf } from "./loop.ts";
import { fieldsOf } from "./peer.ts";
import { Plant, PlantPort } from "./plant.ts";
import type { ControlSpec } from "./spec.ts";

export interface ControllerOptions {
  /** This controller's name: the authority in every URN it writes. */
  readonly name: string;
  /** Where worktrees, briefs, logs and the lock go. Never inside a plant. */
  readonly work: string;
  readonly specs: ReadonlyArray<ControlSpec>;
  readonly gh?: boolean;
  /** A peer controller's bearer for the read door. None configured: the door refuses every peer. */
  readonly peerToken?: Redacted.Redacted | undefined;
  /** Each operator's bearer. A token names its operator; without one, an operator decides on GitHub only. */
  readonly operators?: ReadonlyMap<string, Redacted.Redacted>;
  /** Override the plant layer, for a controller under test. */
  readonly plantLayer?: Layer.Layer<Plant>;
}

/** Proposals per loop the feedback path and the HMI read, newest first. */
const RECENT = 256;

/** Constant-time, and an empty token never matches: a misconfigured door fails closed. */
const same = (token: Redacted.Redacted | undefined, credential: Redacted.Redacted) => {
  if (token === undefined) return false;
  const a = Buffer.from(Redacted.value(token));
  const b = Buffer.from(Redacted.value(credential));
  return a.length > 0 && a.length === b.length && timingSafeEqual(a, b);
};

/** Work dirs this process holds, so a second controller in one process is refused too, not only one in another. */
const held = new Set<string>();

/**
 * One controller over a work dir at a time: `swell once` beside `swell serve` would share worktrees and race the
 * historian. A lock file holds the pid; a dead holder's lock is taken over.
 */
const lockWork = (work: string) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const at = path.join(work, "controller.lock");
    yield* fs.makeDirectory(work, { recursive: true });
    const take = fs.writeFileString(at, String(process.pid), { flag: "wx" });
    const alive = (pid: number) => {
      try {
        process.kill(pid, 0);
        return true;
      } catch {
        return false;
      }
    };
    yield* take.pipe(
      Effect.catch(() =>
        Effect.gen(function* () {
          const holder = Number(yield* fs.readFileString(at));
          const mine = holder === process.pid;
          if ((mine && held.has(at)) || (!mine && Number.isInteger(holder) && alive(holder)))
            return yield* Effect.die(new Error(`swell: another controller (pid ${holder}) holds ${work}`));
          yield* fs.remove(at, { force: true });
          yield* take;
        }),
      ),
      Effect.orDie,
    );
    held.add(at);
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => held.delete(at)).pipe(Effect.andThen(fs.remove(at, { force: true })), Effect.ignore),
    );
  }).pipe(Effect.orDie);

const makeController = (opts: ControllerOptions) =>
  Effect.gen(function* () {
    yield* lockWork(opts.work);
    const store = yield* Historian;
    const plantCtx = yield* Layer.build(
      opts.plantLayer ?? gitPlant({ work: opts.work, gh: opts.gh ?? false }),
    );
    // The plant layer is built once for the controller's life; every job reuses it.
    const plantLayer = Layer.succeedContext(plantCtx);
    const ports = (p: AnyPort) => (p.id === PlantPort.id ? plantLayer : (p.live as Layer.Layer<any>));
    const by = `controller:${opts.name}`;
    // Rules compiled from config are forgotten when the controller closes, so a reload declares them again.
    const compiled = yield* scoped(
      () => opts.specs.map((spec) => ({ spec, ...rulesOf(spec) })),
      (ps) => ps.flatMap((p) => [...p.rules.map((r) => r.id), p.feedback.id]),
    );
    // Each plant has its own simulator, so its queue drains only its own jobs and one plant never waits on another.
    const plants = compiled.map((p) => ({
      ...p,
      sim: Memory.simulator(ports, { store }, by, { rules: () => p.rules }),
    }));
    const all = plants.flatMap((p) => p.rules);
    const door = Memory.simulator(ports, { store }, by, { rules: () => all });

    const enable = Effect.fn("swell/enable")(function* () {
      const now = yield* Clock.currentTimeMillis;
      for (const p of plants) {
        for (const rule of p.rules) {
          const on = yield* door.reader.find(Kernel.RuleEnabled, "by_key", { eq: [rule.id], limit: 1 });
          if (on.length > 0) continue;
          // A loop is switched on for its operator: what it writes carries `via` that operator.
          yield* transact(store, { by, via: p.enablers.get(rule.id), now, trace: undefined }, (db) =>
            db.append(Kernel.RuleEnabled, { rule: rule.id }),
          );
        }
      }
    });

    const proposalsOf = (plant: string, loop: string) =>
      door.reader.find(Proposal, "by_loop", { eq: [plant, loop], order: "desc", limit: RECENT });
    const verdictOf = (plant: string, loop: string, subject: string) =>
      door.reader
        .find(Verdict, "by_key", { eq: [plant, loop, subject], limit: 1 })
        .pipe(Effect.map((r) => Option.fromNullishOr(r[0])));

    /** The feedback path: sample the plant, read decisions made where it keeps its operator, write both. */
    const feedback = Effect.fn("swell/feedback")(function* (p: (typeof plants)[number]) {
      const plant = p.spec.plant;
      yield* Effect.annotateCurrentSpan({ "swell.plant": plant.id });
      const last = (yield* door.reader.find(Sample, "by_plant", {
        eq: [plant.id],
        order: "desc",
        limit: 1,
      }))[0];
      const svc = yield* Plant;
      const sampled = yield* svc.sample(plant, last?.sample);
      const verdicts: Array<FeedbackPayload["verdicts"][number]> = [];
      for (const loop of p.spec.loops) {
        const open = [];
        for (const x of yield* proposalsOf(plant.id, loop.id))
          if (Option.isNone(yield* verdictOf(plant.id, loop.id, x.subject))) open.push(x);
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
      yield* p.sim.entry(p.feedback, {
        verdicts,
        ...(sampled.sample === last?.sample ? {} : { sample: sampled }),
      } satisfies FeedbackPayload);
    });

    /** One plant, once: feedback, then each rule swept and its jobs drained, so a loop reads the measurements just written. */
    const tickPlant = Effect.fn("swell/tick")(function* (p: (typeof plants)[number]) {
      yield* Effect.annotateCurrentSpan({ "swell.plant": p.spec.plant.id });
      yield* feedback(p).pipe(
        Effect.provide(plantLayer),
        // A dead remote skips its feedback this tick; its rules and the other plants still run.
        Effect.catchTag("PlantError", (e) =>
          Effect.logWarning("plant feedback failed").pipe(
            Effect.annotateLogs({ plant: p.spec.plant.id, op: e.op, error: e.message }),
          ),
        ),
      );
      for (const rule of p.rules) {
        yield* p.sim.sweep(rule);
        yield* p.sim.drain;
      }
    });

    /** Every plant once, concurrently: one plant's hour-long actuator never holds another's sample or apply. */
    const tick = enable().pipe(
      Effect.andThen(Effect.forEach(plants, tickPlant, { concurrency: "unbounded", discard: true })),
    );

    /** Each plant on its own fiber at a fixed sample period; a failed tick is logged and the next runs; interrupt stops all. */
    const run = (periodMs: number) =>
      enable().pipe(
        Effect.andThen(
          Effect.forEach(
            plants,
            (p) =>
              tickPlant(p).pipe(
                Effect.catchCause((cause) =>
                  Effect.logError("tick failed", cause).pipe(Effect.annotateLogs({ plant: p.spec.plant.id })),
                ),
                Effect.repeat(Schedule.spaced(periodMs)),
              ),
            { concurrency: "unbounded", discard: true },
          ),
        ),
      );

    const operatorActor = (name: string) => ({
      by: `operator:${name}`,
      person: name,
      roles: new Set(["operator"]),
    });

    const decide = (
      args: { plant: string; loop: string; subject: string; accept: boolean; text: string },
      operator: string,
    ) => door.command(Decide, args, operatorActor(operator));

    const retry = (args: { rule: string; subject: string }, operator: string) =>
      door.command(Retry, args, operatorActor(operator));

    const view = Effect.fn("swell/view")(function* (plant: string) {
      const p = plants.find((x) => x.spec.plant.id === plant);
      if (p === undefined) return yield* new BadRead({ message: `no plant ${plant}` });
      const now = yield* Clock.currentTimeMillis;
      const signatures = yield* signaturesOf(door.reader, plant, now);
      const proposals = [];
      for (const loop of p.spec.loops) {
        for (const x of yield* proposalsOf(plant, loop.id)) {
          const v = yield* verdictOf(plant, loop.id, x.subject);
          proposals.push({
            loop: x.loop,
            subject: x.subject,
            signature: x.signature,
            arming: x.arming,
            operator: x.operator,
            text: x.text,
            cites: x.cites,
            sources: x.sources,
            at: x.at,
            verdict: Option.match(v, {
              onNone: () => null,
              onSome: (v) => ({ accept: v.accept, text: v.text ?? "", cite: v.cite ?? "", at: v.at }),
            }),
          });
        }
      }
      return { plant, now, signatures, proposals, health: yield* p.sim.health };
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
          health: () => door.health,
        }),
      ),
    );
    const OperatorLive = HttpApiBuilder.group(ControllerApi, "operator", (h) =>
      Effect.succeed(
        h.handleAll({
          decide: ({ payload }) =>
            Effect.gen(function* () {
              const { name } = yield* Operator;
              return { verdict: yield* decide(payload, name) };
            }),
          retry: ({ payload }) =>
            Effect.gen(function* () {
              const { name } = yield* Operator;
              return { grant: yield* retry(payload, name) };
            }),
        }),
      ),
    );
    const operators = [...(opts.operators ?? new Map<string, Redacted.Redacted>())];
    const AuthLive = Layer.mergeAll(
      Layer.succeed(
        PeerAuth,
        PeerAuth.of({
          bearer: (app, { credential }) =>
            same(opts.peerToken, credential) ? app : Effect.fail(new NoToken({ message: "bad token" })),
        }),
      ),
      Layer.succeed(
        OperatorAuth,
        OperatorAuth.of({
          bearer: (app, { credential }) => {
            const hit = operators.find(([, token]) => same(token, credential));
            return hit === undefined
              ? Effect.fail(new NoToken({ message: "no operator holds this token" }))
              : Effect.provideService(app, Operator, { name: hit[0] });
          },
        }),
      ),
    );
    const page = hmi(plants.map((p) => p.spec.plant.id));
    const Screen = HttpRouter.add("GET", "/", Effect.succeed(HttpServerResponse.html(page)));

    /** The server as a scoped layer: build it to listen, close the scope to stop. `HttpServer.HttpServer` carries the address. */
    const serve = (port: number) =>
      HttpRouter.serve(Layer.mergeAll(HttpApiBuilder.layer(ControllerApi), Screen), {
        disableLogger: true,
        disableListenLog: true,
      }).pipe(
        Layer.provide(Layer.mergeAll(PeerLive, HmiLive, OperatorLive)),
        // provideMerge, not provide: the router resolves the auth middleware too, and plain provide compiles then dies at runtime.
        Layer.provideMerge(AuthLive),
        Layer.provideMerge(NodeHttpServer.layer(createServer, { port, host: "127.0.0.1" })),
      );

    return {
      name: opts.name,
      plants: plants.map((p) => p.spec),
      store,
      reader: door.reader,
      health: door.health,
      enable: enable(),
      tick,
      run,
      decide,
      retry,
      view,
      serve,
    };
  });

export interface ControllerService extends Effect.Success<ReturnType<typeof makeController>> {}

/** One process over one historian with many plants: sample each plant, sweep every rule, drain, serve the HMI and the peer door. */
export class Controller extends Context.Service<Controller, ControllerService>()("swell/Controller") {
  static readonly layer = (
    opts: ControllerOptions,
  ): Layer.Layer<Controller, never, Historian | Exec | FileSystem.FileSystem | Path.Path> =>
    Layer.effect(Controller, makeController(opts));
}

/** The controller in the caller's scope: for a test or a one-shot verb that owns its historian already. */
export const controller = (
  opts: ControllerOptions,
): Effect.Effect<
  ControllerService,
  never,
  Scope.Scope | Historian | Exec | FileSystem.FileSystem | Path.Path
> => makeController(opts);

export { gitPlant, Historian };
