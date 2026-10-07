---
name: effect
description: How to write and review idiomatic Effect v4 TypeScript. Use before writing, changing or reviewing any code that imports `effect` or `@effect/*`, when choosing between a hand-rolled helper and an Effect module, or when an Effect API is in doubt. Treats Effect as the standard library and the pinned source as the only reference.
---

# Idiomatic Effect

Effect is not a library you call into. It is the standard library: processes, files, paths, SQL, HTTP, CLI, config, schema, time, collections, tracing, testing. The question is never "should this use Effect?" but "which module already owns this?" Hand-rolled code where a module exists is a finding, even when it works.

## The dogma

1. **The edge is a Schema.** Whatever crosses into the program (a config file, a child's stdout, a JSON column, an HTTP body, a message off a queue) is decoded, never cast. A cast is a silent typo; a decode is an error with a path.
2. **Failures are values; defects are bugs.** Expected failures are tagged errors in the type. A defect means the program is wrong. Never `throw` inside an Effect, and never let work that can fail happen outside the `Exit` that settles it.
3. **Dependencies are services.** A thing with a lifetime (a connection, a server, a process runner, a clock) is a `Context.Service` built by a scoped `Layer` and provided once, at the edge. A test swaps the layer, not a parameter.
4. **Nothing ambient.** Time comes from `Clock`, randomness and digests from `Crypto`, files from `FileSystem`, children from the spawner. Then `TestClock` and a test layer control the world.
5. **Name the steps.** A reusable effectful function is `Effect.fn("name")`, so every step is a span and a stack frame. Use `Effect.gen` inline, combinators for behaviour (timeouts, retries, annotations), never a function that only wraps `Effect.gen`.
6. **A driver, not an owner.** Adopt a module as a tool under your own model (a SQL client under your facts), not as a second source of truth beside it (an event log next to your event log). Every adoption and every rejection says why.
7. **The source is the spec.** v4 is not v3 and not its release candidates. An API that is not in the pinned source does not exist, and a behaviour that matters (kill semantics, Windows paths) is probed, not assumed.

## The loop

1. **Find the pin.** Read the exact `effect` version from the catalog or `package.json`. Unstable modules (`@stability unstable`) can break in a minor, so the pin is exact.
2. **Get the source at that tag.** Keep a sparse clone at the pinned tag, holding `LLMS.md`, `ai-docs/`, `migration/`, `packages/effect/src`, and the platform and driver packages you use. In swell it lives at `.artifacts/ref/effect`; check its tag before trusting it.
3. **Ask which module owns it.** Before writing a helper, search `packages/effect/src` (and its unstable folders: `process`, `sql`, `http`, `http-api`, `cli`, `encoding`, `testing`) and `ai-docs/src` for the concept, not the function name you expect.
4. **Read three things.** The module's source and doc comment (including `@stability`), the `ai-docs` example, and a test in `packages/effect/test` that uses it. The test shows the real call shape.
5. **Compile what you doubt.** When a signature is uncertain, write a ten-line sketch against the pinned package and typecheck it before building on it.
6. **Probe what matters.** Run the behaviour you depend on, on the platform you ship to: does the timeout kill the grandchild, does `stat` have the field you compare on? Docs describe intent; a probe describes this machine.
7. **Record the verdict.** Write down what you adopted, what you rejected and why, and where Effect fell short, so the next reader does not relitigate it.

## Four examples that carry the rest

**Decode the edge.** A config cast let `limit: { perday: 1 }` load, and the rate limit quietly became infinite.

```ts
// before: a typo is a missing key, and a missing key is "no limit"
const spec = (await import(file)).default as ControlSpec;

// after: one Schema is the type and the check; an unknown key is an error naming its path
export const ControlSpec = Schema.Struct({ ... }).check(Schema.makeFilter(crossFieldChecks));
export type ControlSpec = typeof ControlSpec.Type;
const spec = yield* Schema.decodeUnknownEffect(ControlSpec)(mod.default, { errors: "all", onExcessProperty: "error" });
```

**Use the module that owns it.** A hand-rolled `execFile` with an `AbortSignal` killed the child and left its grandchild writing. The spawner kills the tree and waits before the timeout's fallback runs.

```ts
const run = Effect.fn("exec")(function* (argv: ReadonlyArray<string>, o: RunOptions) {
  const child = Effect.gen(function* () {
    const h = yield* spawner.spawn(
      ChildProcess.make(argv[0]!, argv.slice(1), { cwd: o.cwd, env: o.env, extendEnv: false }),
    );
    yield* Effect.all([drain(h.stdout), drain(h.stderr)], { concurrency: 2, discard: true });
    return Number(yield* h.exitCode);
  }).pipe(Effect.scoped); // the scope's release kills the whole tree
  return yield* child.pipe(
    Effect.timeoutOrElse({ duration: o.timeout, orElse: () => Effect.succeed(timedOut) }),
  );
});
```

**Settle everything inside the Exit.** Encoding an output after `Effect.exit` threw outside it, so the failure wrote no receipt and retried forever.

```ts
const settled = Effect.gen(function* () {
  const out = yield* rule.effect(subject);
  for (const a of out.append)
    if (!declared.has(a.fact.id)) return yield* new UndeclaredWrite({ fact: a.fact.id });
  return { outcome: "ok", append: yield* Effect.forEach(out.append, encode) } as const;
});
const exit = yield * Effect.exit(settled); // every failure above is now a value the caller records
```

**A service, provided once.** A controller with a `close()` method, wired by hand in every test, becomes a service whose layer owns its lifetime.

```ts
export class Controller extends Context.Service<Controller, ControllerService>()("swell/Controller") {
  static readonly layer = (opts: Options) => Layer.effect(Controller, makeController(opts)); // scoped: closes with the layer
}
// the edge provides it once; a test provides Historian.layer(":memory:") and a fake plant instead
program.pipe(Effect.provide(Controller.layer(opts).pipe(Layer.provide(Historian.layer(db)))));
```

## Smells

Each of these has a module that owns it: `as` at a boundary; `JSON.parse` without a schema; `Date.now()` or `new Date()`; `throw` or `try/finally` inside an Effect; `Effect.runPromise` in the middle of a program or a test; `execFile`, `spawn`, `fs.*`, `path.*` in Effect code; hand-written retries, timeouts, sleeps or polling; `Map`, `sort` and `reduce` folds that `Array`, `Record` and `Order` say directly; module-level mutable state standing in for a service; a test that waits on the wall clock.

## Where Effect falls short

Say so plainly, in the code and the project's notes, rather than routing around it in silence. Examples found so far (effect 4.0.2): `FileSystem.stat` reports no `ino` above 2^53, and NTFS file ids routinely are; `FileSystem.realPath` does not expand Windows 8.3 short names; there is no `homedir` or `hostname`.
