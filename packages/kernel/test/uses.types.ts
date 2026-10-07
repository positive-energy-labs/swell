/** Type test with no runtime: `tsc` fails if either `@ts-expect-error` stops being an error. */
import { Context, Effect, Layer, Schema } from "effect";
import { Port, Rule } from "../src/index.ts";

const meta = {
  owner: "t",
  audience: "dev",
  layer: "core",
  ruled: false,
  open: "type test",
  label: "t",
  plain: "t",
} as const;
class Drive extends Context.Service<Drive, { readonly mk: Effect.Effect<string> }>()("types/Drive") {}
class Gmail extends Context.Service<Gmail, { readonly send: Effect.Effect<string> }>()("types/Gmail") {}
const drive = Layer.succeed(Drive, { mk: Effect.succeed("d") });
const gmail = Layer.succeed(Gmail, { send: Effect.succeed("g") });
const DrivePort = Port.make({ id: "types::drive", service: Drive, live: drive, fake: drive, meta });
Port.make({ id: "types::gmail", service: Gmail, live: gmail, fake: gmail, meta });

const base = {
  reads: [],
  triggers: [],
  subject: Schema.Struct({ urn: Schema.String }),
  want: () => Effect.succeed([]),
  meta,
} as const;

Rule.make({
  ...base,
  id: "types::ok",
  uses: [DrivePort],
  effect: () =>
    Effect.gen(function* () {
      const d = yield* Drive;
      return { result: yield* d.mk };
    }),
});

Rule.make({
  ...base,
  id: "types::undeclared",
  uses: [DrivePort],
  // @ts-expect-error Gmail is not in `uses`, so the effect's R channel is out of bounds.
  effect: () =>
    Effect.gen(function* () {
      const g = yield* Gmail;
      return { result: yield* g.send };
    }),
});
