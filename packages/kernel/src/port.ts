import type { Context, Layer } from "effect";
import type { Declared, Meta } from "./meta.ts";
import { register } from "./registry.ts";

/** An external system as an Effect service. Every port ships a `live` layer (a stub until wired) and a `fake` one, so the whole system runs as a simulator. */
export interface Port<Id extends string, I> extends Declared {
  readonly kind: "port";
  readonly id: Id;
  /** Undefined for ports Effect already carries in its default services (Clock). */
  readonly service: Context.Service<any, any> | undefined;
  readonly live: Layer.Layer<I>;
  readonly fake: Layer.Layer<I>;
  readonly inbound: boolean;
}

export type AnyPort = Port<string, any> | Port<string, never>;
export type ServiceOf<P> = P extends Port<string, infer I> ? I : never;

export const make = <const Id extends string, I>(def: {
  readonly id: Id;
  readonly service: Context.Service<I, any> | undefined;
  readonly live: Layer.Layer<I>;
  readonly fake: Layer.Layer<I>;
  readonly inbound?: boolean;
  readonly impl?: "real" | "stub";
  readonly meta: Meta;
}): Port<Id, I> =>
  register({
    kind: "port",
    impl: def.impl ?? "stub",
    id: def.id,
    service: def.service,
    live: def.live,
    fake: def.fake,
    inbound: def.inbound ?? false,
    meta: def.meta,
  });
