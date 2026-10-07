import { Effect, Option, Tracer } from "effect";

/**
 * W3C traceparent, passed by hand across every ctx.run* and scheduler hop: Convex runs each function
 * in its own isolate, so no AsyncLocalStorage survives the hop.
 */
export const format = (span: { readonly traceId: string; readonly spanId: string }): string =>
  `00-${span.traceId.padStart(32, "0").slice(-32)}-${span.spanId.padStart(16, "0").slice(-16)}-01`;

export const parse = (tp: string): Option.Option<Tracer.ExternalSpan> => {
  const m = /^00-([0-9a-f]{32})-([0-9a-f]{16})-0[01]$/.exec(tp);
  return m ? Option.some(Tracer.externalSpan({ traceId: m[1]!, spanId: m[2]! })) : Option.none();
};

export const current: Effect.Effect<Option.Option<string>> = Effect.currentSpan.pipe(
  Effect.map((s) => Option.some(format(s))),
  Effect.orElseSucceed(() => Option.none<string>()),
);

export const continueFrom =
  (tp: string | undefined) =>
  <A, E, R>(self: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
    Option.match(tp === undefined ? Option.none() : parse(tp), {
      onNone: () => self,
      onSome: (parent) => Effect.withParentSpan(self, parent),
    });
