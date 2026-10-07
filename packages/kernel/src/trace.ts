import { Effect, Option, type Tracer } from "effect";
import { Headers, HttpTraceContext } from "effect/http";

/**
 * W3C traceparent, passed by hand across every ctx.run* and scheduler hop: Convex runs each function
 * in its own isolate, so no AsyncLocalStorage survives the hop. Effect's own propagation format, so the
 * sampled flag survives too.
 */
export const format = (span: Tracer.Span): string => HttpTraceContext.toHeaders(span)["traceparent"]!;

export const parse = (tp: string): Option.Option<Tracer.ExternalSpan> =>
  HttpTraceContext.w3c(Headers.fromRecordUnsafe({ traceparent: tp }));

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
