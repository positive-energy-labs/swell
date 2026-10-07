import { Schema } from "effect";

export class NotImplemented extends Schema.TaggedError<NotImplemented>()("NotImplemented", {
  id: Schema.String,
  intent: Schema.String,
}) {}

const STUB = Symbol.for("swell/kernel/stub");
/** A branded placeholder, not a function, so it never blurs the contextual type of a real body. */
export interface Stub {
  readonly [STUB]: string;
}

/** `stub("intent")` stands where a body will go. Calling it dies with the intent, so a stub can never pass for done. */
export const stub = (intent: string): Stub => ({ [STUB]: intent });

export const isStub = (f: unknown): f is Stub => typeof f === "object" && f !== null && STUB in f;
export const intentOf = (f: Stub): string => f[STUB];
