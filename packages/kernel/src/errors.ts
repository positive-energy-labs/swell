import { Schema } from "effect";

export class InvariantViolation extends Schema.TaggedError<InvariantViolation>()("InvariantViolation", {
  invariant: Schema.String,
  message: Schema.String,
}) {}

export class Unauthorized extends Schema.TaggedError<Unauthorized>()("Unauthorized", {
  need: Schema.String,
}) {}

export const CommandError = Schema.Union([InvariantViolation, Unauthorized]);
export type CommandError = InvariantViolation | Unauthorized;

export const violation = (invariant: string, message: string) =>
  new InvariantViolation({ invariant, message });
