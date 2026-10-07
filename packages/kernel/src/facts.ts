import { Schema } from "effect";
import * as Fact from "./fact.ts";

/**
 * The kernel's own facts. Rules never write these: the kernel writes an attempt before it schedules
 * an effect and a receipt when the effect settles, so "what ran" has exactly one store.
 */
const kernel = {
  owner: "kernel",
  audience: "dev",
  layer: "core",
  ruled: true,
  src: "every rule gets a receipt, so the kernel owns attempts and receipts (Kai)",
} as const;

export const RuleEnabled = Fact.make({
  id: "kernel::rule-enabled",
  class: "kernel",
  fields: { rule: Schema.String },
  key: ["rule"],
  meta: {
    ...kernel,
    label: "Rule enabled",
    plain: "A person switched a rule on. It only acts on things that happened after this moment.",
  },
});

export const Attempt = Fact.make({
  id: "kernel::attempt",
  class: "kernel",
  fields: { rule: Schema.String, subject: Schema.String },
  key: ["rule", "subject"],
  // A rule's attempts since a time, for a rate limit that counts attempts rather than successes.
  indexes: { by_rule: ["rule", "at"] },
  meta: {
    ...kernel,
    label: "Attempt",
    plain: "The system started trying to do one thing a rule wants. It is written before the call goes out.",
  },
});

export const Receipt = Fact.make({
  id: "kernel::receipt",
  class: "kernel",
  fields: {
    rule: Schema.String,
    subject: Schema.String,
    attempt: Fact.Ref("kernel::attempt"),
    outcome: Schema.Literals(["ok", "failed"]),
    result: Schema.optionalKey(Schema.String),
    error: Schema.optionalKey(Schema.String),
  },
  key: ["rule", "subject"],
  invariants: ["a receipt settles exactly one attempt"],
  meta: {
    ...kernel,
    label: "Receipt",
    plain:
      "Proof that a rule's call happened, or that it failed and why. A failed one is retried; an ok one never is.",
  },
});

export const RetryGranted = Fact.make({
  id: "kernel::retry-granted",
  class: "kernel",
  fields: { rule: Schema.String, subject: Schema.String },
  key: ["rule", "subject"],
  meta: {
    ...kernel,
    ruled: false,
    open: "how a person retries a stuck item; the health page button is not designed",
    label: "Retry granted",
    plain: "A person told the system to try a stuck item again after it gave up.",
  },
});

export const KernelFacts = [RuleEnabled, Attempt, Receipt, RetryGranted] as const;
export type KernelFact = (typeof KernelFacts)[number];
