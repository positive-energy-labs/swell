/**
 * The kernel: six primitives. Each is a value made by a constructor that registers it, so the
 * registry and the static types are the same values. The law: primitives never call
 * primitives; they meet only through facts (Db) and ports (Effect services).
 */
export * as Fact from "./fact.ts";
export * as Command from "./command.ts";
export * as Entry from "./entry.ts";
export * as Rule from "./rule.ts";
export * as Port from "./port.ts";
export * as Projection from "./projection.ts";
export * as Trace from "./trace.ts";
export * as Graph from "./graph.ts";
export * as Kernel from "./facts.ts";
export * as Sweep from "./sweep.ts";
export * as Memory from "./memory.ts";

export type { Db, Find, Reader, Writer } from "./db.ts";
export { MAX_LIMIT } from "./db.ts";
export type { Actor, Agent } from "./command.ts";
export type { AnyPort } from "./port.ts";
export { type EntryCtx, Source } from "./entry.ts";
export type { AnyRule } from "./rule.ts";
export type { AnyFact, Draft, Id, Row } from "./fact.ts";
export type { Declared, GraphLayer, Impl, Kind, Meta, Ruling } from "./meta.ts";
export { fnName, groupName } from "./meta.ts";
export { CommandError, InvariantViolation, Unauthorized, violation } from "./errors.ts";
export { DuplicateId, InvalidId, lookup, ofKind, registry, scoped, type Primitive } from "./registry.ts";
export { latest, makeDb, makeReader, settle, type Store, transact, type WriteCtx } from "./store.ts";
export { isStub, NotImplemented, stub } from "./stub.ts";
export { enablerOf, Job, UndeclaredWrite } from "./sweep.ts";
export type { SimulatorOptions } from "./memory.ts";
