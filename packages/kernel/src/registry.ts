import { Data, Effect, type Scope } from "effect";
import type { Command } from "./command.ts";
import type { Entry } from "./entry.ts";
import type { Fact } from "./fact.ts";
import { ID, type Kind } from "./meta.ts";
import type { Port } from "./port.ts";
import type { Projection } from "./projection.ts";
import type { Rule } from "./rule.ts";

export type Primitive =
  | Fact<any, any, any>
  | Command<any, any, any, any, any>
  | Entry
  | Rule<any, any, any, any, any>
  | Port<any, any>
  | Projection<any, any, any, any>;

/** Thrown at module load: a clash is a boot failure, never a runtime surprise. */
export class DuplicateId extends Data.TaggedError("DuplicateId")<{ readonly id: string }> {
  override get message() {
    return `kernel: primitive id '${this.id}' is registered twice`;
  }
}
export class InvalidId extends Data.TaggedError("InvalidId")<{ readonly id: string }> {
  override get message() {
    return `kernel: primitive id '${this.id}' is not 'namespace::kebab-name'`;
  }
}

const store = new Map<string, Primitive>();

export const register = <P extends { readonly kind: Kind; readonly id: string }>(p: P): P => {
  if (!ID.test(p.id)) throw new InvalidId({ id: p.id });
  if (store.has(p.id)) throw new DuplicateId({ id: p.id });
  store.set(p.id, p as unknown as Primitive);
  return p;
};

/**
 * Primitives declared at module load live for the process. Ones compiled at runtime from a config (a
 * controller's rules) are forgotten when the scope that compiled them closes, so a reload or a second
 * controller in one process can declare the same ids again.
 */
export const scoped = <A>(
  compile: () => A,
  ids: (a: A) => ReadonlyArray<string>,
): Effect.Effect<A, never, Scope.Scope> =>
  Effect.acquireRelease(Effect.sync(compile), (a) =>
    Effect.sync(() => {
      for (const id of ids(a)) store.delete(id);
    }),
  );

export const registry = (): ReadonlyArray<Primitive> => [...store.values()];

export const ofKind = <K extends Kind>(kind: K): ReadonlyArray<Extract<Primitive, { kind: K }>> =>
  registry().filter((p): p is Extract<Primitive, { kind: K }> => p.kind === kind);

export const lookup = (id: string): Primitive | undefined => store.get(id);
