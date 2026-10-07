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
export class DuplicateId extends Error {
  readonly id: string;
  constructor(id: string) {
    super(`kernel: primitive id '${id}' is registered twice`);
    this.id = id;
  }
}
export class InvalidId extends Error {
  readonly id: string;
  constructor(id: string) {
    super(`kernel: primitive id '${id}' is not 'namespace::kebab-name'`);
    this.id = id;
  }
}

const store = new Map<string, Primitive>();

export const register = <P extends { readonly kind: Kind; readonly id: string }>(p: P): P => {
  if (!ID.test(p.id)) throw new InvalidId(p.id);
  if (store.has(p.id)) throw new DuplicateId(p.id);
  store.set(p.id, p as unknown as Primitive);
  return p;
};

export const registry = (): ReadonlyArray<Primitive> => [...store.values()];

export const ofKind = <K extends Kind>(kind: K): ReadonlyArray<Extract<Primitive, { kind: K }>> =>
  registry().filter((p): p is Extract<Primitive, { kind: K }> => p.kind === kind);

export const lookup = (id: string): Primitive | undefined => store.get(id);
