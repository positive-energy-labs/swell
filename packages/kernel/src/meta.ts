/** Metadata every primitive carries. The architecture graph draws it for people, not the compiler. A domain narrows layer and audience. */
/** The graph layer a primitive is drawn in. Not an Effect Layer. */
export type GraphLayer = string;
export type Audience = string;

export type Ruling =
  | { readonly ruled: true; readonly src: string }
  | { readonly ruled: false; readonly open: string };

export type Meta = Ruling & {
  readonly label: string;
  readonly plain: string;
  readonly owner: string;
  readonly audience: Audience;
  readonly layer: GraphLayer;
};

export type Impl = "real" | "stub";

export type Kind = "fact" | "command" | "entry" | "rule" | "port" | "projection";

export interface Declared {
  readonly kind: Kind;
  readonly id: string;
  readonly impl: Impl;
  readonly meta: Meta;
}

export const ID = /^[a-z][a-z0-9]*::[a-z][a-z0-9-]*$/;

type Camel<S extends string> = S extends `${infer H}-${infer T}` ? `${H}${Capitalize<Camel<T>>}` : S;
/** `time::submit-week` -> `submitWeek`, at the type level so Confect refs stay literal. */
export type FnName<Id extends string> = Id extends `${string}::${infer N}` ? Camel<N> : never;
export type GroupName<Id extends string> = Id extends `${infer G}::${string}` ? G : never;

export const fnName = <Id extends string>(id: Id): FnName<Id> =>
  id.split("::")[1]!.replace(/-([a-z0-9])/g, (_, c: string) => c.toUpperCase()) as FnName<Id>;
export const groupName = <Id extends string>(id: Id): GroupName<Id> => id.split("::")[0] as GroupName<Id>;
