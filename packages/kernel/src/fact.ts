import { Schema } from "effect";
import type { Declared, Meta } from "./meta.ts";
import { register } from "./registry.ts";

/** All fact classes share one append-only discipline. `kernel` facts (attempt, receipt, rule enabled) belong to the kernel. */
export type FactClass = "event" | "commitment" | "reference" | "observed" | "inferred" | "kernel";

export const Envelope = {
  at: Schema.Number,
  by: Schema.String,
  /** Set when an agent wrote the fact: the person it acted for. */
  via: Schema.optionalKey(Schema.String),
  trace: Schema.optionalKey(Schema.String),
} as const;
type Envelope = typeof Envelope;

/** A minted id for a fact kind: a string branded with the fact id, so refs cannot cross kinds. */
export type Id<K extends string> = string & { readonly __id: K };
export const Ref = <K extends string>(kind: K): Schema.Codec<Id<K>, string> =>
  Schema.String.annotate({ identifier: `Id<${kind}>` }) as unknown as Schema.Codec<Id<K>, string>;

type FieldName<F extends Schema.Struct.Fields> = Extract<keyof F, string>;
export type Indexes<F extends Schema.Struct.Fields> = Readonly<
  Record<string, ReadonlyArray<FieldName<F> | "at">>
>;

export interface Fact<
  FactId extends string = string,
  Fields extends Schema.Struct.Fields = Schema.Struct.Fields,
  Ix extends Indexes<Fields> = Indexes<Fields>,
> extends Declared {
  readonly kind: "fact";
  readonly id: FactId;
  readonly table: string;
  readonly class: FactClass;
  readonly fields: Fields;
  readonly draft: Schema.Struct<Fields>;
  readonly row: Schema.Struct<Fields & Envelope>;
  readonly key: ReadonlyArray<FieldName<Fields>>;
  /** At most one row per key; the kernel checks it inside the writing mutation (serializable). */
  readonly unique: boolean;
  readonly indexes: Ix;
  readonly invariants: ReadonlyArray<string>;
}

export type AnyFact = Fact<string, any, any>;
export type Draft<F extends AnyFact> = F["draft"]["Type"];
export type Row<F extends AnyFact> = F["row"]["Type"] & {
  readonly _id: Id<F["id"]>;
  readonly _creationTime: number;
};
export type IndexName<F extends AnyFact> = Extract<keyof F["indexes"], string>;

export const tableOf = (id: string): string => id.replace("::", "_").replaceAll("-", "_");

export const make = <
  const FactId extends string,
  const Fields extends Schema.Struct.Fields,
  const Ix extends Indexes<Fields> = {},
>(def: {
  readonly id: FactId;
  readonly class: FactClass;
  readonly fields: Fields;
  readonly key?: ReadonlyArray<FieldName<Fields>>;
  readonly unique?: boolean;
  readonly indexes?: Ix;
  readonly invariants?: ReadonlyArray<string>;
  readonly meta: Meta;
}): Fact<FactId, Fields, Ix & { readonly by_key: ReadonlyArray<FieldName<Fields>> }> => {
  const key = def.key ?? [];
  if (def.unique && key.length === 0) throw new Error(`${def.id}: unique needs a key`);
  return register({
    kind: "fact",
    impl: "real",
    id: def.id,
    table: tableOf(def.id),
    class: def.class,
    fields: def.fields,
    draft: Schema.Struct(def.fields),
    row: Schema.Struct({ ...def.fields, ...Envelope } as Fields & Envelope),
    key,
    unique: def.unique ?? false,
    indexes: { ...def.indexes, ...(key.length > 0 ? { by_key: key } : {}) } as Ix & {
      readonly by_key: ReadonlyArray<FieldName<Fields>>;
    },
    invariants: def.invariants ?? [],
    meta: def.meta,
  });
};
