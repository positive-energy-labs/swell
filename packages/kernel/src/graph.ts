import type { Meta } from "./meta.ts";
import type { Primitive } from "./registry.ts";

/** The registry in `system.json` declaration shape. Edges are not listed; `tools/graph` derives them from the reads/on/writes/calls/records/shows fields. */
// OPEN: "entry" means three things: the Entry primitive (a webhook door), the graph kind every
// command is drawn as, and the `time::entry` fact. Rename one before the words compound.
export type GraphKind = "actor" | "entry" | "fact" | "rule" | "effect" | "view";

export interface Actor {
  readonly id: string;
  readonly label: string;
  readonly plain: string;
  readonly agent?: boolean;
  readonly src: string;
}

export interface Story {
  readonly id: string;
  readonly name: string;
  readonly why: string;
  readonly steps: ReadonlyArray<{ readonly lights: ReadonlyArray<string>; readonly say: string }>;
}

export interface Node {
  readonly id: string;
  readonly kind: GraphKind;
  readonly label: string;
  readonly layer: string;
  readonly plain: string;
  readonly ruled: boolean;
  readonly src?: string;
  readonly open?: string;
  readonly agent?: boolean;
  readonly note?: string;
  readonly owner?: string;
  readonly audience?: string;
  readonly primitive?: string;
  readonly impl?: string;
  readonly reads?: ReadonlyArray<string>;
  readonly on?: ReadonlyArray<string>;
  readonly writes?: ReadonlyArray<string>;
  readonly calls?: ReadonlyArray<string>;
  readonly records?: ReadonlyArray<string>;
  readonly shows?: ReadonlyArray<string>;
}

const RECEIPT = "kernel::receipt";

const base = (id: string, kind: GraphKind, meta: Meta, primitive: string, impl: string, note?: string) => ({
  id,
  kind,
  label: meta.label,
  layer: meta.layer,
  plain: meta.plain,
  ruled: meta.ruled,
  ...(meta.ruled ? { src: meta.src } : { open: meta.open }),
  owner: meta.owner,
  audience: meta.audience,
  primitive,
  impl,
  ...(note ? { note } : {}),
});

const uniq = (xs: ReadonlyArray<string>) => [...new Set(xs)];
const ne = <K extends string>(k: K, xs: ReadonlyArray<string>) =>
  (xs.length > 0 ? { [k]: uniq(xs) } : {}) as unknown as { [P in K]?: ReadonlyArray<string> };

export const declare = (
  prims: ReadonlyArray<Primitive>,
  actors: ReadonlyArray<Actor>,
  stories: ReadonlyArray<Story>,
): { layers: object; kinds: object; nodes: ReadonlyArray<Node>; stories: ReadonlyArray<Story> } => {
  const usedPorts = new Set(
    prims.flatMap((p) => (p.kind === "rule" ? p.uses.map((u: { id: string }) => u.id) : [])),
  );
  const nodes: Array<Node> = actors.map((a) => ({
    id: a.id,
    kind: "actor",
    label: a.label,
    layer: "people",
    plain: a.plain,
    ruled: true,
    src: a.src,
    ...(a.agent ? { agent: true } : {}),
    ...ne(
      "writes",
      prims.flatMap((p) =>
        (p.kind === "command" && p.role === a.id) || (p.kind === "entry" && p.from === a.id) ? [p.id] : [],
      ),
    ),
  }));
  for (const p of prims) {
    const stubNote = p.impl === "stub" ? "stub: declared, not built" : undefined;
    switch (p.kind) {
      case "fact":
        nodes.push({
          ...base(p.id, "fact", p.meta, "fact", p.impl, [p.class, ...p.invariants].join("; ")),
        });
        break;
      case "command": {
        const writes = p.writes.map((f: { id: string }) => f.id);
        nodes.push({
          ...base(p.id, "entry", p.meta, "command", p.impl, stubNote ?? `role: ${p.role}`),
          ...ne(
            "reads",
            p.reads.map((f: { id: string }) => f.id).filter((id: string) => !writes.includes(id)),
          ),
          ...ne("writes", writes),
        });
        break;
      }
      case "entry":
        nodes.push({
          ...base(p.id, "entry", p.meta, "entry", p.impl, stubNote ?? `source: ${p.source}`),
          ...ne(
            "reads",
            p.reads.map((f) => f.id),
          ),
          ...ne(
            "writes",
            p.writes.map((f) => f.id),
          ),
        });
        break;
      case "rule": {
        const on = p.triggers.map((t: { on: string; fact?: { id: string }; clock?: { id: string } }) =>
          t.on === "fact" ? t.fact!.id : t.clock!.id,
        );
        nodes.push({
          ...base(p.id, "rule", p.meta, "rule", p.impl, stubNote),
          reads: uniq([...p.reads.map((f: { id: string }) => f.id), ...on, RECEIPT]),
          ...ne("on", on),
          ...ne(
            "writes",
            p.writes.map((f: { id: string }) => f.id),
          ),
          ...ne(
            "calls",
            p.uses.filter((u: { inbound: boolean }) => !u.inbound).map((u: { id: string }) => u.id),
          ),
        });
        break;
      }
      case "port":
        nodes.push({
          ...base(p.id, p.inbound ? "entry" : "effect", p.meta, "port", p.impl, stubNote),
          ...(!p.inbound && usedPorts.has(p.id) ? { records: [RECEIPT] } : {}),
        });
        break;
      case "projection":
        nodes.push({
          ...base(p.id, "view", p.meta, "projection", p.impl, stubNote),
          ...ne("reads", [...p.reads.map((f: { id: string }) => f.id), ...(p.readsRules ? ["@rules"] : [])]),
          ...ne("shows", p.shows),
        });
        break;
    }
  }
  return { layers: LAYERS, kinds: KINDS, nodes, stories };
};

export const LAYERS = {
  core: { label: "TC core", means: "The firm's own records and the rules that act on them." },
  pi: { label: "PI", means: "Project Intelligence: what happened elsewhere and what it seems to mean." },
  integrations: {
    label: "Integrations",
    means: "Doors to and from the outside world: Gmail, Drive, Xero, Chat, Revit.",
  },
  people: { label: "People", means: "The humans and agents who touch the system." },
};

export const KINDS = {
  entry: { label: "Entry point", means: "A door: a person's command, or something arriving from outside." },
  fact: { label: "Fact", means: "Something that happened or was promised, stored once and never rewritten." },
  rule: { label: "Rule", means: "A standing instruction: 'this should exist; if it does not, do it'." },
  effect: { label: "Effect", means: "A call out to another product (Drive, Gmail, Xero, Chat)." },
  view: { label: "View", means: "A screen a human reads. It only shows facts; it owns nothing." },
  actor: { label: "Person or agent", means: "Someone who acts on the system or reads it." },
};
