import { InvariantViolation, Unauthorized } from "@swell/kernel";
import { Context, Schema } from "effect";
import {
  HttpApi,
  HttpApiEndpoint,
  HttpApiGroup,
  HttpApiMiddleware,
  HttpApiSchema,
  HttpApiSecurity,
} from "effect/http-api";
import { Signature } from "./facts.ts";
import { Decide, Retry } from "./loop.ts";

/**
 * The controller's door, one contract the server and the peer client both derive from: route names, the wire
 * encoding of `eq`/`gte`/`lt`, the limit bound, auth, and error statuses cannot drift apart.
 */
const Value = Schema.Union([Schema.String, Schema.Number, Schema.Boolean]);
const Limit = Schema.optional(Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 2048 })));
const Wire = <S extends Schema.Constraint>(s: S) => Schema.optional(Schema.fromJsonString(s));
const err = <T extends string>(tag: T, httpApiStatus: number) =>
  Schema.TaggedError<{ _tag: T }>()(tag, { message: Schema.String }, { httpApiStatus });

export class NoToken extends err("NoToken", 401) {}
export class BadRead extends err("BadRead", 400) {}

/**
 * A peer controller's bearer: reads only. `requiredForClient` turns a peer that forgot its token into a
 * missing-layer compile error. With no peer token configured, the door refuses every peer.
 */
export class PeerAuth extends HttpApiMiddleware.Service<PeerAuth>()("swell/PeerAuth", {
  requiredForClient: true,
  security: { bearer: HttpApiSecurity.bearer },
  error: NoToken,
}) {}

/** Who is deciding: resolved from the credential, never from the request body. */
export class Operator extends Context.Service<Operator, { readonly name: string }>()("swell/Operator") {}

/** An operator's bearer: each token names one operator, and that name is the only one its holder decides as. */
export class OperatorAuth extends HttpApiMiddleware.Service<OperatorAuth, { provides: Operator }>()(
  "swell/OperatorAuth",
  { security: { bearer: HttpApiSecurity.bearer }, error: NoToken },
) {}

export const Rows = Schema.Array(Schema.Record(Schema.String, Schema.Unknown));
export const Tallies = Schema.Array(
  Schema.Struct({ key: Schema.String, value: Schema.Record(Schema.String, Schema.Number) }),
);

/** What one controller may read of another: facts by index and tallies by range, both bounded. Never a write. */
export class PeerGroup extends HttpApiGroup.make("peer")
  .add(
    HttpApiEndpoint.get("facts", "/facts/:table", {
      params: { table: Schema.String },
      query: {
        index: Schema.optional(Schema.String),
        eq: Wire(Schema.Array(Value)),
        gte: Wire(Value),
        lt: Wire(Value),
        order: Schema.optional(Schema.Literals(["asc", "desc"])),
        limit: Limit,
      },
      success: Rows,
      error: BadRead,
    }),
    HttpApiEndpoint.get("tallies", "/tallies/:id", {
      params: { id: Schema.String },
      query: { gte: Schema.optional(Schema.String), lt: Schema.optional(Schema.String), limit: Limit },
      success: Tallies,
    }),
  )
  .middleware(PeerAuth) {}

export const Plan = Schema.Struct({
  rule: Schema.String,
  enabled: Schema.Boolean,
  wanted: Schema.Number,
  done: Schema.Number,
  inflight: Schema.Number,
  pending: Schema.Array(
    Schema.Struct({ subject: Schema.Unknown, urn: Schema.String, failures: Schema.Number }),
  ),
  dead: Schema.Array(Schema.Struct({ urn: Schema.String, failures: Schema.Number, error: Schema.String })),
});

export const ProposalView = Schema.Struct({
  loop: Schema.String,
  subject: Schema.String,
  signature: Schema.String,
  arming: Schema.Number,
  operator: Schema.String,
  text: Schema.String,
  cites: Schema.Array(Schema.String),
  sources: Schema.Array(Schema.String),
  at: Schema.Number,
  verdict: Schema.NullOr(
    Schema.Struct({ accept: Schema.Boolean, text: Schema.String, cite: Schema.String, at: Schema.Number }),
  ),
});

export const View = Schema.Struct({
  plant: Schema.String,
  now: Schema.Number,
  signatures: Schema.Array(Signature),
  proposals: Schema.Array(ProposalView),
  health: Schema.Array(Plan),
});

/** The HMI's reads: a gauge, no auth. */
export class HmiGroup extends HttpApiGroup.make("hmi").add(
  HttpApiEndpoint.get("view", "/view", {
    query: { plant: Schema.optional(Schema.String) },
    success: View,
    error: BadRead,
  }),
  HttpApiEndpoint.get("health", "/health", { success: Schema.Array(Plan) }),
) {}

const refused = [
  InvariantViolation.pipe(HttpApiSchema.status(422)),
  Unauthorized.pipe(HttpApiSchema.status(403)),
];

/** The operator's verbs, decoded from the commands' own arg schemas, as the operator the token names. */
export class OperatorGroup extends HttpApiGroup.make("operator")
  .add(
    HttpApiEndpoint.post("decide", "/decide", {
      payload: Schema.Struct(Decide.args),
      success: Schema.Struct({ verdict: Schema.String }),
      error: refused,
    }),
    HttpApiEndpoint.post("retry", "/retry", {
      payload: Schema.Struct(Retry.args),
      success: Schema.Struct({ grant: Schema.String }),
      error: refused,
    }),
  )
  .middleware(OperatorAuth) {}

export const ControllerApi = HttpApi.make("swell").add(PeerGroup).add(HmiGroup).add(OperatorGroup);
