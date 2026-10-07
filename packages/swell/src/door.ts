import { InvariantViolation, Unauthorized } from "@swell/kernel";
import { Schema } from "effect";
import {
  HttpApi,
  HttpApiEndpoint,
  HttpApiGroup,
  HttpApiMiddleware,
  HttpApiSchema,
  HttpApiSecurity,
} from "effect/http-api";
import { Decide } from "./loop.ts";

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

/** Bearer auth. `requiredForClient` turns a peer that forgot its token into a missing-layer compile error. */
export class DoorAuth extends HttpApiMiddleware.Service<DoorAuth>()("swell/DoorAuth", {
  requiredForClient: true,
  security: { bearer: HttpApiSecurity.bearer },
  error: NoToken,
}) {}

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
  .middleware(DoorAuth) {}

/** The HMI's reads: a gauge, no auth. */
export class HmiGroup extends HttpApiGroup.make("hmi").add(
  HttpApiEndpoint.get("view", "/view", {
    query: { plant: Schema.optional(Schema.String) },
    success: Schema.Unknown,
  }),
  HttpApiEndpoint.get("health", "/health", { success: Schema.Unknown }),
) {}

/** The operator's one verb, decoded from the command's own arg schemas, under the same token as the peer door. */
export class OperatorGroup extends HttpApiGroup.make("operator")
  .add(
    HttpApiEndpoint.post("decide", "/decide", {
      payload: Schema.Struct({ ...Decide.args, operator: Schema.String }),
      success: Schema.Struct({ verdict: Schema.String }),
      error: [
        InvariantViolation.pipe(HttpApiSchema.status(422)),
        Unauthorized.pipe(HttpApiSchema.status(403)),
      ],
    }),
  )
  .middleware(DoorAuth) {}

export const ControllerApi = HttpApi.make("swell").add(PeerGroup).add(HmiGroup).add(OperatorGroup);
