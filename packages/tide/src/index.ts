/**
 * tide: evidence in, waves out. A tide is a set of kernel primitives compiled from one `tide.config.ts`:
 * sensors read a plant into readings, an issues rollup folds them by fingerprint, a loop proposes one change
 * per fingerprint when its evidence clears the bar, a verdict applies it. Node-only parts live in `./host`.
 */
export * from "./facts.ts";
export { ActGroup, BadRead, NoPeerToken, PageGroup, PeerAuth, PeerGroup, TideApi } from "./door.ts";
export {
  Decide,
  defaultWhen,
  defineTide,
  evidenceHash,
  type LoopSpec,
  type ObservePayload,
  rulesOf,
  type TideSpec,
} from "./loop.ts";
export {
  fieldsOf,
  observedOf,
  Peer,
  PeerError,
  PeerPort,
  type PeerService,
  peerHttp,
  peerOf,
  UnknownIndex,
  urnOf,
} from "./peer.ts";
export {
  type ActuatorSpec,
  type Brief,
  type Changes,
  type Decision,
  failedSensed,
  fakePlant,
  fakeWorld,
  type FakeWorld,
  type Head,
  Plant,
  PlantError,
  PlantPort,
  type PlantService,
  type PlantSpec,
  Sensed,
  SensedJson,
  type SensorSpec,
} from "./plant.ts";
