/**
 * tide: evidence in, waves out. A tide is a set of kernel primitives compiled from one `tide.config.ts`:
 * sensors read a plant into readings, an issues rollup folds them by fingerprint, a loop proposes one change
 * per fingerprint when its evidence clears the bar, a verdict applies it. Node-only parts live in `./host`.
 */
export * from "./facts.ts";
export {
  ClockPort,
  Decide,
  defaultWhen,
  defineTide,
  evidenceHash,
  type LoopSpec,
  type ObservePayload,
  rulesOf,
  type TideSpec,
} from "./loop.ts";
export { observedOf, Peer, PeerPort, type PeerService, peerHttp, peerOf, urnOf } from "./peer.ts";
export {
  type ActuatorSpec,
  type Brief,
  type Changes,
  type Decision,
  fakePlant,
  fakeWorld,
  type FakeWorld,
  type Head,
  Plant,
  PlantPort,
  type PlantService,
  type PlantSpec,
  type Sensed,
  type SensorSpec,
} from "./plant.ts";
