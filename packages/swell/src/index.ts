/**
 * swell: evidence in, moves out. A control spec compiles into kernel primitives: sensors and observers measure
 * a plant into measurements, a signatures rollup folds their signals, a loop proposes one move per signature
 * when its evidence crosses the threshold, an operator's verdict applies it. Node-only parts live in `./controller`.
 */
export * from "./facts.ts";
export { BadRead, ControllerApi, DoorAuth, HmiGroup, NoToken, OperatorGroup, PeerGroup } from "./door.ts";
export {
  type ControlSpec,
  Decide,
  defaultThreshold,
  defineControl,
  evidenceHash,
  type FeedbackPayload,
  type LoopSpec,
  rulesOf,
} from "./loop.ts";
export {
  citeOf,
  fieldsOf,
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
  fakePlant,
  fakeWorld,
  type FakeWorld,
  type Instrument,
  Measured,
  MeasuredJson,
  measureFailed,
  type ObserverSpec,
  Plant,
  PlantError,
  PlantPort,
  type PlantService,
  type PlantSpec,
  type Sampled,
  type SensorSpec,
} from "./plant.ts";
