/**
 * swell: evidence in, moves out. A control spec compiles into kernel primitives: sensors and observers measure
 * a plant into measurements, a signatures rollup folds their signals, a loop proposes one move per signature per
 * arming when its evidence crosses the threshold, an operator's verdict applies it. Node-only parts live in
 * `./controller`; test doubles in `./testing`.
 */
export * from "./facts.ts";
export {
  BadRead,
  ControllerApi,
  HmiGroup,
  NoToken,
  Operator,
  OperatorAuth,
  OperatorGroup,
  PeerAuth,
  PeerGroup,
  Plan,
  ProposalView,
  View,
} from "./door.ts";
export { Decide, defaultThreshold, FeedbackPayload, Retry, rulesOf, subjectOf } from "./loop.ts";
export {
  ActuatorSpec,
  ControlSpec,
  decodeControl,
  defineControl,
  GitPlantSpec,
  LoopSpec,
  Mode,
  ObserverSpec,
  PlantSpec,
  SensorSpec,
} from "./spec.ts";
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
  Brief,
  Changes,
  Decision,
  type Instrument,
  Measured,
  MeasuredJson,
  measureFailed,
  Plant,
  PlantError,
  PlantPort,
  type PlantService,
  Sampled,
} from "./plant.ts";
