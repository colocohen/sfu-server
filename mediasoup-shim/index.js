/**
 * mediasoup → sfu-server drop-in.
 *
 * Install it under the name "mediasoup" with a package alias:
 *   npm i mediasoup@npm:sfu-server-mediasoup
 * and an application's `import * as mediasoup from 'mediasoup'` — or
 * Node 22+ `require('mediasoup')` — gets sfu-server's mediasoup v3 API.
 * No application code changes.
 */
import { mediasoup as compat } from 'sfu-server';

export const {
  createWorker, getSupportedRtpCapabilities, parseScalabilityMode,
  observer, version, workerBin, setLogEventListeners,
  errors, ortc, extras,
  UnsupportedError, NotFoundError, WorkerClosedError, InvalidStateError,
  Worker, Router, WebRtcServer, WebRtcTransport, Producer, Consumer,
} = compat;

// `mediasoup.types.*`: TS apps import types; JS apps occasionally touch
// error classes through it. Point it at the same namespace.
export const types = compat;
export default { ...compat, types: compat };
