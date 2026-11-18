/** Runtime side of 'mediasoup/types'. Apps import it `import type`, which
 *  erases at build; anything that imports it for real gets the same
 *  namespace (error classes live here in real mediasoup too). */
import { mediasoup as compat } from 'sfu-server';
export const {
  UnsupportedError, NotFoundError, WorkerClosedError, InvalidStateError,
  Worker, Router, WebRtcServer, WebRtcTransport, Producer, Consumer,
} = compat;
export default compat;
