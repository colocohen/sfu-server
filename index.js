/**
 * sfu-server — Selective Forwarding Unit for Node.js.
 * Multi-party WebRTC routing over webrtc-server connections.
 *
 * Two front doors, one engine:
 *   native   — Room / Router (+ stable-webrtc as the connection layer)
 *   compat   — a drop-in mediasoup v3 server API
 */
export { Router, Room } from './src/sfu.js';
export { getRouterRtpCapabilities } from './src/rtp_capabilities.js';

// mediasoup drop-in:  import { mediasoup } from 'sfu-server'
//                     const worker = await mediasoup.createWorker();
export * as mediasoup from './src/mediasoup_compat.js';

// ortc arithmetic is public the way mediasoup's is (mediasoup.ortc):
export * as ortc from './src/ortc.js';
