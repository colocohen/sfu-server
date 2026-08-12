<p align="center">
  <img src="https://github.com/colocohen/sfu-server/raw/main/sfu-server.svg" width="450" alt="sfu-server"/>
</p>

<h1 align="center">SFU-SERVER</h1>
<p align="center">
  <em>Multi-party WebRTC routing for Node.js</em>
</p>

<p align="center">
  <a href="https://www.npmjs.com/package/sfu-server">
    <img src="https://img.shields.io/npm/v/sfu-server?color=blue" alt="npm">
  </a>
  <a href="https://github.com/colocohen/sfu-server">
    <img src="https://img.shields.io/github/stars/colocohen/sfu-server?style=social" alt="github">
  </a>
  <a href="https://github.com/colocohen/sfu-server/issues">
    <img src="https://img.shields.io/github/issues/colocohen/sfu-server?color=orange" alt="issues">
  </a>
  <img src="https://img.shields.io/github/license/colocohen/sfu-server?color=brightgreen" alt="license">
</p>

A Selective Forwarding Unit for Node.js. Route audio, video and data between
any number of participants — **without transcoding, without native bindings,
and without anything to compile**.

Pass it a peer connection, and it does the rest: every participant sees every
other one, simulcast layers are chosen per viewer, keyframes are requested
when they are actually needed, and streams that come and go are re-wired as
they do.

Also ships a **complete mediasoup v3 server API** — 187 methods and 67 events,
measured against mediasoup 3.24.2 on every test run. The official mediasoup
demo runs on it unmodified; see [Migration from mediasoup](#migration-from-mediasoup)
for how to switch.

## Table of Contents

- [Features](#features)
- [Install](#install)
- [Quick start](#quick-start)
  - [A conference room in fifteen lines](#a-conference-room-in-fifteen-lines)
  - [A complete server you can run](#a-complete-server-you-can-run)
  - [Routing by hand](#routing-by-hand)
  - [Recording and broadcasting with ffmpeg](#recording-and-broadcasting-with-ffmpeg)
- [How this differs from mediasoup](#how-this-differs-from-mediasoup)
- [API](#api)
  - [Room](#room)
  - [Router](#router)
  - [Producers and consumers](#producers-and-consumers)
  - [DataChannels](#datachannels)
  - [Observers](#observers)
  - [Statistics](#statistics)
  - [PlainTransport](#plaintransport)
  - [Native API reference](#native-api-reference)
- [Using the mediasoup API](#using-the-mediasoup-api)
  - [Entities](#entities)
  - [Options that matter](#options-that-matter)
  - [Not the same as mediasoup](#not-the-same-as-mediasoup)
- [Migration from mediasoup](#migration-from-mediasoup)
- [Signaling is yours](#signaling-is-yours)
- [Simulcast and layer selection](#simulcast-and-layer-selection)
- [Codec support](#codec-support)
- [Why pure JavaScript matters](#why-pure-javascript-matters)
- [Architecture](#architecture)
- [Scaling](#scaling)
- [Performance](#performance)
- [Debugging](#debugging)
- [Troubleshooting](#troubleshooting)
- [Status and known gaps](#status-and-known-gaps)
- [Interoperability](#interoperability)
- [RFC compliance](#rfc-compliance)
- [Use cases](#use-cases)
- [Sponsors](#-sponsors)
- [License](#-license)

## Features

- **Selective forwarding** — packets are routed, never decoded or re-encoded; CPU cost stays flat as participants grow
- **Simulcast** — spatial and temporal layer selection per viewer, with automatic bandwidth adaptation (RFC 8853, RID-based)
- **Codecs** — VP8, VP9, H.264, H.265, AV1 video; Opus audio. Keyframe detection per codec, so a viewer is never left waiting on a decode point that already went by
- **DataChannels** — routed between participants over SCTP, with per-message subchannel addressing
- **Active speaker detection** — RFC 6464 audio levels, exposed as observers
- **PlainTransport** — plain RTP over UDP in *both* directions: pipe a room into ffmpeg for recording or RTMP, or feed an external stream in
- **PipeTransport** — connect routers together; participants split across routers still see each other
- **mediasoup v3 API** — the complete server surface, as a drop-in
- **Pure JavaScript** — no worker binaries, no Python, no C++ toolchain; `npm install` and run

## Install

```bash
npm install sfu-server
```

That is the whole install. `webrtc-server` and `rtp-packet` are pulled in
automatically; there is nothing else to add, and nothing to compile on any
platform.

**Requirements:**

- **Node.js 18 or newer.** Developed and tested on Node 22 and 26; earlier versions are expected to work.
- **Windows, Linux and macOS.** Windows and Linux are tested; macOS is expected to work (Node is Node). No native compilation on any of them.

## Quick start

### A conference room in fifteen lines

A `Room` is the "everybody sees everybody" policy. Hand it a peer connection
per participant; it handles the rest.

```js
import { RTCPeerConnection } from 'webrtc-server';
import { Room } from 'sfu-server';

const room = new Room();

signaling.on('participant', async (socket) => {
  const pc = new RTCPeerConnection();

  room.join(pc);          // ← that is the whole integration

  // ...your usual offer/answer exchange with this participant...
});
```

Everything after that is automatic: a participant who publishes a camera is
consumed by everyone already in the room, a participant who joins later is
consumed by — and consumes — everyone present, and a participant who leaves
takes their streams with them.

### A complete server you can run

The fragments above leave out signalling, which is the part every reader has
to write anyway. Here is the whole thing — a working conference server in one
file, with a WebSocket for signalling:

```js
import { WebSocketServer } from 'ws';
import { RTCPeerConnection } from 'webrtc-server';
import { Room } from 'sfu-server';

const room = new Room();
const wss  = new WebSocketServer({ port: 8080 });

room.on('error', (err, ctx) => console.error('routing failed:', err.message, ctx));

wss.on('connection', (socket) => {
  const pc = new RTCPeerConnection();
  room.join(pc);

  // Server → client
  pc.onicecandidate = ({ candidate }) => {
    if (candidate) socket.send(JSON.stringify({ candidate }));
  };

  // The SFU adds transceivers as other participants publish, which makes
  // the connection need renegotiation. Offer whenever it says so.
  pc.onnegotiationneeded = async () => {
    const offer = await pc.createOffer();
    await pc.setLocalDescription(offer);
    socket.send(JSON.stringify({ sdp: pc.localDescription }));
  };

  // Client → server
  socket.on('message', async (raw) => {
    const msg = JSON.parse(raw);

    if (msg.sdp) {
      await pc.setRemoteDescription(msg.sdp);
      if (msg.sdp.type === 'offer') {
        const answer = await pc.createAnswer();
        await pc.setLocalDescription(answer);
        socket.send(JSON.stringify({ sdp: pc.localDescription }));
      }
    } else if (msg.candidate) {
      await pc.addIceCandidate(msg.candidate);
    }
  });

  socket.on('close', () => pc.close());
});

console.log('conference server on ws://localhost:8080');
```

The browser side is ordinary WebRTC — `getUserMedia`, `addTrack`, the same
offer/answer loop, and `ontrack` to display what arrives. Nothing about the
client is specific to this library.

**Watch for `onnegotiationneeded`.** It is the one piece people miss: an SFU
adds and removes transceivers as participants come and go, so the connection
renegotiates repeatedly over its lifetime, not just at the start. Handle it
and joins mid-call work; ignore it and only the first participant ever
appears.

### Routing by hand

`Room` is a policy on top of `Router`. When "everybody sees everybody" is not
what you want — a webinar where only the presenter is seen, a spatial room
where you consume only nearby participants — use the `Router` directly and
decide the pairings yourself.

```js
import { Router } from 'sfu-server';

const router = new Router();

const presenter = router.addPeer(presenterPc);
const viewer    = router.addPeer(viewerPc);

router.on('producer', (producer, owner) => {
  if (owner.id !== presenter.id) return;   // only the presenter is forwarded

  viewer.consume(producer, (err, consumer) => {
    if (err) return console.error(err);
    console.log('viewer is receiving', producer.kind);
  });
});
```

### Recording and broadcasting with ffmpeg

An SFU forwards RTP; it does not record files or speak RTMP. What it gives you
is the stream, and ffmpeg does the rest — the same division of labour
mediasoup uses.

```js
import { mediasoup } from 'sfu-server';

const transport = await router.createPlainTransport({
  listenIp: { ip: '127.0.0.1' },
});

await transport.connect({ ip: '127.0.0.1', port: 5004 });
const consumer = await transport.consume({ producerId: producer.id });

// consumer.rtpParameters describes exactly what will arrive on port 5004 —
// write it into an SDP file and point ffmpeg at it:
//   ffmpeg -protocol_whitelist file,udp,rtp -i input.sdp out.mp4
//   ffmpeg -protocol_whitelist file,udp,rtp -i input.sdp -f flv rtmp://...
```

The same transport works in reverse: `transport.produce()` turns arriving RTP
into a producer, so an ffmpeg process, an IP camera or an external broadcast
becomes a participant in the room.

## How this differs from mediasoup

|  | sfu-server | mediasoup |
|---|---|---|
| Runtime | pure JavaScript | C++ worker + Node.js API |
| Install | `npm install` | requires Python and a C++ toolchain |
| Windows | works out of the box | often painful (MSBuild, Python, build failures) |
| Debugging | `console.log`, single process | gdb into a C++ subprocess |
| API | mediasoup v3 drop-in, plus a smaller native one | native |
| Production maturity | new | since 2018 |
| CPU footprint | higher (JS packet path) | lower (native) |
| Ecosystem | one server package | server + client libraries + tooling |

The two are not competing designs; sfu-server was built to *be* mediasoup
where the ergonomics of pure JavaScript matter — `npm install` on Windows, a
readable packet path, a container without a build step. If none of that is a
problem for you, mediasoup is more mature and lower-cost per stream.

## API

Two front doors onto one engine. The **native** API (`Room`, `Router`) is
smaller and shaped around what an SFU actually does; the **mediasoup** API is
the complete v3 server surface, for applications and tooling that already
speak it. Both drive the same routing engine — mixing them in one process is
fine, mixing them on one router is not.

### Room

The "everybody sees everybody" policy, which is what most conferencing
applications want. It is a thin layer over `Router`, and reading it is the
fastest way to understand the engine.

```js
const room = new Room();

const peer = room.join(pcOrPeer);   // returns the peer handle
room.close();                       // closes every peer and the router
```

`join()` accepts a `webrtc-server` `RTCPeerConnection`, or a `stable-webrtc`
peer wrapper. It may be called **before** the connection has negotiated
anything — a peer that joins mid-negotiation, or before its `pc` has even
finished being born, is held as state until it is ready rather than rejected.

| event | fires when | arguments |
|---|---|---|
| `producer` | a participant starts sending a stream | `(producer)` |
| `consumer` | a stream was routed to a participant | `(consumer)` |
| `error` | a routing attempt failed | `(err, { peer, producer })` |

The `error` event matters: consuming can fail for reasons that are nobody's
bug (a participant left mid-setup, a codec had no overlap), and the room
carries on. Listen to it in development — a silent room usually has an error
nobody subscribed to.

### Router

The routing engine. `Room` uses it; use it directly when the pairing is a
decision your application wants to make — a webinar where only the presenter
is forwarded, a spatial room where you consume only nearby participants, a
moderated stage.

```js
const router = new Router();

const peer = router.addPeer(pcOrPeer);
router.peers;                            // current peer handles
router.close();
```

**Peer handle**

```js
peer.id;
peer.consume(producer, (err, consumer) => {});   // route a stream to this peer
peer.close();
```

`consume()` is callback-based on purpose: it resolves when the route is
actually **bound** — the producer's media is flowing and packets are being
forwarded — not when the parameters were computed. That is later than
mediasoup's promise resolves, and it is the moment your application usually
cares about.

| event | fires when | arguments |
|---|---|---|
| `peer` | a participant attached and is ready | `(peer)` |
| `producer` | a participant started sending | `(producer, owner)` |
| `producerclose` | that stream ended | `(producer)` |
| `consumer` | a route was bound | `(consumer)` |
| `consumerclose` | a route ended | `(consumer)` |

**Feeding RTP in from outside a peer connection** — a decoder, a file, a
test generator:

```js
const sink = router.addPlainPeer({ sendRtp: (buf) => socket.send(buf, port, ip) });
const producer = router.addPlainProducer({
  peerHandle: sink, kind: 'video', ssrc: 0x1234, codecName: 'VP8',
});
producer.push(rtpPacket);   // feed packets in
```

This is the primitive `PlainTransport` is built on, exposed for applications
that want the pipe without the mediasoup object model.

### Producers and consumers

A **producer** is a stream a participant sends in. A **consumer** is that
stream forwarded to someone else. The native API hands you plain handles:

```js
producer.id;
producer.kind;                 // 'audio' | 'video'
producer.peerId;               // who is sending it
producer.requestKeyframe();
producer.close();

consumer.id;
consumer.producerId;
consumer.pause();              // stop forwarding, keep the route
consumer.resume();             // resume; a fresh keyframe is requested for you
consumer.getStats();           // { forwarded, bound, paused, layerSsrc, spatialLayer }
consumer.setPreferredSpatialLayer(0);    // cap the resolution this viewer gets
consumer.setPreferredTemporalLayer(1);   // cap the frame rate
consumer.currentSpatialLayer;            // what is actually being sent
consumer.currentTemporalLayer;
consumer.close();
```

The mediasoup API exposes the same streams through its own objects, with
mediasoup's names and shapes — `consumer.setPreferredLayers({ spatialLayer,
temporalLayer })`, `consumer.requestKeyFrame()`, `producer.pause()`,
`getStats()` returning mediasoup's stat objects, and the full event set. The
two are views onto the same engine, not two implementations; pick the one that
fits your application and stay in it.

### DataChannels

Data is routed like media: a participant's channel becomes a producer, and
everyone else gets a consumer of it. Through the mediasoup API:

```js
const dataProducer = await transport.produceData({ label: 'chat' });
const dataConsumer = await otherTransport.consumeData({
  dataProducerId: dataProducer.id,
});

dataConsumer.on('message', (data, ppid) => { /* ... */ });
```

Reliability settings travel with the stream: an unordered, partially-reliable
chat channel stays unordered and partially reliable for every consumer of it,
because a consumer that silently upgraded to reliable would add latency the
sender deliberately traded away.

**Subchannels** narrow the fan-out without opening more channels. A consumer
subscribes to a set of numeric subchannels, and a message addressed to one
reaches only those consumers — per-room or per-role routing over a single
producer:

```js
await dataConsumer.setSubchannels([1, 2]);
dataProducer.send('for room 1 only', undefined, [1]);
```

A **DirectTransport** produces data from server code rather than from a
participant, which is how you put a bot or a system message into the room:

```js
const direct  = await router.createDirectTransport();
const botData = await direct.produceData({ label: 'bot' });
botData.send('welcome');
```

### Observers

Who is speaking, without decoding anything — the SFU reads RFC 6464 audio
levels off the header extension:

```js
const observer = await router.createAudioLevelObserver({
  maxEntries: 1, threshold: -50, interval: 800,
});

observer.on('volumes', ([{ producer, volume }]) => {
  console.log(producer.id, 'is loudest at', volume, 'dBov');
});
observer.on('silence', () => console.log('nobody is speaking'));

await observer.addProducer({ producerId: producer.id });
```

`createActiveSpeakerObserver()` reports a single dominant speaker instead of
levels, which is what a UI usually wants for the "big tile".

A note on thresholds: a quiet room is rarely quieter than about −60 dBov, so
a threshold of −80 will report someone speaking essentially always. Measure
your room's floor and set it above.

### Statistics

```js
await transport.getStats();
await producer.getStats();
await consumer.getStats();
```

Shapes follow the mediasoup stat objects, so dashboards and monitoring written
against mediasoup read them unchanged.

For per-event telemetry rather than snapshots, `enableTraceEvent()` turns on a
stream of what is happening to a stream:

```js
await consumer.enableTraceEvent(['keyframe', 'pli', 'nack', 'bwe']);
consumer.on('trace', (e) => console.log(e.type, e.direction, e.info));
```

Tracing is opt-in per entity and per type because it costs work on the packet
path: an entity nobody traces pays one boolean test per event, and the bwe
estimate is not even read unless someone asked for it.

### PlainTransport

Plain RTP over UDP — no ICE, no DTLS, no negotiation. This is the door to
ffmpeg, GStreamer, IP cameras and recorders, in both directions. It is
available through the mediasoup API (`router.createPlainTransport()`), which
is where external tooling expects to find it.

`comedia: true` learns the far end's address from the first packet it sends,
which is how you point ffmpeg at the server without knowing which port ffmpeg
will source from.

### Native API reference

Every method of the native API. Types are JavaScript values; anything marked
optional has the default shown.

#### `new Room()`

| method | returns | notes |
|---|---|---|
| `join(pcOrPeer)` | peer handle | accepts a `webrtc-server` `RTCPeerConnection` or a `stable-webrtc` peer. Throws if the argument is neither. Safe to call before the connection has negotiated. |
| `close()` | — | closes every peer and the underlying router. Idempotent. |
| `on(event, fn)` / `off` / `once` | the room | chainable |

#### `new Router()`

| method | returns | notes |
|---|---|---|
| `addPeer(pcOrPeer)` | peer handle | throws `addPeer: expected a stable-webrtc Peer or an RTCPeerConnection` if given anything else |
| `peers` | array of peer handles | live view; do not mutate |
| `addPlainPeer({ sendRtp })` | peer handle | a peer that is a socket, not a connection. `sendRtp(buffer)` is called for every outgoing packet. |
| `addPlainProducer({ peerHandle, kind, ssrc, codecName })` | producer handle | `ssrc` is required — there is no negotiation to learn it from. `codecName` is `'VP8'`, `'VP9'`, `'H264'`, `'H265'`, `'AV1'`; omit it and keyframe detection is disabled for that producer. |
| `watchFeedback(on)` | — | start/stop observing inbound NACK and FIR. Off by default because RTCP is a steady stream. |
| `close()` | — | closes every peer, producer and consumer |

#### Peer handle

| member | returns | notes |
|---|---|---|
| `id` | string | engine-assigned, e.g. `'peer-3'` |
| `producers` | array of producer handles | what this peer is currently sending; live view |
| `consume(producer, cb)` | — | `cb(err, consumer)` fires when the route is **bound** and packets are flowing, or with an error if it could not converge within 15 seconds |
| `close()` | — | closes this peer's producers and every consumer of them |

#### Producer handle

| member | returns | notes |
|---|---|---|
| `id` | string | |
| `kind` | `'audio'` \| `'video'` | |
| `peerId` | string | which peer is sending it |
| `requestKeyframe()` | — | sends a PLI upstream, throttled — several callers inside the throttle window produce one request |
| `push(rtp, info)` | — | plain producers only: feed a packet in |
| `close()` | — | every consumer of it is closed too |

#### Consumer handle

| member | returns | notes |
|---|---|---|
| `id`, `producerId`, `peerId` | string | |
| `pause()` | — | stops forwarding; the route and its state stay |
| `resume()` | — | re-bases the timeline and requests a fresh keyframe, so the viewer does not wait for the producer's next natural one |
| `getStats()` | object | see below |
| `setPreferredSpatialLayer(idx)` | — | a **ceiling**, not a pin: if that layer is not carrying, a lower live one is used. `null` means no ceiling. |
| `setPreferredTemporalLayer(idx)` | — | ceiling on temporal layers; `null` means all |
| `currentSpatialLayer` | number \| null | what is actually being forwarded |
| `currentTemporalLayer` | number \| null | |
| `close()` | — | |

#### `consumer.getStats()`

```js
{
  forwarded: 1843,        // packets forwarded to this viewer
  bound: true,            // is the route live
  paused: false,
  producerId: 'producer-2',
  layerSsrc: 471069948,   // the source layer currently being forwarded
  spatialLayer: 1,        // its index, low to high
}
```

`forwarded` is the number to watch. A consumer that is `bound` with `forwarded`
stuck is a routing problem; one climbing steadily while the viewer sees black
is a keyframe problem. That single distinction resolves most "no video"
reports.

## Using the mediasoup API

If you already have a mediasoup application, or you are following mediasoup
documentation and examples, use the compatible API instead. It is the complete
v3 server surface — 187 methods and properties, 67 events, measured against
mediasoup 3.24.2 itself:

```js
import { mediasoup } from 'sfu-server';

const worker = await mediasoup.createWorker();
const router = await worker.createRouter({ mediaCodecs });
const transport = await router.createWebRtcTransport({ listenIps });
const producer  = await transport.produce({ kind, rtpParameters });
const consumer  = await transport.consume({ producerId, rtpCapabilities });
```

**What this buys you over mediasoup itself:** `npm install` with nothing to
compile — no Python, no C++ toolchain, no worker binary, and no build step
that breaks on Windows.

**What it costs you:** see [Status and known gaps](#status-and-known-gaps).

#### Entities

| | |
|---|---|
| `createWorker(settings)` | the process itself — there is no subprocess to spawn, so this is cheap and `numWorkers > 1` buys nothing |
| `worker.createRouter({ mediaCodecs })` | a room's routing domain |
| `worker.createWebRtcServer({ listenInfos })` | one shared UDP port for many transports |
| `router.createWebRtcTransport(options)` | a participant's connection |
| `router.createPlainTransport(options)` | plain RTP, in or out — ffmpeg, cameras, recorders |
| `router.createPipeTransport(options)` | router-to-router |
| `router.createDirectTransport()` | data produced by server code |
| `router.createAudioLevelObserver(options)` | who is speaking |
| `router.createActiveSpeakerObserver(options)` | which one is dominant |
| `router.pipeToRouter({ producerId, router })` | make a producer consumable elsewhere |

#### Options that matter

`createWebRtcTransport`:

```js
await router.createWebRtcTransport({
  listenIps: [{ ip: '0.0.0.0', announcedIp: '203.0.113.7' }],
  webRtcServer,                       // share one port instead of listening again
  enableSctp: true,                   // data channels
  numSctpStreams: { OS: 1024, MIS: 1024 },
  initialAvailableOutgoingBitrate: 600000,
  appData: { peerId },                // yours, carried untouched
});
```

`consume` — the ones with real consequences:

```js
await transport.consume({
  producerId,
  rtpCapabilities,                    // the client's, from mediasoup-client
  paused: true,                       // strongly recommended, see below
  preferredLayers: { spatialLayer: 1, temporalLayer: 2 },
  enableRtx: true,
});
```

**`paused: true` is the mediasoup convention, and it matters here for the same
reason:** a packet that arrives before the client has finished setting up the
receiver binds to the wrong m-section. Create paused, tell the client, then
`resume()`. This library requests a fresh keyframe for you on resume, so the
viewer does not wait for the producer's next natural one.

`produce`:

```js
await transport.produce({
  kind, rtpParameters,
  paused: false,
  keyFrameRequestDelay: 1000,         // throttle how often this producer is asked
  appData,
});
```

`keyFrameRequestDelay` is enforced on the **producer**, so ten consumers of one
camera cannot combine into a PLI storm.

#### Not the same as mediasoup

- **`numWorkers`** — there is no subprocess. One worker is the process; more
  of them only splits your routers into groups for no benefit.
- **`worker.died`** — cannot fire the way it does in mediasoup, where it means
  a crashed C++ child. If this process dies, it dies.
- **`workerBin`, `logLevel`, `logTags`** — accepted and ignored; there is no
  worker to configure. Use `SFU_DEBUG` instead.

## Migration from mediasoup

Most applications need a single change:

```js
// before
import * as mediasoup from 'mediasoup';

// after
import { mediasoup } from 'sfu-server';
```

Everything else — `createWorker`, `createRouter`, `createWebRtcTransport`,
producers, consumers, observers, `pipeToRouter` — stays the same.

If rewriting imports is not an option (a TypeScript build, a third-party
library, or a large codebase), point the `mediasoup` dependency at a thin
shim package that re-exports this one. The official mediasoup demo runs on
this library that way, unmodified, in strict-mode TypeScript.

Behaviours that differ from mediasoup are listed in
[Not the same as mediasoup](#not-the-same-as-mediasoup) — a short list, and
none of them touch the packet path.

## Signaling is yours

Like every WebRTC stack, this one does not ship a signalling protocol — how
participants find each other and exchange SDP is your application's design,
not the SFU's. WebSocket, HTTP, protoo, WHIP: all fine.

What the SFU needs from your signalling is small:

1. a peer connection per participant (`webrtc-server`'s `RTCPeerConnection`)
2. offer/answer exchanged with that participant, as usual
3. `room.join(pc)` — or, on the mediasoup API, the standard
   `createWebRtcTransport` → `connect` → `produce` / `consume` request flow

If you want a worked example of the second, the official mediasoup demo is
one, and it runs here unmodified — its protoo signalling included.

## Simulcast and layer selection

A browser publishing with simulcast sends the same picture three times at
different resolutions, and each of those carries three temporal layers. An SFU
chooses, per viewer, which one to forward — and the choice matters more than
it sounds.

**Spatial layers** are re-evaluated continuously, never latched. Chrome sheds
and revives layers as bandwidth and CPU move, and it rolls their SSRCs while
doing it; a forwarder that picks a layer once and remembers it freezes the
moment its layer goes quiet. Selection here is a condition over the layers'
current liveness: drop to a carrying layer immediately, climb back only after
the better one has been steadily alive — dropping late means a frozen viewer
now, while climbing late merely means a smaller picture for a moment longer.

**Temporal layers** lower frame rate without touching resolution and without
needing a keyframe, because the encoder guarantees that nothing below a layer
references a higher one. That makes them the right first move when a link
degrades: 30fps to 15fps is barely noticeable, where 720p to 360p is a visible
jump.

**Bandwidth adaptation** runs on the transport's own estimate, per viewer. The
app's `setPreferredLayers` ceiling always wins — an estimate may lower what a
viewer receives, never raise it past what the application asked for.

**Keyframes** are tracked as a debt. A consumer owes its viewer a decodable
entry point from the moment it binds and after every resume, and the debt is
settled only by a keyframe that actually reaches it — not by one that was
requested. Without that bookkeeping, a keyframe discarded during the pause
window that mediasoup's own `paused: true` practice creates leaves the viewer
black with nobody left to ask.

## Codec support

| Codec | Forwarding | Keyframe detection | Temporal layers |
|---|---|---|---|
| VP8 | ✔ | ✔ | ✔ |
| VP9 | ✔ | ✔ | ✔ |
| H.264 | ✔ | ✔ (IDR, incl. FU-A and STAP-A) | — |
| H.265 | ✔ | ✔ | — |
| AV1 | ✔ | ✔ | — |
| Opus | ✔ | n/a | n/a |

Codec parsing lives in [rtp-packet](https://www.npmjs.com/package/rtp-packet),
not here: an SFU decides *what* to forward, and the codec library knows *what
the bytes mean*. An unrecognised codec is never guessed at — a fabricated
keyframe reading is worse than none, because it settles a debt that was never
paid.

Temporal layers exist only where the codec signals them in its payload
descriptor. H.264, H.265 and AV1 do not carry a payload-level temporal id, so
spatial selection applies to them and temporal does not — the same trade-off
mediasoup itself makes.

## Why pure JavaScript matters

This is not a wrapper. ICE, DTLS 1.3, SRTP, SCTP, RTP packetization and the
routing engine are all JavaScript, in
[webrtc-server](https://www.npmjs.com/package/webrtc-server),
[rtp-packet](https://www.npmjs.com/package/rtp-packet) and this package.

**Installation is `npm install`.** No Python, no C++ toolchain, no build step
to fail on a Windows machine or in a slim container.

**Debugging is `console.log`.** A media bug in a C++ worker means gdb and a
cross-process protocol; here the packet path is a function you can read, log,
and step through. Every bug found while bringing the mediasoup demo up on this
stack — a keyframe discarded in a pause window, a layer selection latched onto
a dying SSRC, an extension id that meant one thing to us and another to the
receiver — was found that way.

**It runs wherever Node runs**, including environments that forbid native
modules.

## Architecture

```
   your application
        │
        ├── Room / Router ──────────► native API
        └── mediasoup ──────────────► drop-in mediasoup v3 API
                │
                ▼
        routing engine (sfu.js)
                │
                ▼
        webrtc-server      ICE · DTLS · SRTP · SCTP · RTP
                │
                ▼
        rtp-packet         packetization · codecs · RTCP
```

Both front doors drive the same engine — they are not parallel
implementations. The engine is **reactive**: every trigger (a peer event, a
first packet, a timeout) re-evaluates the same set of conditions, and each
condition is idempotent. There is no separate state machine to drift out of
sync, which is why a producer that changes SSRC mid-call, a consumer created
before its transport connects, and a participant who joins during a
renegotiation all resolve through the same path.

## Scaling

One `Router` handles a room. To spread participants across routers — and, with
one router per process, across CPU cores — connect them with `pipeToRouter()`:

```js
await routerA.pipeToRouter({ producerId: producer.id, router: routerB });
```

A producer on A becomes consumable on B, over a pipe of plain RTP. The
transport pair is created once per destination router and reused, and the
piped producer's lifetime follows the original: close the camera and both ends
go with it.

## Performance

An SFU's cost is per **packet forwarded**, not per participant, because
nothing is decoded. Adding a viewer adds one forwarding path per stream they
consume — a memory copy, a header rewrite and a socket write — so a room of N
participants costs roughly N² of those, and CPU stays flat where a transcoding
server would climb.

Where the time actually goes on the packet path, and what is done about it:

- **Header rewriting** happens in place, without allocating a new packet per
  consumer
- **Codec parsing** (keyframe detection, temporal ids) runs only when
  something needs the answer — a consumer with no keyframe debt and no
  temporal cap does not pay for it
- **Codec identity** is resolved once per producer, not per packet
- **Diagnostics** are compiled out by a flag check, not merely quiet

No benchmark numbers are published yet, because the ones worth publishing come
from a loaded server rather than a loop on a laptop. If you measure this in a
real deployment, that report is welcome.

## Debugging

```bash
SFU_DEBUG=1 node server.js
```

Prints the routing engine's decisions: when a consumer binds, which layer it
rides, when a keyframe is requested and whether it arrived, and when a
producer's SSRC drifts. Off by default, and the packet path skips the work
entirely when it is off.

## Troubleshooting

**Only the first participant is ever visible.** Almost always a missing
`onnegotiationneeded` handler. An SFU adds transceivers as participants come
and go, so the connection renegotiates repeatedly, not just at start. See the
[complete server example](#a-complete-server-you-can-run).

**Consumer times out after 15 seconds.** The route never converged. Either
the transport did not connect (check ICE / DTLS on the peer connection), or
`consume()` was called before `transport.connect()` completed. Enable
`SFU_DEBUG=1` and look for the last cascade line for that consumer — it names
which condition was still missing.

**Viewer is bound but seeing black.** A keyframe never reached the consumer.
On simulcast, the layer it landed on may have gone quiet — look for
`layer switch` lines in the debug output. On an unknown codec, keyframe
detection is off by design and the viewer waits for the next natural one.

**`consumer.getStats().forwarded` climbs but the viewer sees nothing.** The
payload type or extension id the viewer's answer used and the one the engine
is stamping are different. Check the codec section of the client's SDP against
`router.rtpCapabilities`.

**Silent room, no errors visible.** Listen to the `error` event on `Room` —
routing attempts that fail silently reach nowhere else, and a room that looks
frozen usually has one caught listener away from an explanation.

**"unsupported codec" from `router.createWebRtcTransport` or `produce`.** The
producer declared a codec the router does not know. Print
`router.rtpCapabilities.codecs` to see what is negotiable, and use
`worker.createRouter({ mediaCodecs })` to narrow it deliberately.

## Status and known gaps

This library is new. The mediasoup API surface is measured — 187 members and
67 events, checked against mediasoup 3.24.2 on every test run — and the
official mediasoup demo runs on it unmodified with several participants.
It has **not** been through production load: no month-long soak, no fifty-user
room, no adversarial network.

Known gaps, all deliberate and declared rather than silently missing:

- **SCTP piping** — `pipeToRouter()` carries media; `dataProducerId` is not implemented
- **SRTP on PlainTransport** — `enableSrtp` is refused rather than ignored
- **Simulcast over PlainTransport** — the first encoding is ingested
- **Temporal layers for H.264 / H.265 / AV1** — the codecs do not carry a payload-level temporal id (mediasoup does not do this either)

If you hit something that does not behave like mediasoup, that is a bug worth
reporting — the compatibility claim is meant to be exact. Please [open an
issue](https://github.com/colocohen/sfu-server/issues).

## Interoperability

Verified against the clients that matter for an SFU:

| | audio | video | data | simulcast |
|---|---|---|---|---|
| Chrome / Edge | ✔ | ✔ | ✔ | ✔ |
| mediasoup-client | ✔ | ✔ | ✔ | ✔ |
| ffmpeg (via PlainTransport) | ✔ | ✔ | — | — |

The strongest interop test is the one that was not written for this library:
the **official mediasoup demo**, unmodified, with its React client, its protoo
signalling, its chat bot, its observers and its simulcast — running against
this server in strict-mode TypeScript, with several participants.

Safari and Firefox are expected to work — VP8 and H.264 are supported and
negotiated — but have not been exercised. H.265 is present as a router
capability, but Safari-specific H.265 negotiation has not been tested. If you
try either browser, a report is valuable.

## RFC compliance

The SFU layer implements:

- RFC 3550 — RTP (forwarding, sequence and timestamp rewriting)
- RFC 4585 — RTCP feedback: PLI relaying with throttling, NACK observation
- RFC 4588 — RTX (retransmission stream handling)
- RFC 6184 / 7798 — H.264 / H.265 payload formats (keyframe detection incl. FU-A, STAP-A)
- RFC 6464 — client-to-mixer audio level, for speaker detection
- RFC 7741 — VP8 payload format (keyframe and temporal layer reading)
- RFC 8285 — RTP header extensions (id translation between participants)
- RFC 8852 / 8853 — RID and simulcast
- RFC 8831 / 8832 — WebRTC data channels
- RFC 9798 — AV1 payload format
- draft-ietf-payload-vp9 — VP9 payload format
- 3GPP TS 26.114 — CVO (video orientation)

The transport layer beneath — ICE, DTLS-SRTP, SCTP — is
[webrtc-server](https://www.npmjs.com/package/webrtc-server); packetization and
codec formats are [rtp-packet](https://www.npmjs.com/package/rtp-packet). Each
lists its own compliance.

## Use cases

- **Video conferencing** — the case the library is shaped around
- **Live broadcast** — one publisher, many viewers, with simulcast so each gets what their link can carry
- **Webinars and classrooms** — `Router` directly, forwarding only the presenter
- **Recording** — PlainTransport into ffmpeg
- **Restreaming** — PlainTransport into ffmpeg into RTMP (YouTube, Twitch)
- **Live transcription** — PlainTransport into a speech pipeline
- **Watch-party and gaming voice** — audio routing with active speaker detection

## 🙏 Sponsors

sfu-server is independently developed and maintained. If it is useful to you,
a star on the [GitHub repository](https://github.com/colocohen/sfu-server) or
a sponsorship goes a long way. Issue reports and pull requests are welcome.

## 📜 License

**Apache License 2.0**

```
Copyright © 2026 colocohen

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```
