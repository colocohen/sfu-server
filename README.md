# sfu-server

**SFU (Selective Forwarding Unit) for Node.js — multi-party WebRTC routing, in pure JavaScript.**

> ⚠️ **Status: under active development.** This is an early placeholder
> release that reserves the package name while the first working version
> is being built. The APIs below describe what is coming — they are not
> functional in `0.0.1`. Follow the repository for progress; the first
> usable release is expected within weeks, building on components that
> are already published and working (see the ecosystem section below).

## What this will be

An SFU routes media between the participants of a multi-party call:
each participant uploads their stream once, and the server forwards it
to everyone else — without decoding or re-encoding. `sfu-server` brings
that to Node.js as a plain JavaScript library:

- **Rooms** with N participants — each participant is a standard
  WebRTC peer connection.
- **Selective forwarding** — encoded packets are routed, never
  transcoded: low CPU, low latency.
- **Per-subscriber stream rewriting** (SSRC / sequence / timestamp
  spaces) so every consumer sees one clean, continuous stream.
- **Keyframe management** — PLI/FIR from subscribers forwarded to the
  producer, with throttling.
- **Simulcast layer selection** per subscriber, driven by each
  subscriber's own bandwidth estimation *(planned)*.
- **W3C-flavored API** — if you know browser WebRTC, you already know
  most of this library.

## Part of a working ecosystem

`sfu-server` is the orchestration layer on top of an existing,
published, actively-developed WebRTC stack by the same author:

| Package | Role |
|---|---|
| [`webrtc-server`](https://www.npmjs.com/package/webrtc-server) | Full W3C `RTCPeerConnection` for Node.js (full + lite ICE, DTLS, SRTP, SCTP) |
| [`rtp-packet`](https://www.npmjs.com/package/rtp-packet) | Complete RTP/RTCP stack: all major codecs, RTX/NACK, RED, FlexFEC, SRTP, jitter buffer — including the stream-forwarding rewriter this SFU is built on |
| [`media-processing`](https://www.npmjs.com/package/media-processing) | WebCodecs for Node (FFmpeg-backed), MediaStream, capture, HLS |
| [`stable-webrtc`](https://www.npmjs.com/package/stable-webrtc) | Resilience layer: reconnects, liveness, signaling fallback — the recommended connection layer for SFU participants |

The transport, the RTP mechanics, and the media pipeline all exist and
interoperate with Chrome today; this package assembles them into a
multi-party router.

## Planned API (subject to change)

```js
import { Room } from 'sfu-server';

const room = new Room();

// each participant wraps a peer connection
// (a stable-webrtc Peer, or a raw webrtc-server RTCPeerConnection)
const alice = room.addParticipant(alicePeer);
const bob   = room.addParticipant(bobPeer);

room.on('producer', (producer) => {
  // someone started sending — route it to everyone else
  for (const p of room.participants) {
    if (p !== producer.participant) p.consume(producer);
  }
});
```

## License

MIT © colocohen
