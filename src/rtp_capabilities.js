// src/rtp_capabilities.js
// mediasoup-compat (M4) — Router RTP capabilities.
//
// Produces the router's RtpCapabilities in mediasoup v3 format, DERIVED
// from webrtc-server's canonical SDP tables (SDP.DEFAULT_*_CODECS /
// _EXTENSIONS) rather than hardcoded — one source of truth, so a codec
// added to the stack surfaces here automatically.
//
// SPIKE SCOPE: the capability set is intentionally the SFU's ROUTED
// reality, not the stack's full negotiation menu — opus + VP8(+RTX),
// plus the header extensions our receive/send paths actually parse and
// stamp. mediasoup-client's device.load() validates this object
// strictly; passing it is the acceptance test for the format.
//
// mediasoup shape reference (mediasoup/src/RtpParameters.ts):
//   codecs: [{ kind, mimeType, preferredPayloadType, clockRate,
//              channels?, parameters, rtcpFeedback: [{type, parameter}] }]
//   headerExtensions: [{ kind, uri, preferredId, preferredEncrypt,
//                        direction }]

import { SDP } from 'webrtc-server';

/* ── feedback string → mediasoup {type, parameter} ── */
function fb(entry) {
  var sp = entry.indexOf(' ');
  if (sp < 0) return { type: entry, parameter: '' };
  return { type: entry.slice(0, sp), parameter: entry.slice(sp + 1) };
}

/* ── stable preferred payload types ──
 * opus keeps its conventional 111. Video codecs are allocated in pairs
 * (codec, its RTX) starting at 96, in the transport's own table order —
 * deterministic across runs, which matters because an app may cache
 * router capabilities. These are PREFERRED types: the value actually
 * used is whatever negotiation settles on. */
var PT_AUDIO_OPUS = 111;
var PT_VIDEO_BASE = 96;

function buildCodecs() {
  var codecs = [];

  // opus — from DEFAULT_AUDIO_CODECS (name/clockRate/channels/fmtp/feedback)
  var opus = (SDP.DEFAULT_AUDIO_CODECS || []).filter(function (c) {
    return c.name === 'opus';
  })[0];
  if (opus) {
    codecs.push({
      kind:                 'audio',
      mimeType:             'audio/opus',
      preferredPayloadType: PT_AUDIO_OPUS,
      clockRate:            opus.clockRate,
      channels:             opus.channels || 2,
      parameters:           opus.fmtp ? Object.assign({}, opus.fmtp) : {},
      rtcpFeedback:         (opus.feedback || []).map(fb),
    });
  }

  // EVERY video codec the transport can carry, not just VP8.
  //
  // An SFU forwards RTP; it does not decode, so the codec set it can
  // route is the codec set the transport understands — and that has been
  // VP8/VP9/H264/H265/AV1 for a while. This file advertised VP8 alone,
  // so a client asking for anything else got "codec not supported" from
  // the router's capabilities and never even tried. H264 matters most:
  // without it there is no Safari and no iOS.
  //
  // The fmtp defaults travel with the codec (H264's profile-level-id in
  // particular) because a bare H264 line with no profile is exactly the
  // configuration browsers refuse.
  var videos = (SDP.DEFAULT_VIDEO_CODECS || []);
  var nextPt = PT_VIDEO_BASE;
  for (var vi = 0; vi < videos.length; vi++) {
    var v = videos[vi];
    if (!v || !v.name) continue;
    var pt = nextPt++;
    codecs.push({
      kind:                 'video',
      mimeType:             'video/' + v.name,
      preferredPayloadType: pt,
      clockRate:            v.clockRate || 90000,
      parameters:           v.fmtp ? Object.assign({}, v.fmtp) : {},
      rtcpFeedback:         (v.feedback || []).map(fb),
    });
    if (v.rtx) {
      codecs.push({
        kind:                 'video',
        mimeType:             'video/rtx',
        preferredPayloadType: nextPt++,
        clockRate:            v.clockRate || 90000,
        parameters:           { apt: pt },
        rtcpFeedback:         [],
      });
    }
  }

  return codecs;
}

/* ── header extensions: only what the stack actually parses/stamps ── */
var CLAIMED = {
  audio: [
    'urn:ietf:params:rtp-hdrext:sdes:mid',
    'http://www.webrtc.org/experiments/rtp-hdrext/abs-send-time',
    'http://www.ietf.org/id/draft-holmer-rmcat-transport-wide-cc-extensions-01',
    'urn:ietf:params:rtp-hdrext:ssrc-audio-level',
  ],
  video: [
    'urn:ietf:params:rtp-hdrext:sdes:mid',
    'http://www.webrtc.org/experiments/rtp-hdrext/abs-send-time',
    'http://www.ietf.org/id/draft-holmer-rmcat-transport-wide-cc-extensions-01',
    'urn:ietf:params:rtp-hdrext:sdes:rtp-stream-id',
    'urn:ietf:params:rtp-hdrext:sdes:repaired-rtp-stream-id',
    // CVO (3GPP TS 26.114): one byte carrying camera/flip/rotation. We
    // claim it because we now READ it — a producer's
    // 'videoorientationchange' cannot fire for an extension the peer was
    // never told to send.
    'urn:3gpp:video-orientation',
  ],
};

function buildHeaderExtensions() {
  var out = [];
  var kinds = { audio: SDP.DEFAULT_AUDIO_EXTENSIONS, video: SDP.DEFAULT_VIDEO_EXTENSIONS };
  for (var kind in kinds) {
    var table = kinds[kind] || [];
    for (var i = 0; i < table.length; i++) {
      var e = table[i];
      if (CLAIMED[kind].indexOf(e.uri) < 0) continue;
      out.push({
        kind:             kind,
        uri:              e.uri,
        preferredId:      e.id,
        preferredEncrypt: false,
        direction:        'sendrecv',
      });
    }
  }
  return out;
}

export function getRouterRtpCapabilities() {
  return {
    codecs:           buildCodecs(),
    headerExtensions: buildHeaderExtensions(),
  };
}
