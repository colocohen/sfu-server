/**
 * ortc.js — codec & capability arithmetic for the mediasoup-compat layer.
 *
 * WHAT THIS IS
 * ------------
 * mediasoup inherited its data model from ORTC (a dead standard that
 * replaced SDP strings with structured objects). Everything an app hands
 * us or receives from us — rtpCapabilities, rtpParameters — is in that
 * shape. This file is the arithmetic over those objects, and nothing
 * else: pure functions, no state, no pc, no network, no I/O.
 *
 * WHY IT'S ITS OWN FILE (house rule: file = domain)
 * -------------------------------------------------
 * The rest of the compat layer is object lifecycle over live connections.
 * This is a calculator. It is the only part of the layer that can be
 * fully unit-tested with no socket and no browser, which is exactly why
 * it should not be buried inside code that can't.
 *
 * THE PROBLEM IT SOLVES
 * ---------------------
 * A producer arrives declaring VP8 at payload type 101 (its browser
 * picked that). A different browser wants to consume it and declares it
 * understands VP8 at 96. Same codec, different numbers — and the header
 * extension ids differ too. Someone has to compute: is this consumable
 * at all, and if so, exactly what parameters does THIS consumer get?
 *
 * That someone is this file.
 *
 * WHY WE CAN FREELY RE-NUMBER
 * ---------------------------
 * Because the SFU rewrites the payload type per consumer anyway —
 * RtpForwarder is constructed with { ssrc, payloadType } and stamps
 * both on every forwarded packet. So the consumer's preferred PT is
 * simply what we stamp for that consumer. No transcoding, no constraint.
 *
 * THE LESSON THAT SHAPES THIS FILE
 * --------------------------------
 * From the M4c silent-black-video bug: the client demuxes by ITS OWN
 * numbering, not ours. Consumer parameters are therefore built in the
 * CONSUMER'S dialect throughout — its preferred payload types, its
 * header-extension ids. Our internal numbering never leaks outward.
 *
 * mediasoup reference: mediasoup/node/src/ortc.ts
 */

/* ── small helpers ────────────────────────────────────────────────── */

function lower(s) {
  return String(s == null ? '' : s).toLowerCase();
}

function is_rtx_codec(codec) {
  return /\/rtx$/i.test(String(codec && codec.mimeType || ''));
}

function codec_kind(codec) {
  // mediasoup capabilities carry `kind`; rtpParameters codecs do not —
  // there the kind comes from the producer. Derive from mimeType so a
  // codec object is self-describing either way.
  if (codec && codec.kind) return codec.kind;
  var mt = lower(codec && codec.mimeType);
  if (mt.indexOf('audio/') === 0) return 'audio';
  if (mt.indexOf('video/') === 0) return 'video';
  return null;
}

function params_of(codec) {
  return (codec && codec.parameters) || {};
}

/* ── codec matching ───────────────────────────────────────────────── */

/**
 * Do these two codecs describe the same thing?
 *
 * mimeType + clockRate + channels is the baseline. Two codecs need more
 * than that because the same mimeType can name incompatible variants —
 * an H.264 baseline decoder cannot decode a high-profile stream, and
 * announcing a match there produces a stream that arrives and never
 * decodes (the worst failure mode: everything reports healthy).
 */
export function matchCodecs(a, b, opts) {
  var aMime = lower(a && a.mimeType);
  var bMime = lower(b && b.mimeType);
  if (!aMime || aMime !== bMime) return false;
  if (a.clockRate !== b.clockRate) return false;

  // channels: audio only; absent means 1 (RFC 7587 taught us not to
  // assume "/2" — see the mono-microphone bug in the main ROADMAP).
  if (codec_kind(a) === 'audio') {
    var aCh = a.channels != null ? a.channels : 1;
    var bCh = b.channels != null ? b.channels : 1;
    if (aCh !== bCh) return false;
  }

  var ap = params_of(a), bp = params_of(b);

  switch (aMime) {
    case 'video/h264':
    case 'video/h264-svc': {
      // packetization-mode: absent means 0 per RFC 6184.
      var aPm = ap['packetization-mode'] != null ? Number(ap['packetization-mode']) : 0;
      var bPm = bp['packetization-mode'] != null ? Number(bp['packetization-mode']) : 0;
      if (aPm !== bPm) return false;
      // profile-level-id: mediasoup runs a full profile-compatibility
      // check (h264-profile-level-id) that treats different LEVELS of the
      // same PROFILE as compatible. We compare the profile octets only —
      // the first 4 hex chars (profile_idc + profile_iop) — and ignore
      // the level byte, which is the same rule in spirit.
      var aP = String(ap['profile-level-id'] || '42e01f').slice(0, 4).toLowerCase();
      var bP = String(bp['profile-level-id'] || '42e01f').slice(0, 4).toLowerCase();
      if (aP !== bP) return false;
      break;
    }
    case 'video/vp9': {
      // VP9 profile-id is compared ONLY in strict mode, exactly as
      // mediasoup does it — and for the same reason. A router's
      // supported-capability entry for VP9 carries no profile-id at all
      // (neither ours nor mediasoup's), so a strict comparison against a
      // configured profile-id of 2 defaults the other side to 0 and
      // drops VP9 outright: the demo's own config asks for profile-id 2,
      // and VP9 silently vanished from the router's capabilities.
      // Strictness belongs where two REAL codec descriptions meet
      // (producer vs consumer), not where a request meets a generic
      // capability entry.
      if (opts && opts.strict) {
        var aProf = ap['profile-id'] != null ? Number(ap['profile-id']) : 0;
        var bProf = bp['profile-id'] != null ? Number(bp['profile-id']) : 0;
        if (aProf !== bProf) return false;
      }
      break;
    }
    case 'video/apt':
      break;
    default:
      break;
  }

  return true;
}

/**
 * Find the RTX codec that repairs `primary` inside a codec list.
 * Association is by the `apt` parameter, which names the primary's PT.
 */
function find_rtx_for(codecs, primary) {
  var pt = primary.payloadType != null ? primary.payloadType : primary.preferredPayloadType;
  for (var i = 0; i < codecs.length; i++) {
    var c = codecs[i];
    if (!is_rtx_codec(c)) continue;
    if (Number(params_of(c).apt) === Number(pt)) return c;
  }
  return null;
}

/* ── header extensions ────────────────────────────────────────────── */

// Extensions that make sense to STAMP on a stream we send to a consumer.
// RID / repaired-RID are deliberately absent: they identify simulcast
// layers on the way IN to an SFU. Sending them downstream is meaningless
// and confuses receivers. (mediasoup excludes them for the same reason.)
var CONSUMER_EXTENSION_ALLOWLIST = {
  'urn:ietf:params:rtp-hdrext:sdes:mid': true,
  'http://www.webrtc.org/experiments/rtp-hdrext/abs-send-time': true,
  'http://www.ietf.org/id/draft-holmer-rmcat-transport-wide-cc-extensions-01': true,
  'urn:3gpp:video-orientation': true,
  'urn:ietf:params:rtp-hdrext:toffset': true,
  'http://www.webrtc.org/experiments/rtp-hdrext/playout-delay': true,
  'urn:ietf:params:rtp-hdrext:ssrc-audio-level': true,
};

/* ── public: canConsume ───────────────────────────────────────────── */

/**
 * Can an endpoint with `consumerCaps` receive this producer?
 *
 * mediasoup apps call router.canConsume() before nearly every consume().
 * The answer is yes when at least one NON-RTX producer codec has a match
 * in the consumer's capabilities — RTX alone repairs nothing, so a
 * consumer that shares only the rtx entry cannot actually decode.
 *
 * @param {Object} producerRtpParameters  the producer's rtpParameters
 * @param {Object} consumerCaps           the endpoint's rtpCapabilities
 * @returns {boolean}
 */
export function canConsume(producerRtpParameters, consumerCaps) {
  if (!producerRtpParameters || !consumerCaps) return false;
  var pCodecs = producerRtpParameters.codecs || [];
  var cCodecs = consumerCaps.codecs || [];

  for (var i = 0; i < pCodecs.length; i++) {
    if (is_rtx_codec(pCodecs[i])) continue;
    for (var j = 0; j < cCodecs.length; j++) {
      if (is_rtx_codec(cCodecs[j])) continue;
      if (matchCodecs(pCodecs[i], cCodecs[j], { strict: true })) return true;
    }
  }
  return false;
}

/* ── public: getConsumerRtpParameters ─────────────────────────────── */

/**
 * Build the rtpParameters for ONE consumer of ONE producer.
 *
 * This object is what the app signals to its client, and what the client
 * feeds to its local transport.consume(). Everything in it is expressed
 * in the CONSUMER'S numbering — its preferred payload types, its header
 * extension ids — because that is the numbering the client will use to
 * demux what arrives. Our internal mids/PTs never appear here.
 *
 * @param {Object} opts
 * @param {Object} opts.producerRtpParameters  the producer's rtpParameters
 * @param {Object} opts.consumerCaps           the endpoint's rtpCapabilities
 * @param {string} opts.kind                   'audio' | 'video'
 * @param {number} opts.ssrc                   the SSRC we will send under
 * @param {number} [opts.rtxSsrc]              our RTX SSRC, if any
 * @param {string} opts.mid                    the client-facing mid
 * @param {string} [opts.cname]                RTCP cname to announce
 * @returns {Object} rtpParameters
 * @throws {Error} when no codec matches (call canConsume first)
 */
export function getConsumerRtpParameters(opts) {
  opts = opts || {};
  var producerRtpParameters = opts.producerRtpParameters || {};
  var consumerCaps          = opts.consumerCaps || {};
  var kind                  = opts.kind;
  var ssrc                  = opts.ssrc;
  var rtxSsrc               = opts.rtxSsrc;
  var mid                   = opts.mid;

  var pCodecs = producerRtpParameters.codecs || [];
  var cCodecs = consumerCaps.codecs || [];

  // 1. Pick the codec. First producer codec with a consumer match wins —
  //    producer order reflects the sender's own preference, which is the
  //    closest thing to an authoritative ranking we have.
  var chosenProducerCodec = null;
  var chosenConsumerCodec = null;
  for (var i = 0; i < pCodecs.length && !chosenConsumerCodec; i++) {
    if (is_rtx_codec(pCodecs[i])) continue;
    for (var j = 0; j < cCodecs.length; j++) {
      if (is_rtx_codec(cCodecs[j])) continue;
      if (matchCodecs(pCodecs[i], cCodecs[j], { strict: true })) {
        chosenProducerCodec = pCodecs[i];
        chosenConsumerCodec = cCodecs[j];
        break;
      }
    }
  }
  if (!chosenConsumerCodec) {
    throw new Error('getConsumerRtpParameters: no compatible codec ' +
      '(producer offers ' + pCodecs.map(function (c) { return c.mimeType; }).join(',') +
      ') — call canConsume() first');
  }

  var outPt = chosenConsumerCodec.preferredPayloadType != null
    ? chosenConsumerCodec.preferredPayloadType
    : chosenConsumerCodec.payloadType;

  // rtcpFeedback: intersect. Announcing feedback the consumer never
  // declared invites it to send RTCP we don't handle; announcing
  // feedback IT wants but the producer can't honour (a NACK we can't
  // satisfy upstream) is equally dishonest. Keep only what both claim.
  var pFb = chosenProducerCodec.rtcpFeedback || [];
  var cFb = chosenConsumerCodec.rtcpFeedback || [];
  var fb = [];
  for (var f = 0; f < cFb.length; f++) {
    for (var g = 0; g < pFb.length; g++) {
      if (cFb[f].type === pFb[g].type &&
          (cFb[f].parameter || '') === (pFb[g].parameter || '')) {
        fb.push({ type: cFb[f].type, parameter: cFb[f].parameter || '' });
        break;
      }
    }
  }

  var codecs = [{
    mimeType:     chosenConsumerCodec.mimeType,
    payloadType:  outPt,
    clockRate:    chosenConsumerCodec.clockRate,
    // Parameters come from the PRODUCER: they describe the actual
    // encoded bitstream we are forwarding (profile, packetization mode,
    // opus stereo/fec flags). The consumer's copy is a wish list; the
    // producer's is the truth about the bytes on the wire.
    parameters:   Object.assign({}, params_of(chosenProducerCodec)),
    rtcpFeedback: fb,
  }];
  if (kind === 'audio' || codec_kind(chosenConsumerCodec) === 'audio') {
    codecs[0].channels = chosenConsumerCodec.channels != null
      ? chosenConsumerCodec.channels
      : (chosenProducerCodec.channels != null ? chosenProducerCodec.channels : 1);
  }

  // 2. RTX — only when BOTH the consumer declares an rtx entry for this
  //    codec AND we actually have an RTX ssrc to send on. Announcing RTX
  //    we can't serve means NACKs arrive and go unanswered forever.
  var rtxOut = null;
  if (rtxSsrc != null) {
    var cRtx = find_rtx_for(cCodecs, chosenConsumerCodec);
    if (cRtx) {
      rtxOut = {
        mimeType:     cRtx.mimeType,
        payloadType:  cRtx.preferredPayloadType != null ? cRtx.preferredPayloadType : cRtx.payloadType,
        clockRate:    cRtx.clockRate,
        parameters:   { apt: outPt },
        rtcpFeedback: [],
      };
      codecs.push(rtxOut);
    }
  }

  // 3. Header extensions, in the CONSUMER's ids. The allowlist keeps
  //    RID/RRID out (producer-side only) and everything else the client
  //    declared for this kind stays with the id IT chose.
  var headerExtensions = [];
  var cExts = consumerCaps.headerExtensions || [];
  for (var e = 0; e < cExts.length; e++) {
    var ext = cExts[e];
    if (ext.kind && kind && ext.kind !== kind) continue;
    if (!CONSUMER_EXTENSION_ALLOWLIST[ext.uri]) continue;
    headerExtensions.push({
      uri:        ext.uri,
      id:         ext.preferredId != null ? ext.preferredId : ext.id,
      encrypt:    !!ext.preferredEncrypt,
      parameters: {},
    });
  }

  // 4. Encodings — exactly one. An SFU consumer always sees a single
  //    continuous stream: RtpForwarder rewrites simulcast layer switches
  //    into one seq/ts space (switchSource re-bases), so the consumer
  //    never learns there were layers. When layer selection lands (M3),
  //    scalabilityMode goes here — the shape does not change.
  var encoding = { ssrc: ssrc };
  if (rtxOut && rtxSsrc != null) encoding.rtx = { ssrc: rtxSsrc };

  return {
    mid:              mid != null ? String(mid) : undefined,
    codecs:           codecs,
    headerExtensions: headerExtensions,
    encodings:        [encoding],
    rtcp: {
      cname: (producerRtpParameters.rtcp && producerRtpParameters.rtcp.cname) ||
             opts.cname || 'sfu-server',
      reducedSize: true,
    },
  };
}

/* ── public: validateProducerRtpParameters ────────────────────────── */

/**
 * A producer may declare its own payload types and extension ids (that
 * flexibility is explicitly part of mediasoup's contract), but it must
 * NOT declare a codec the router doesn't have — nothing downstream could
 * ever consume it, and the failure would surface much later as a
 * mysteriously unconsumable producer.
 *
 * @throws {Error} naming the offending codec
 * @returns {true}
 */
export function validateProducerRtpParameters(rtpParameters, routerCaps) {
  var codecs = (rtpParameters && rtpParameters.codecs) || [];
  var caps   = (routerCaps && routerCaps.codecs) || [];
  if (!codecs.length) throw new Error('rtpParameters.codecs is empty');

  var hasMedia = false;
  for (var i = 0; i < codecs.length; i++) {
    if (is_rtx_codec(codecs[i])) continue;
    hasMedia = true;
    var ok = false;
    for (var j = 0; j < caps.length; j++) {
      if (is_rtx_codec(caps[j])) continue;
      if (matchCodecs(codecs[i], caps[j])) { ok = true; break; }
    }
    if (!ok) {
      throw new Error('unsupported codec ' + codecs[i].mimeType +
        ' — not in the router capabilities');
    }
  }
  if (!hasMedia) throw new Error('rtpParameters.codecs has no media codec (rtx only)');
  return true;
}

/* ── public: filterCapabilitiesByMediaCodecs ──────────────────────── */

/**
 * worker.createRouter({ mediaCodecs }) narrows what the router will
 * accept — apps use it to force H.264-only rooms, drop stereo, and so
 * on. Given our full capability set and the app's mediaCodecs list,
 * return the capabilities that router should advertise.
 *
 * RTX entries follow their primary automatically: mediasoup documents
 * that feature codecs MUST NOT appear in mediaCodecs, so an app cannot
 * (and should not) list them.
 *
 * With no mediaCodecs given, nothing is filtered.
 */
export function filterCapabilitiesByMediaCodecs(fullCaps, mediaCodecs) {
  if (!mediaCodecs || !mediaCodecs.length) return fullCaps;

  var all  = (fullCaps && fullCaps.codecs) || [];
  var kept = [];
  var keptPts = {};

  for (var i = 0; i < all.length; i++) {
    var cap = all[i];
    if (is_rtx_codec(cap)) continue;              // handled with its primary
    for (var j = 0; j < mediaCodecs.length; j++) {
      var want = mediaCodecs[j];
      if (want.kind && codec_kind(cap) !== want.kind) continue;
      if (!matchCodecs(cap, want)) continue;
      var entry = Object.assign({}, cap);
      // An app may pin a payload type; honour it, since its clients may
      // already be built around that number.
      if (want.preferredPayloadType != null) {
        entry.preferredPayloadType = want.preferredPayloadType;
      }
      // App-supplied parameters override ours (e.g. forcing stereo=0,
      // or a specific H.264 profile-level-id).
      if (want.parameters) {
        entry.parameters = Object.assign({}, entry.parameters || {}, want.parameters);
      }
      kept.push(entry);
      keptPts[cap.preferredPayloadType] = entry.preferredPayloadType;
      break;
    }
  }

  // Re-attach each surviving primary's RTX, re-pointing apt if the app
  // renumbered the primary.
  for (var k = 0; k < all.length; k++) {
    var rtx = all[k];
    if (!is_rtx_codec(rtx)) continue;
    var apt = Number(params_of(rtx).apt);
    if (!(apt in keptPts)) continue;
    var out = Object.assign({}, rtx);
    out.parameters = Object.assign({}, params_of(rtx), { apt: keptPts[apt] });
    kept.push(out);
  }

  return {
    codecs:           kept,
    headerExtensions: (fullCaps && fullCaps.headerExtensions) || [],
  };
}

export { is_rtx_codec as isRtxCodec };
