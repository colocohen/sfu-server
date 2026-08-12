/**
 * sfu.js — the SFU core, one domain file.
 *
 * House patterns (matching webrtc-server / stable-webrtc / turn-server):
 *   - constructor functions + closure state, no ES6 classes
 *   - ONE plain state object; records are plain objects in arrays
 *   - REACTIVE CASCADES: every trigger calls run_cascades(), each cascade
 *     checks CONDITIONS (not history), guarded + idempotent — no polling
 *   - callbacks + EventEmitter throughout; no promises in the engine
 *   - public surface camelCase, internal functions snake_case
 *
 * Vocabulary (mediasoup concepts over W3C connections):
 *   Router    — media hub; peers attach, producers get routed
 *   producer  — a stream a peer sends in            (plain record + handle)
 *   consumer  — that stream forwarded to a peer     (plain record + handle)
 *   Room      — Router + "everybody sees everybody" policy
 *
 * Integration points (verified against webrtc-server source):
 *   pc._manager                       — ConnectionManager (api.js:1192)
 *   manager.ev.on('rtp', fn)          — decrypted-RTP tap (rtp, rinfo, info{ssrc})
 *   manager.ev.on('pli', fn)          — a viewer asked US for a keyframe (ssrc)
 *   manager.sendRtp(pkt)              — inject: stamper→senderBuffer→BWE→SRTP
 *   manager.requestKeyframe(ssrc)     — PLI upstream for a remote ssrc
 *   transceiver.sender.ssrc           — outgoing SSRC (readable; api.js:1046)
 *   receiver.getSynchronizationSources() — incoming SSRC once media flows
 *   pc.addEventListener('track')      — producer detection without clobbering
 *                                       stable-webrtc's pc.ontrack property
 *   pc.addTransceiver(kind, sendonly) — consumer m-line, NO track → no encoder;
 *                                       we inject pre-encoded packets. Engine's
 *                                       onnegotiationneeded schedules the offer.
 */

import { EventEmitter } from 'node:events';
import {
  RtpForwarder, payloadOffset, readExtension,
  VP8Depacketizer, VP9Depacketizer,
  H264Depacketizer, H265Depacketizer, AV1Depacketizer,
} from 'rtp-packet';

var CONSUME_TIMEOUT_MS  = 15000;
var PLI_MIN_INTERVAL_MS = 500;

var _ids = { peer: 0, producer: 0, consumer: 0 };

// Lifecycle diagnostics — the three blind runs of M1 taught us the router
// must narrate itself. On when SFU_DEBUG or WEBRTC_DEBUG is set.
var _DIAG = !!(typeof process !== 'undefined' && process.env &&
               (process.env.SFU_DEBUG || process.env.WEBRTC_DEBUG));
// Ingress keyframe tracing (#26 diagnosis). Module scope, next to _diag,
// so it is visible everywhere _diag is — the previous attempt declared it
// inside a nested scope the tap could not see, and node --check plus the
// whole suite stayed green because no test drives a live RTP tap. Flip to
// false when the keyframe question is settled.
var _DBG_KF = true;
// Anchor-debt retry cadence (#26). Chosen at 1000ms against VP8 and then
// measured against H.264, where the first keyframe is several times
// larger and lands later: a full second between "the anchor was
// discarded in the pause window" and "ask again" is a full second of
// black for the viewer, and it was the difference between the H.264
// run that worked and the one that did not. Shorter is safe by
// construction — the retry stops the instant an anchor reaches the
// consumer, and KF_MAX_RETRIES bounds it besides.
var KF_RETRY_MS = 300;
// A codec whose keyframes we cannot recognise (AV1 today) would never
// settle its debt, and an unbounded PLI stream is worse than a late
// anchor. After this many tries the debt is written off: the producer
// has certainly sent one by now, we simply could not see it.
var KF_MAX_RETRIES = 10;

/* ── M3a: simulcast layer selection ──────────────────────────────
 * A simulcast producer is not one stream but several live at once
 * (r0/r1/r2). Which one a consumer forwards is a CONDITION over the
 * layers' current liveness — never a latched decision — so it lives in
 * the cascades like everything else.
 *
 * Ranking without rid: the tap counts bytes per ssrc, and a higher
 * spatial layer is unavoidably the fatter stream. That is codec- and
 * SDP-agnostic, needs no new engine↔transport coupling, and degrades
 * sanely if a layer is added or dropped mid-call. (When webrtc-server
 * grows a public rid accessor this becomes a label lookup instead of an
 * inference — the selection logic does not change.)
 */
var LAYER_STALE_MS   = 400;    // no packet for this long ⇒ layer is not carrying
var LAYER_UP_HOLD_MS = 2000;   // a better layer must stay fresh this long before we climb
var LAYER_RATE_WIN   = 3000;   // byte-rate observation window
var LAYER_SWITCH_MIN_MS = 500; // floor between two switches on ONE consumer

/* ── M3c-BWE: bandwidth-driven layer selection ────────────────────
 * The transport already measures what it can actually push to each
 * viewer (transport-cc + REMB, via getEstimatedBandwidth()). Until now
 * nothing consumed that number: we forwarded the best layer the PRODUCER
 * offered, regardless of whether the VIEWER's link could carry it —
 * which on a weak link means loss, NACK storms and stutter instead of a
 * clean drop to a smaller layer.
 *
 * The rule is deliberately conservative, in both directions:
 *   - a layer is affordable when the estimate covers its measured rate
 *     plus headroom (audio, retransmissions, and the estimate's own
 *     error all have to fit)
 *   - drop as soon as the current layer stops fitting; climb only after
 *     the estimate has been comfortably above the next one for a while
 * Asymmetry is the point: a viewer who cannot keep up is suffering NOW,
 * while a viewer who could take more is merely getting less than ideal.
 */
var BWE_HEADROOM      = 1.25;  // estimate must exceed layer rate by this factor
var BWE_CLIMB_MARGIN  = 1.6;   // and by this much before climbing back up
var BWE_MIN_SAMPLE_MS = 2000;  // ignore the estimate until it has settled

/* ── Packet inspection ────────────────────────────────────────────
 * None of this is SFU logic — it is RTP/codec knowledge, and it lives
 * in rtp-packet where the edge cases already are (STAP-A walking, FU-A
 * start bits, VP8's KEYIDX-without-TID descriptor, AV1's Z/N bits).
 * This file briefly carried its own copies of all of it; they were
 * deleted once it became clear the library had covered every codec the
 * whole time. What remains here is only the dispatch: which codec a
 * given producer speaks, and what a forwarder does with the answer.
 */
var KEYFRAME_PEEK = {
  VP8:  VP8Depacketizer.peekKeyframe,
  VP9:  VP9Depacketizer.peekKeyframe,
  H264: H264Depacketizer.peekKeyframe,
  H265: H265Depacketizer.peekKeyframe,
  AV1:  AV1Depacketizer.peekKeyframe,
};

var TEMPORAL_PEEK = {
  VP8: VP8Depacketizer.peekTemporalId,
  VP9: VP9Depacketizer.peekTemporalId,
};

// Does this packet start a decodable entry point? The anchor debt (#26)
// is settled by one, and a codec we cannot read would keep asking the
// producer for keyframes forever while the viewer stayed black — see
// KF_MAX_RETRIES for that case.
function is_keyframe(rtp, codecName) {
  // No default codec. An empty name used to fall back to VP8, which
  // meant "parse these bytes as a VP8 descriptor" for a stream we had
  // simply failed to identify — a guess that can report a keyframe that
  // never happened. Unknown means unknown; KF_MAX_RETRIES covers the
  // consequence.
  var peek = codecName ? KEYFRAME_PEEK[String(codecName).toUpperCase()] : null;
  if (!peek) return false;
  var ps = payloadOffset(rtp);
  if (ps < 0 || ps >= rtp.length) return false;
  return !!peek(rtp.subarray(ps));
}

// Which temporal layer is this packet in? null means the stream carries
// no temporal structure, and such a packet must be forwarded to
// everyone — a consumer that caps temporal layers must not silence a
// non-layered stream.
function packet_tid(rtp, codecName) {
  // Same rule as is_keyframe: never guess the codec. A wrong guess here
  // is worse than no answer — a fabricated TID makes a forwarder drop
  // packets a viewer needed.
  var peek = codecName ? TEMPORAL_PEEK[String(codecName).toUpperCase()] : null;
  if (!peek) return null;                  // unknown, or a codec with no temporal info
  var ps = payloadOffset(rtp);
  if (ps < 0 || ps >= rtp.length) return null;
  return peek(rtp.subarray(ps));
}

// Application listeners run inside engine operations, so a handler that
// throws must not take the operation down with it. The tap already had
// this discipline (its throw reached node's uncaught-exception path and
// wedged a whole process); the same rule belongs on every emit the
// engine makes from inside a public method.
function make_safe_emit(ev, diag) {
  return function (name) {
    var args = Array.prototype.slice.call(arguments);
    try { ev.emit.apply(ev, args); }
    catch (e) { diag('listener for "' + name + '" threw and was contained: ' + (e && e.message)); }
  };
}

function _diag(msg) {
  if (_DIAG && typeof console !== 'undefined') console.log('[sfu-diag] ' + msg);
}

function unwrap_pc(peer_or_pc) {
  if (!peer_or_pc) return null;
  if (peer_or_pc.connection && peer_or_pc.connection.pc) return peer_or_pc.connection.pc;  // stable-webrtc Peer
  if (typeof peer_or_pc.addTransceiver === 'function') return peer_or_pc;                  // raw pc
  return null;
}


/* ═══════════════════════════ Router ═══════════════════════════ */

function Router(options) {
  var self = this;
  var ev = new EventEmitter();
  var emit_safely = make_safe_emit(ev, _diag);

  var state = {
    closed: false,
    peers: [],        // { id, pc, manager, wrapper, handle, closed, _listeners, _tap, _pli }
    producers: [],    // { id, peer, kind, mid, track, transceiver, ssrc, closed, handle }
    consumers: [],    // { id, peer, producer, transceiver, ssrc, pt, fwd, bound, paused,
                      //   last_pli_at, created_at, cb, closed, handle }
    // Hot-path routing table, rebuilt on bind/unbind:
    //   routes[producing_peer.id][src_ssrc] = [consumer records]
    routes: {},
  };

  /* ── Reactive core ─────────────────────────────────────────────
   * Triggers (pc events, first media packet, timeout timers) mutate
   * records in place and then call run_cascades(). Each cascade is a
   * CONDITION over current state with a guard that makes re-runs
   * no-ops — the connection_manager doctrine.
   */
  // RE-ENTRANCY GUARD + PASS BUDGET.
  //
  // The cascades are meant to be re-run freely — that is the whole
  // doctrine — but every cascade may now MUTATE state that other
  // cascades read (3c moves routes and emits 'consumerlayerschange';
  // handles call back in through setPreferredSpatialLayer). A listener
  // that reaches run_cascades() from inside a pass turns a converging
  // loop into an unbounded one, and a self-feeding pair of conditions
  // (3c flipping a consumer between two layers that each look best to
  // the other) would spin the event loop forever — the process stops
  // answering anything, including Ctrl+C, which is exactly the failure
  // seen live with 4+ participants.
  //
  // So: a pass in progress records that a re-run was asked for and
  // returns immediately; the outer pass loops again to honour it, up to
  // a budget. Hitting the budget is a BUG, not a condition, so it says
  // so loudly instead of hanging silently.
  var _cascade_depth = 0, _cascade_again = false;
  var CASCADE_MAX_PASSES = 8;

  function run_cascades() {
    if (state.closed) return;
    if (_cascade_depth > 0) { _cascade_again = true; return; }

    _cascade_depth++;
    try {
      var passes = 0;
      do {
        _cascade_again = false;
        run_cascades_once();
        passes++;
        if (passes >= CASCADE_MAX_PASSES && _cascade_again) {
          console.warn('[sfu-server] cascade budget exhausted after ' + passes +
                       ' passes — a cascade keeps re-triggering itself; ' +
                       'stopping this cycle to keep the event loop alive.');
          break;
        }
      } while (_cascade_again);
    } finally {
      _cascade_depth--;
    }
  }

  function run_cascades_once() {
    for (var i = state.consumers.length - 1; i >= 0; i--) {
      var c = state.consumers[i];
      if (!c || c.closed) continue;

      // 0. Transceiver: the consuming peer's pc exists (attached) AND
      //    its INITIAL offer/answer has settled. The second condition is
      //    the glare gate, learned in M1 run 3: joining triggers consume
      //    instantly, the shared-cert cache makes the pc exist within
      //    ms, and addTransceiver at that moment made the SERVER offer
      //    first — colliding with the browser's own initial offer.
      //    stable-webrtc resolved the glare correctly (our offer rolled
      //    back, browser's won) but our m-lines died with the rollback
      //    and no re-offer followed. Waiting for remoteDescription +
      //    stable means we only ever ADD to a settled session, so our
      //    renegotiation is the only offer in flight. (Same reason
      //    mediasoup consumes only on connected transports.)
      if (c.transceiver == null && !c.peer.plain && c.peer.attached && !c.peer.closed &&
          c.peer.pc.remoteDescription && c.peer.pc.signalingState === 'stable') {
        try {
          // streamIds: group this consumer's track under its SOURCE
          // PARTICIPANT's stream — the viewer gets one MediaStream per
          // participant (video+audio paired: correct grouping, correct
          // lipsync, one <video> per person). Without it every forwarded
          // track claimed the same default stream and browsers merged
          // all participants into one.
          c.transceiver = c.peer.pc.addTransceiver(c.producer.kind, {
            direction: 'sendonly',
            streamIds: [c.producer.peer.id],
          });
          _diag(c.id + ' cascade0: addTransceiver(' + c.producer.kind + ', sendonly) — mid pending negotiation');
        } catch (e) {
          var cb0 = c.cb; c.cb = null;
          close_consumer(c);
          if (cb0) cb0(e);
          continue;
        }
      }

      // 1. Outgoing SSRC: negotiation committed a local description →
      //    the sendonly transceiver's sender has its SSRC.
      // A PLAIN peer has no transceiver to read an outgoing ssrc from,
      // and the caller may not have supplied one (the mediasoup
      // PlainTransport path does; the native addPlainPeer path need
      // not). Allocate one: the identifiers are ours to choose, and
      // without this the consume parks until the router closes.
      if (c.ssrc == null && c.peer.plain) {
        c.ssrc = (Math.random() * 0xfffffff) >>> 0;
        c.pt = c.pt != null ? c.pt : (c.producer.kind === 'video' ? 96 : 111);
        _diag(c.id + ' cascade1: plain peer — allocated ssrc=' + c.ssrc + ' pt=' + c.pt);
      }

      if (c.ssrc == null && c.transceiver && c.transceiver.sender &&
          c.transceiver.sender.ssrc != null) {
        c.ssrc = c.transceiver.sender.ssrc >>> 0;
        _diag(c.id + ' cascade1: outgoing ssrc=' + c.ssrc);
      }

      // 2. Outgoing payload type: the viewer's ANSWER names the PT for
      //    the producer's codec on our mid.
      if (c.ssrc != null && c.pt == null) {
        c.pt = resolve_payload_type(c);
        if (c.pt != null) _diag(c.id + ' cascade2: outgoing pt=' + c.pt);
      }

      // 3. Producer SSRC: once its peer's media actually flows, the
      //    W3C surface exposes it. (The trigger that gets us here is the
      //    producing peer's first tap packet — see attach_peer_tap.)
      var p = c.producer;
      if (p.ssrc == null && !p.closed) {
        p.ssrc = read_producer_ssrc(p);
        if (p.ssrc != null) {
          _diag(p.id + ' cascade3: producer ssrc=' + p.ssrc);
          // A CVO watch registered before the ssrc was known (the app
          // subscribes right after produce()) becomes reachable now.
          if (p._cvo_wanted) {
            if (!p.peer._cvo_watch) p.peer._cvo_watch = {};
            p.peer._cvo_watch[p.ssrc] = p;
          }
        }
      }

      // 3b. SSRC DRIFT: the source SSRC is a CONDITION, not history.
      //     Chrome recreates senders on renegotiation rollback (glare on
      //     the producing tab's addStream) and the stream continues under
      //     a NEW ssrc — the latched key goes stale and every consumer
      //     of this producer freezes at its last counter (found live in
      //     M2: [sfu-fwd] showed all of one peer's consumers frozen
      //     while its siblings grew). Re-read on every cascade pass; on
      //     drift: re-key the routes and glide each forwarder through
      //     switchSource() so seq/ts stay continuous for the viewers.
      if (p.ssrc != null && !p.closed) {
        var cur = read_producer_ssrc(p);
        if (cur != null && cur !== p.ssrc && p._cvo_wanted && p.peer._cvo_watch) {
          delete p.peer._cvo_watch[p.ssrc];
          p.peer._cvo_watch[cur] = p;
        }
        if (cur != null && cur !== p.ssrc) {
          _diag(p.id + ' SSRC DRIFT ' + p.ssrc + ' → ' + cur + ' — re-keying routes');
          // Only the REFERENCE moves here. Which ssrc each consumer
          // actually forwards is cascade 3c's business now (M3a): with
          // simulcast a producer has several live ssrcs and consumers can
          // legitimately sit on different ones, so a producer-wide re-key
          // would trample per-consumer choices. 3c sees the dead layer as
          // stale on its next pass and moves only whoever was on it.
          p.ssrc = cur;
        }
      }

      // 3c. LAYER SELECTION (M3a). Bound video consumers of a simulcast
      //     producer re-evaluate their source every pass: a layer that
      //     stopped carrying (Chrome sheds r1/r2 under BWE or when the
      //     tab backgrounds) is a frozen viewer within LAYER_STALE_MS,
      //     and the fix is to ride the layer that IS carrying. Because
      //     this is a condition and not an event, it also covers layers
      //     appearing late, dying, and coming back.
      if (c.bound && !c.paused && p.kind === 'video' &&
          (!c.last_switch_at || Date.now() - c.last_switch_at > LAYER_SWITCH_MIN_MS)) {
        var lys = producer_layers(p);
        if (lys.length > 1 || (lys.length === 1 && lys[0].ssrc !== c.src_ssrc)) {
          var want = choose_layer(c, lys, consumer_budget_bps(c));
          if (want && want.ssrc !== c.src_ssrc) {
            _diag(c.id + ' cascade3c: layer switch ' + c.src_ssrc + ' → ' + want.ssrc +
                  ' (' + lys.length + ' layers, rate=' + Math.round(want.rate) + 'B/s)');
            switch_consumer_layer(c, want.ssrc);
          }
        }
      }

      // 4. BIND — all conditions met, exactly once.
      if (!c.bound && c.ssrc != null && c.pt != null && p.ssrc != null) {
        bind_consumer(c);
      }

      // 5. Timeout — negotiation/media never converged (covers the
      //    never-attached peer too: created_at counts from consume()).
      if (!c.bound && c.cb && (Date.now() - c.created_at) > CONSUME_TIMEOUT_MS) {
        var cb = c.cb; c.cb = null;
        close_consumer(c);
        cb(new Error('consume: did not converge in ' + CONSUME_TIMEOUT_MS + 'ms ' +
          '(peerAttached=' + c.peer.attached + ' outSsrc=' + c.ssrc +
          ' outPt=' + c.pt + ' producerSsrc=' + p.ssrc + ')'));
      }
    }
  }

  function resolve_payload_type(c) {
    if (!c.transceiver || !c.peer.pc) return null;
    var desc = c.peer.pc.remoteDescription;
    if (!desc || !desc.sdp) return null;
    var mid = c.transceiver.mid;
    if (mid == null) return null;
    var codec = producer_codec_name(c.producer);
    var sections = desc.sdp.split(/\r?\nm=/);
    for (var i = 1; i < sections.length; i++) {
      var sec = 'm=' + sections[i];
      var m = sec.match(/a=mid:(\S+)/);
      if (!m || m[1] !== String(mid)) continue;
      var r = sec.match(new RegExp('a=rtpmap:(\\d+) ' + codec + '/', 'i'));
      return r ? parseInt(r[1], 10) : null;
    }
    return null;
  }

  function resolve_rtx_payload_type(c, primaryPt) {
    // The viewer's answer maps RTX per m-line: a=rtpmap:<N> rtx/90000 +
    // a=fmtp:<N> apt=<primaryPt>. Audio typically has no RTX — null is a
    // valid outcome and simply means NACKs on that stream go unserved
    // (opus conceals short losses anyway).
    if (primaryPt == null || !c.transceiver || !c.peer.pc) return null;
    var desc = c.peer.pc.remoteDescription;
    if (!desc || !desc.sdp) return null;
    var mid = c.transceiver.mid;
    if (mid == null) return null;
    var sections = desc.sdp.split(/\r?\nm=/);
    for (var i = 1; i < sections.length; i++) {
      var sec = 'm=' + sections[i];
      var m = sec.match(/a=mid:(\S+)/);
      if (!m || m[1] !== String(mid)) continue;
      var f = sec.match(new RegExp('a=fmtp:(\\d+) apt=' + primaryPt + '(?:\\D|$)'));
      return f ? parseInt(f[1], 10) : null;
    }
    return null;
  }

  // HOT PATH. This used to run per packet, and getParameters() is not a
  // field read — it builds a parameters object and, on this stack, walks
  // the parsed SDP. The answer cannot change without a renegotiation
  // that replaces the transceiver, so resolve it once and keep it on the
  // record; the cache is dropped whenever the transceiver changes.
  function producer_codec_name(p) {
    if (p.plain) return p.codecName || '';
    if (p._codecName != null && p._codecFor === p.transceiver) return p._codecName;
    var name = resolve_producer_codec_name(p);
    p._codecName = name;
    p._codecFor  = p.transceiver;
    return name;
  }

  // The negotiated id of a header extension on THIS producer's m-line.
  // Cached with the same discipline as the codec name: it cannot change
  // without a renegotiation that replaces the transceiver.
  function producer_ext_id(p, uri) {
    if (p._extIds && p._extFor === p.transceiver) return p._extIds[uri];
    var map = {};
    try {
      var recv = p.transceiver && p.transceiver.receiver;
      if (recv && typeof recv.getParameters === 'function') {
        var ps = recv.getParameters();
        var exts = (ps && ps.headerExtensions) || [];
        for (var i = 0; i < exts.length; i++) {
          if (exts[i] && exts[i].uri != null) map[exts[i].uri] = exts[i].id;
        }
      }
    } catch (e) {}
    p._extIds = map;
    p._extFor = p.transceiver;
    return map[uri];
  }

  function resolve_producer_codec_name(p) {
    try {
      var recv = p.transceiver && p.transceiver.receiver;
      if (recv && typeof recv.getParameters === 'function') {
        var params = recv.getParameters();
        if (params && params.codecs && params.codecs.length && params.codecs[0].mimeType) {
          return params.codecs[0].mimeType.split('/')[1];
        }
      }
    } catch (e) {}
    // NO GUESS. This used to answer 'VP8' for any video producer whose
    // codec could not be resolved, and that answer flows straight into
    // is_keyframe: a VP8 descriptor read over an H.264 packet can report
    // a keyframe that never happened, settling the anchor debt early and
    // leaving the viewer black with nobody left to ask. An empty name
    // means "unknown", which the keyframe path treats as "cannot tell"
    // and the retry cap resolves. The two consumers that want a label
    // for logging or for registerOutboundStream fall back themselves.
    return '';
  }

  // The producer's live layers, scoped to ITS receiver (so a peer's other
  // m-line can never leak in), ranked low→high by observed byte rate.
  function producer_layers(p) {
    var out = [];
    if (p.plain) {
      var st0 = (p.peer._src_stats || {})[p.ssrc];
      return [{
        ssrc:  p.ssrc,
        rate:  st0 ? (st0.rate || st0.win_bytes) : 0,
        fresh: !!(st0 && Date.now() - st0.last_at < LAYER_STALE_MS),
      }];
    }
    try {
      var recv = p.transceiver && p.transceiver.receiver;
      if (!recv || typeof recv.getSynchronizationSources !== 'function') return out;
      var srcs = recv.getSynchronizationSources() || [];
      var st = (p.peer._src_stats) || {};
      var now = Date.now();
      for (var i = 0; i < srcs.length; i++) {
        if (srcs[i].source == null) continue;
        var ssrc = srcs[i].source >>> 0;
        var o = st[ssrc];
        out.push({
          ssrc:  ssrc,
          rate:  o ? (o.rate || o.win_bytes) : 0,
          fresh: !!(o && now - o.last_at < LAYER_STALE_MS),
        });
      }
      out.sort(function (a, b) { return a.rate - b.rate; });   // r0 … r2
    } catch (e) {}
    return out;
  }

  // Which layer SHOULD this consumer be forwarding right now?
  //   - never a layer above its preferred spatial index (M3b hook)
  //   - the highest FRESH candidate wins (mediasoup's default is the
  //     best available; bandwidth-driven capping is M3c)
  //   - climbing waits out LAYER_UP_HOLD_MS of continuous freshness so a
  //     layer that flickers back for one burst cannot start a ping-pong;
  //     dropping is immediate, because a stalled layer is a frozen viewer
  function choose_layer(c, layers, budget) {
    if (!layers.length) return null;
    var maxIdx = (c.preferred_spatial == null) ? layers.length - 1
                                               : Math.min(c.preferred_spatial, layers.length - 1);

    // BANDWIDTH CEILING. Applied on top of the app's ceiling, never
    // instead of it: setPreferredLayers is a promise to the app that it
    // will not be given MORE than it asked for, and an estimate must not
    // override that. A layer whose measured rate we do not have yet
    // (rate 0) is treated as affordable — refusing to forward a stream
    // because it has not been measured would be worse than forwarding it.
    if (budget != null) {
      var curIdxB = -1;
      for (var ci = 0; ci < layers.length; ci++) {
        if (layers[ci].ssrc === c.src_ssrc) { curIdxB = ci; break; }
      }
      var afford = -1;
      for (var bi = 0; bi <= maxIdx; bi++) {
        var need = layers[bi].rate;
        if (!need) { afford = bi; continue; }                 // unmeasured: allow
        // The wider margin guards CLIMBING only. Applying it downwards
        // too (caught by the policy tests) made a viewer that had to
        // shed one layer fall all the way to the bottom, which is the
        // opposite of graceful: dropping is already the cautious move,
        // and it should land on the largest layer that fits.
        var climbing = (curIdxB >= 0 && bi > curIdxB);
        var factor = climbing ? BWE_CLIMB_MARGIN : BWE_HEADROOM;
        if (budget >= need * factor) afford = bi;
      }
      if (afford >= 0 && afford < maxIdx) maxIdx = afford;
      else if (afford < 0) maxIdx = 0;                        // nothing fits: lowest layer
    }
    var curIdx = -1;
    for (var i = 0; i < layers.length; i++) if (layers[i].ssrc === c.src_ssrc) { curIdx = i; break; }

    var best = null, bestIdx = -1;
    for (var j = 0; j <= maxIdx; j++) {
      if (layers[j].fresh) { best = layers[j]; bestIdx = j; }
    }
    if (!best) return null;                       // nothing carrying: hold what we have
    if (curIdx < 0) return best;                  // unbound / our layer vanished entirely
    if (bestIdx <= curIdx) return best;           // same or lower — take it immediately
    // climbing: require sustained freshness
    var now = Date.now();
    if (!c.up_since || c.up_target !== best.ssrc) { c.up_since = now; c.up_target = best.ssrc; return layers[curIdx].fresh ? layers[curIdx] : best; }
    if (now - c.up_since < LAYER_UP_HOLD_MS) return layers[curIdx].fresh ? layers[curIdx] : best;
    return best;
  }

  // Move a bound consumer onto a different source ssrc: re-key the hot
  // table, glide the forwarder so the viewer's seq/ts stay continuous,
  // and take on a keyframe debt — the new layer's frames are useless
  // until an anchor from IT arrives (#26 settles the debt).
  function switch_consumer_layer(c, ssrc) {
    c.last_switch_at = Date.now();
    var pid = c.producer.peer.id;
    var by = state.routes[pid] || (state.routes[pid] = {});
    var old = by[c.src_ssrc];
    if (old) {
      var oi = old.indexOf(c);
      if (oi >= 0) old.splice(oi, 1);
      if (!old.length) delete by[c.src_ssrc];
    }
    c.src_ssrc = ssrc;
    (by[ssrc] || (by[ssrc] = [])).push(c);
    try { if (c.fwd && typeof c.fwd.switchSource === 'function') c.fwd.switchSource(); } catch (e) {}
    c.kf_pending = true;
    c.last_pli_at = 0;              // ask now, don't wait out the retry window
    request_keyframe(c.producer, ssrc);
    if (c.handle) c.handle.currentLayerSsrc = ssrc;
    // The tap is the hot path: an application (or compat) listener that
    // throws must not abort a layer switch, and must not reach node's
    // uncaught-exception path from inside packet handling.
    try { ev.emit('consumerlayerschange', c.handle, { ssrc: ssrc }); }
    catch (e) { _diag('consumerlayerschange listener threw: ' + (e && e.message)); }
  }

  // What this consumer's OWN link can carry, in bytes/sec to match the
  // layer rates we measure. null when the transport cannot say yet.
  function consumer_budget_bps(c) {
    try {
      var mgr = c.peer && c.peer.manager;
      if (!mgr || typeof mgr.getEstimatedBandwidth !== 'function') return null;
      var bps = mgr.getEstimatedBandwidth();
      if (!bps || !isFinite(bps)) return null;
      if (!c.peer._bwe_since) c.peer._bwe_since = Date.now();
      if (Date.now() - c.peer._bwe_since < BWE_MIN_SAMPLE_MS) return null;
      return bps / 8;   // bytes/sec
    } catch (e) { return null; }
  }

  function spatial_index_of(c) {
    if (!c.producer || c.producer.kind !== 'video' || c.src_ssrc == null) return null;
    var lys = producer_layers(c.producer);
    for (var i = 0; i < lys.length; i++) if (lys[i].ssrc === c.src_ssrc) return i;
    return null;
  }

  function read_producer_ssrc(p) {
    // Declared, not discovered: there is no receiver to ask.
    if (p.plain) return p.ssrc;
    try {
      var recv = p.transceiver && p.transceiver.receiver;
      if (recv && typeof recv.getSynchronizationSources === 'function') {
        var srcs = recv.getSynchronizationSources();
        if (srcs && srcs.length) {
          // STICKINESS (#25b, exposed the moment the always-nudge made
          // drift reachable): under simulcast several layer ssrcs are
          // live AT ONCE, and the most-recent entry flip-flops between
          // them packet-by-packet — naive "take srcs[0]" made cascade
          // 3b re-key r0↔r1 every rate-limit window, PLI-storming the
          // producer and glitching every viewer. Drift means the bound
          // ssrc is GONE from the live set, not that someone else
          // happened to speak last: while p.ssrc is still among the
          // fresh sources, keep it.
          if (p.ssrc != null) {
            for (var si = 0; si < srcs.length; si++) {
              if (srcs[si].source != null && (srcs[si].source >>> 0) === p.ssrc) return p.ssrc;
            }
          }
          if (srcs[0].source != null) return srcs[0].source >>> 0;
        }
      }
    } catch (e) {}
    return null;
  }

  function bind_consumer(c) {
    c.bound = true;
    _diag(c.id + ' cascade4: BIND — forwarding ' + c.producer.ssrc + ' → ssrc=' + c.ssrc + ' pt=' + c.pt);
    c.fwd = new RtpForwarder({ ssrc: c.ssrc, payloadType: c.pt });

    // Register our injected SSRC with the outbound machinery: SR
    // generation needs the clockRate to compute RTP timestamps at NTP
    // time — without this, forwarded streams get no valid Sender
    // Reports and the viewer's A/V sync degrades.
    try {
      if (c.peer.plain) throw 0;   // no sender, no SR machinery: a plain sink just receives RTP
      var rtxPt = resolve_rtx_payload_type(c, c.pt);
      var rtxSsrc = null;
      try { rtxSsrc = c.transceiver.sender.rtxSsrc; } catch (e) {}
      if (rtxPt != null && rtxSsrc != null) {
        _diag(c.id + ' RTX armed: rtxSsrc=' + rtxSsrc + ' rtxPt=' + rtxPt);
      }
      c.peer.manager.registerOutboundStream(c.ssrc, {
        payloadType:    c.pt,
        clockRate:      c.producer.kind === 'video' ? 90000 : 48000,
        // Label only: the outbound machinery uses it for SR/codec
        // bookkeeping, where a sensible default beats an empty string.
        codecName:      producer_codec_name(c.producer) ||
                        (c.producer.kind === 'video' ? 'VP8' : 'opus'),
        mid:            c.transceiver.mid != null ? String(c.transceiver.mid) : null,
        rtxSsrc:        rtxSsrc,
        rtxPayloadType: rtxPt,
      });
    } catch (e) {}

    // Which layer do we start on? The same condition 3c will keep
    // applying — so a consumer never begins life on a layer that is
    // already silent (the freeze seen live: we latched r1 exactly as
    // Chrome shed it, and nothing re-evaluated).
    var pid = c.producer.peer.id;
    var src = c.producer.ssrc;
    if (c.producer.kind === 'video') {
      var l0 = producer_layers(c.producer);
      var pick = choose_layer(c, l0, consumer_budget_bps(c));
      if (pick) src = pick.ssrc;
    }
    c.src_ssrc = src;
    if (c.handle) c.handle.currentLayerSsrc = src;
    if (!state.routes[pid]) state.routes[pid] = {};
    if (!state.routes[pid][src]) state.routes[pid][src] = [];
    state.routes[pid][src].push(c);
    refresh_unbound_flags();

    // A viewer joining mid-stream can't decode until a keyframe — ask
    // upstream immediately instead of waiting for its PLI round-trip.
    c.last_pli_at = Date.now();
    if (c.producer.kind === 'video') c.kf_pending = true;   // owes its viewer an anchor
    request_keyframe(c.producer, c.src_ssrc);

    if (c.cb) { var cb = c.cb; c.cb = null; cb(null, c.handle); }
    emit_safely('consumer', c.handle);
  }

  // A PLI names ONE stream. With simulcast the stream a viewer needs an
  // anchor for is its own layer, not the producer's nominal ssrc.
  function request_keyframe(p, ssrc) {
    var target = (ssrc != null) ? ssrc : p.ssrc;
    if (target == null) return;
    _diag(p.id + ' requestKeyframe → upstream PLI for ssrc=' + target);
    try { p.peer.manager.requestKeyframe(target); } catch (e) {}
    try { ev.emit('producerpli', p.handle, { ssrc: target }); }
    catch (e) { _diag('producerpli listener threw: ' + (e && e.message)); }
  }

  /* ── Hot path ──────────────────────────────────────────────────
   * ONE tap per peer. Dual purpose:
   *  (a) forwarding: routes[peer][ssrc] → rewrite → inject
   *  (b) trigger: first packets are when producer SSRCs become
   *      readable — an unroutable ssrc while unbound consumers exist
   *      schedules a cascade run (the _has_unbound guard keeps this
   *      off the steady-state path).
   */
  function attach_peer_tap(peer) {
    peer._tap = function (rtp, rinfo, info) {
      try { tap_inner(rtp, rinfo, info); }
      catch (e) {
        if (!peer._tapFaulted) {
          peer._tapFaulted = true;
          console.error('[sfu-server] RTP tap threw and was contained ' +
                        '(reported once per peer): ' + (e && e.stack || e));
        }
      }
    };

    function tap_inner(rtp, rinfo, info) {
      if (state.closed || peer.closed || !info) return;

      // Per-SSRC observation: liveness + volume for every stream this
      // peer sends. Cascade 3c reads it; nothing else may write it.
      var st = peer._src_stats || (peer._src_stats = {});
      var e = st[info.ssrc];
      var nowP = Date.now();   // one clock read for the whole packet
      if (!e) { e = st[info.ssrc] = { bytes: 0, win_bytes: 0, win_at: nowP, rate: 0, last_at: 0 }; }
      e.bytes += rtp.length; e.win_bytes += rtp.length; e.last_at = nowP;
      if (nowP - e.win_at >= LAYER_RATE_WIN) {
        e.rate = e.win_bytes / ((nowP - e.win_at) / 1000);
        e.win_bytes = 0; e.win_at = nowP;
      }

      // CVO: opt-in per producer. The flag is set only when an app has
      // subscribed to 'videoorientationchange' on the matching Producer.
      if (peer._cvo_watch) {
        var wp = peer._cvo_watch[info.ssrc];
        if (wp && !wp.closed) {
          var cvoId = producer_ext_id(wp, 'urn:3gpp:video-orientation');
          if (cvoId != null) {
            // rtp-packet reads the extension; the SFU only decides
            // which one it wants and what to do with the answer.
            var cvoBuf = readExtension(rtp, cvoId);
            var cvo = cvoBuf ? cvoBuf[0] : null;
            if (cvo != null && cvo !== wp._cvo_last) {
              wp._cvo_last = cvo;
              try { ev.emit('producervideoorientation', wp.handle, cvo); }
              catch (e) { _diag('videoorientation listener threw: ' + (e && e.message)); }
            }
          }
        }
      }

      var by_ssrc = state.routes[peer.id];
      var list = by_ssrc && by_ssrc[info.ssrc];

      if (list) {
        // KEYFRAME ANCHOR RETRY (#26 — the last black window). Measured
        // chain: Chrome answers a PLI on the bound layer, but the
        // consumer is still paused (mediasoup's own paused:true
        // practice) and the forwarder drops the very anchor it is
        // waiting for; the resume-time requests that follow all land
        // inside Chrome's keyframe throttle window — it just sent one —
        // so they are satisfied by the frame we already discarded, and
        // NOBODY ASKS AGAIN. The viewer decodes nothing forever while
        // packets pour in ([ms-fwd] climbing, zero [ms-kf]).
        //
        // So a consumer carries an explicit debt: from bind and from
        // every resume it OWES its viewer a keyframe, and the debt is
        // only settled by a keyframe that actually leaves for it.
        // Incoming packets are the clock — no new timer, and the retry
        // stops the instant the anchor gets through.
        // ONE pass over the route list to learn what this packet needs
        // parsing for. Two separate scans (keyframe debt, temporal caps)
        // walked the same array twice per packet for answers that are
        // almost always both false.
        var nowT2 = nowP, needKf = false, anyCap = false;
        for (var ai = 0; ai < list.length; ai++) {
          var ac = list[ai];
          if (ac.closed) continue;
          if (!ac.paused && ac.kf_pending) needKf = true;
          if (ac.preferred_temporal != null) anyCap = true;
          if (needKf && anyCap) break;
        }
        // The codec name is needed only if we are going to parse.
        var codecName = (_DBG_KF || needKf || anyCap)
                        ? producer_codec_name(list[0].producer) : '';
        var isKf = (_DBG_KF || needKf) ? is_keyframe(rtp, codecName) : false;
        var tid  = anyCap ? packet_tid(rtp, codecName) : undefined;

        if (_DBG_KF && isKf) {
          if (!peer._kfLast) peer._kfLast = {};
          var kk = 'k' + info.ssrc;
          if (!peer._kfLast[kk] || nowT2 - peer._kfLast[kk] > 500) {
            peer._kfLast[kk] = nowT2;
            var live = 0, pausedN = 0;
            for (var kq = 0; kq < list.length; kq++) {
              if (list[kq].closed) continue;
              if (list[kq].paused) pausedN++; else live++;
            }
            _diag('[sfu-kf-in] ' + peer.id + ' KEYFRAME ssrc=' + info.ssrc + ' → ' +
                  live + ' live route(s)' +
                  (pausedN ? ', ' + pausedN + ' PAUSED (anchor discarded)' : ''));
          }
        }

        // Read the TID once per packet, and only when some consumer on
        // this route actually caps temporal layers — an untouched route
        // pays one boolean.
        var tid;
        var anyCap = false;
        for (var ti = 0; ti < list.length; ti++) {
          if (!list[ti].closed && list[ti].preferred_temporal != null) { anyCap = true; break; }
        }
        if (anyCap) tid = packet_tid(rtp, producer_codec_name(list[0].producer));

        for (var i = 0; i < list.length; i++) {
          var c = list[i];
          if (c.closed || c.paused) continue;
          // TEMPORAL FILTERING. A packet above the consumer's ceiling is
          // dropped outright: the encoder guaranteed nothing below it
          // references it, so the decoder never notices. Packets with no
          // temporal information (tid === null) always pass — a stream
          // without temporal structure must not go dark because an app
          // asked for a low temporal layer.
          if (c.preferred_temporal != null && tid != null && tid > c.preferred_temporal) {
            continue;
          }
          if (tid != null && tid !== c.current_temporal) {
            c.current_temporal = tid;
          }
          var out = c.fwd.forward(rtp);
          if (out) { try { c.peer.manager.sendRtp(out); } catch (e) {} }
          if (c.kf_pending) {
            if (isKf) {
              c.kf_pending = false;
              c.kf_tries = 0;
              _diag(c.id + ' keyframe anchor delivered — debt settled');
            } else if (nowT2 - c.last_pli_at > KF_RETRY_MS) {
              c.last_pli_at = nowT2;
              c.kf_tries = (c.kf_tries || 0) + 1;
              if (c.kf_tries > KF_MAX_RETRIES) {
                c.kf_pending = false;
                _diag(c.id + ' anchor debt written off after ' + (c.kf_tries - 1) +
                      ' tries (codec keyframes not recognised?)');
              } else {
                _diag(c.id + ' still without an anchor — re-requesting keyframe');
                request_keyframe(c.producer, c.src_ssrc);
              }
            }
          }
        }
        return;
      }
      // unknown ssrc from this peer — a producer may just have started
      // (pre-bind)… OR a bound producer's stream has MOVED to a new
      // ssrc. Harvest #25, caught live under the official demo: Chrome
      // rolls simulcast layer ssrcs under BWE/background pressure (an
      // old r0 dies; fresh r0/r1/r2 appear mid-call), and this nudge
      // used to fire only while `peer._has_unbound` — so drift AFTER
      // full bind never woke the cascades, 3b sat unreachable, and the
      // route stayed keyed to a dead ssrc ([ms-fwd] frozen at 138 while
      // the same peer's audio kept climbing; 'SSRC DRIFT' count: 0).
      // The M2 doctrine said it first: ssrc is a CONDITION, not
      // history — so the trigger can't be gated on bind state either.
      // Always nudge, rate-limited: a packet storm on an unknown ssrc
      // costs one cascade pass per window, not one per packet.
      var nowT = Date.now();
      if (!state.last_unknown_nudge || nowT - state.last_unknown_nudge > 250) {
        state.last_unknown_nudge = nowT;
        run_cascades();
        refresh_unbound_flags();
      }
    }
    peer.manager.ev.on('rtp', peer._tap);
    // One listener per peer on a shared emitter: past ten peers node
    // starts warning about a leak that isn't one. Say so explicitly
    // rather than let a real leak hide behind a suppressed warning.
    try {
      if (typeof peer.manager.ev.setMaxListeners === 'function') {
        peer.manager.ev.setMaxListeners(0);
      }
    } catch (e) {}

    // Viewer PLI on one of OUR outgoing ssrcs → throttle → upstream.
    peer._pli = function (media_ssrc) {
      if (state.closed || peer.closed) return;
      var s = media_ssrc >>> 0;
      for (var i = 0; i < state.consumers.length; i++) {
        var c = state.consumers[i];
        if (c.closed || !c.bound || c.peer !== peer || c.ssrc !== s) continue;

        // OBSERVABILITY IS NOT CONDITIONAL. The throttle below decides
        // whether we RELAY the request; it must not decide whether the
        // app gets to see that a viewer asked. mediasoup's trace stream
        // is how an app knows its viewers are struggling, and a
        // throttled PLI is exactly the interesting case.
        try { ev.emit('consumerpli', c.handle, { ssrc: s }); }
        catch (e) { _diag('consumerpli listener threw: ' + (e && e.message)); }

        var now = Date.now();
        if (now - c.last_pli_at < PLI_MIN_INTERVAL_MS) continue;
        c.last_pli_at = now;
        _diag(c.id + ' viewer PLI on ssrc=' + s + ' → relaying upstream');
        request_keyframe(c.producer, c.src_ssrc);
      }
    };
    peer.manager.ev.on('pli', peer._pli);

    // NACK / FIR: the transport already parses every RTCP packet and
    // re-emits it, so the remaining trace types cost a subscription
    // rather than new plumbing. Attached only when something asks —
    // see watchFeedback below — because RTCP on a busy connection is a
    // steady stream and formatting it for nobody is waste.
    peer._rtcp = function (raw, rinfo, parsed) {
      if (state.closed || peer.closed || !parsed) return;
      var name = parsed.name;
      if (name !== 'NACK' && name !== 'FIR') return;
      var ssrc = parsed.mediaSsrc >>> 0;
      for (var i = 0; i < state.consumers.length; i++) {
        var c = state.consumers[i];
        if (c.closed || c.peer !== peer || c.ssrc !== ssrc) continue;
        try {
          ev.emit('consumerfeedback', c.handle, {
            type: name.toLowerCase(),
            ssrc: ssrc,
            count: name === 'NACK' && parsed.lostSequenceNumbers
                   ? parsed.lostSequenceNumbers.length : 1,
          });
        } catch (e) { _diag('feedback listener threw: ' + (e && e.message)); }
      }
    };
    // A peer that attaches while tracing is already on inherits it.
    if (state.feedbackWatched) {
      try { peer.manager.ev.on('rtcp', peer._rtcp); peer._rtcpWatched = true; } catch (e) {}
    }
  }

  function refresh_unbound_flags() {
    for (var i = 0; i < state.peers.length; i++) state.peers[i]._has_unbound = false;
    for (var j = 0; j < state.consumers.length; j++) {
      var c = state.consumers[j];
      if (!c.closed && !c.bound) c.producer.peer._has_unbound = true;
    }
  }

  /* ── Records ─────────────────────────────────────────────────── */

  function add_producer(peer, track, transceiver) {
    var p = {
      id: 'producer-' + (++_ids.producer),
      peer: peer, kind: track.kind, track: track,
      transceiver: transceiver || null,
      mid: transceiver ? transceiver.mid : null,
      ssrc: null, closed: false, handle: null,
    };
    p.handle = {
      id: p.id, kind: p.kind, peerId: peer.id,
      requestKeyframe: function () { request_keyframe(p); },
      close: function () { close_producer(p); },
      // Turned on by the compat layer when an app subscribes; the tap
      // does no CVO work for producers nobody is watching.
      watchVideoOrientation: function (on) {
        if (!peer._cvo_watch) peer._cvo_watch = {};
        if (on) {
          if (p.ssrc != null) peer._cvo_watch[p.ssrc] = p;
          p._cvo_wanted = true;
        } else if (p.ssrc != null) {
          delete peer._cvo_watch[p.ssrc];
          p._cvo_wanted = false;
        }
      },
    };
    state.producers.push(p);
    if (track && typeof track.addEventListener === 'function') {
      track.addEventListener('ended', function () { close_producer(p); });
    }
    emit_safely('producer', p.handle, peer.handle);
    return p;
  }

  /* ── PLAIN PRODUCER (PlainTransport ingress) ──────────────────
   * A producer whose media arrives as raw RTP on a UDP socket — ffmpeg
   * pushing a file into the room, an IP camera, an external broadcast.
   * It has no track and no transceiver, so the two things the engine
   * normally reads from a peer connection have to come from elsewhere:
   *   ssrc  — the caller declares it (it is in the SDP handed to ffmpeg)
   *   media — pushed in by the transport instead of tapped off a pc
   * Everything downstream is unchanged: it joins state.producers, the
   * same cascades bind consumers to it, and the same routing table
   * forwards it. Only its SOURCE differs.
   */
  this.addPlainProducer = function (opts) {
    opts = opts || {};
    if (state.closed) throw new Error('Router closed');
    var peer = null;
    for (var i = 0; i < state.peers.length; i++) {
      if (state.peers[i].handle === opts.peerHandle) { peer = state.peers[i]; break; }
    }
    if (!peer) throw new Error('addPlainProducer: unknown peer');
    if (opts.ssrc == null) throw new TypeError('addPlainProducer: ssrc is required');

    var p = {
      id: 'producer-' + (++_ids.producer),
      peer: peer, kind: opts.kind || 'video', track: null,
      transceiver: null, mid: null, plain: true,
      codecName: opts.codecName || null,
      ssrc: opts.ssrc >>> 0, closed: false, handle: null,
    };
    p.handle = {
      id: p.id, kind: p.kind, peerId: peer.id,
      requestKeyframe: function () { request_keyframe(p); },
      close: function () { close_producer(p); },
      // The transport calls this for every packet it receives.
      push: function (rtp, info) {
        if (p.closed || state.closed) return;
        // Through the peer's own emitter, so the packet takes exactly
        // the path a browser peer's packet takes.
        peer.manager.ev.emit('rtp', rtp, null, info || { ssrc: p.ssrc });
      },
    };
    state.producers.push(p);
    _diag(p.id + ' plain producer: ssrc=' + p.ssrc + ' kind=' + p.kind);
    emit_safely('producer', p.handle, peer.handle);
    run_cascades();
    return p.handle;
  };

  function close_producer(p) {
    if (p.closed) return;
    p.closed = true;
    for (var i = state.consumers.length - 1; i >= 0; i--) {
      if (state.consumers[i].producer === p) close_consumer(state.consumers[i]);
    }
    var idx = state.producers.indexOf(p);
    if (idx >= 0) state.producers.splice(idx, 1);
    emit_safely('producerclose', p.handle);
  }

  function close_consumer(c) {
    if (c.closed) return;
    c.closed = true;
    var by_ssrc = state.routes[c.producer.peer.id];
    var key = (c.src_ssrc != null) ? c.src_ssrc : c.producer.ssrc;
    if (by_ssrc && key != null && by_ssrc[key]) {
      var list = by_ssrc[key];
      var i = list.indexOf(c);
      if (i >= 0) list.splice(i, 1);
      if (!list.length) delete by_ssrc[key];
    }
    var idx = state.consumers.indexOf(c);
    if (idx >= 0) state.consumers.splice(idx, 1);
    refresh_unbound_flags();
    emit_safely('consumerclose', c.handle);
  }

  /* ── Public: addPlainPeer ─────────────────────────────────────
   * A PlainTransport is a peer with no RTCPeerConnection at all: no
   * ICE, no DTLS, no negotiation — just an address to send RTP to
   * (ffmpeg, GStreamer, a recorder). It reaches the SAME routing table
   * and the SAME cascades as a browser peer; what differs is that its
   * outgoing ssrc/pt are chosen by the caller instead of discovered
   * from a transceiver, so cascades 0-2 have nothing to do and 4 can
   * bind immediately. Keeping it inside the cascade engine (rather than
   * bolting on a private forward path) is what makes producer churn,
   * layer switching and keyframe debt work for it for free.
   */
  // Turn RTCP feedback tracing on. Off by default: 'rtcp' fires for
  // every receiver report on every connection, and a listener that
  // formats those for an app that never subscribed is pure cost.
  this.watchFeedback = function (on) {
    for (var i = 0; i < state.peers.length; i++) {
      var pr = state.peers[i];
      if (!pr._rtcp || pr.closed) continue;
      try {
        if (on && !pr._rtcpWatched) { pr.manager.ev.on('rtcp', pr._rtcp); pr._rtcpWatched = true; }
        else if (!on && pr._rtcpWatched) { pr.manager.ev.off('rtcp', pr._rtcp); pr._rtcpWatched = false; }
      } catch (e) {}
    }
    state.feedbackWatched = !!on;
  };

  this.addPlainPeer = function (opts) {
    opts = opts || {};
    if (state.closed) throw new Error('Router closed');
    if (typeof opts.sendRtp !== 'function') {
      throw new TypeError('addPlainPeer: sendRtp(buffer) is required');
    }
    var peer = {
      id: 'peer-' + (++_ids.peer),
      plain: true, attached: true, closed: false,
      pc: null, wrapper: null, _listeners: {},
      manager: {
        sendRtp: function (buf) { try { opts.sendRtp(buf); } catch (e) {} },
        requestKeyframe: function () {},          // no upstream from a sink
        registerOutboundStream: function () {},
        ev: new EventEmitter(),
      },
      _src_stats: {},
    };
    peer.handle = {
      id: peer.id,
      // What this peer is currently sending. Room reads it to pair a
      // late joiner with everything already flowing — it was reading
      // `undefined` and looping zero times, so a participant who joined
      // after a stream started never received it. The failure was
      // silent: no error, just a room where only the first arrival is
      // ever seen.
      get producers() {
        var out = [];
        for (var i = 0; i < state.producers.length; i++) {
          var pr = state.producers[i];
          if (!pr.closed && pr.peer === peer) out.push(pr.handle);
        }
        return out;
      },
      close: function () { close_peer(peer); },
      consume: function (producer_handle, cb2, plainOpts) {
        return consume(peer, producer_handle, cb2, plainOpts);
      },
    };
    state.peers.push(peer);
    attach_peer_tap(peer);
    _diag(peer.id + ' addPlainPeer: plain RTP peer attached');
    // A plain peer is a peer. Room listens for 'peer' to pair newcomers
    // with what is already flowing, so a plain peer that never announced
    // itself was invisible to the policy layer: it could produce, and
    // nothing was ever routed TO it. Announce it like any other.
    emit_safely('peer', peer.handle);
    return peer.handle;
  };

  /* ── Public: addPeer / consume ───────────────────────────────── */

  this.addPeer = function (peer_or_pc) {
    if (state.closed) throw new Error('Router closed');

    var pc = unwrap_pc(peer_or_pc);
    var is_stable_wrapper = !!(peer_or_pc && peer_or_pc.connection &&
                               typeof peer_or_pc.on === 'function');

    // stable-webrtc creates its RTCPeerConnection ASYNCHRONOUSLY — the
    // engine acquires the shared DTLS certificate first and only its
    // callback runs `new RTCPeerConnection(...)`. A Peer handed to us
    // right after construction therefore has connection.pc === null.
    // House pattern applies to ourselves too: attaching is a reactive
    // condition, not a constructor assumption — accept the wrapper now,
    // attach when the pc exists (its own 'statechange' events are the
    // trigger; the first one fires long before any media).
    if (!pc && !is_stable_wrapper) {
      throw new Error('addPeer: expected a stable-webrtc Peer or an RTCPeerConnection');
    }

    var peer = {
      id: 'peer-' + (++_ids.peer),
      pc: pc || null, manager: pc ? pc._manager : null,
      wrapper: (peer_or_pc !== pc) ? peer_or_pc : null,
      closed: false, attached: false,
      _listeners: {}, _tap: null, _pli: null,
      _has_unbound: false, handle: null,
    };
    if (pc && !peer.manager) {
      throw new Error('addPeer: pc has no _manager — not a webrtc-server RTCPeerConnection?');
    }

    peer.handle = {
      id: peer.id,
      consume: function (producer_handle, cb) {
        consume(peer, producer_handle, cb);
      },
      close: function () { close_peer(peer); },
      get producers() {
        var out = [];
        for (var i = 0; i < state.producers.length; i++) {
          if (state.producers[i].peer === peer && !state.producers[i].closed) {
            out.push(state.producers[i].handle);
          }
        }
        return out;
      },
    };

    if (peer.pc) {
      _diag(peer.id + ' addPeer: pc already exists — attaching immediately');
      attach_peer(peer);
    } else {
      _diag(peer.id + ' addPeer: pc not born yet — reactive bootstrap armed');
      // Reactive attach: any engine state change re-checks the condition.
      peer._listeners.bootstrap = function () {
        if (peer.closed || peer.attached) return;
        var got = unwrap_pc(peer.wrapper);
        if (!got) return;                      // pc not born yet — next event
        if (!got._manager) return;             // defensive: wait for a full pc
        peer.pc = got;
        peer.manager = got._manager;
        _diag(peer.id + ' bootstrap: pc born — attaching');
        try { peer.wrapper.off('statechange', peer._listeners.bootstrap); } catch (e) {}
        attach_peer(peer);
        run_cascades();
      };
      peer.wrapper.on('statechange', peer._listeners.bootstrap);
      // The engine emits statechange on every transition, the first of
      // which (negotiation start) happens right after pc creation — no
      // timer needed. If the wrapper closes before a pc ever exists,
      // close_peer cleans the listener.
    }

    state.peers.push(peer);
    emit_safely('peer', peer.handle);
    return peer.handle;
  };

  // Everything that requires a live pc — split out so it can run at
  // construction (raw pc) or reactively (stable wrapper, post-cert).
  function attach_peer(peer) {
    if (peer.attached || peer.closed) return;
    peer.attached = true;
    var pc = peer.pc;

    // addEventListener — never the on* properties (stable-webrtc owns those).
    peer._listeners.track = function (event) {
      _diag(peer.id + ' trigger: track event (kind=' + (event && event.track && event.track.kind) + ')');
      if (peer.closed || !event || !event.track) return;
      add_producer(peer, event.track, event.transceiver);
      run_cascades();
    };
    pc.addEventListener('track', peer._listeners.track);

    // Negotiation progress is what assigns sender SSRCs and lands the
    // remote answer — the natural cascade trigger.
    peer._listeners.signaling = function () {
      _diag(peer.id + ' trigger: signalingstatechange state=' + pc.signalingState +
            ' RD=' + !!pc.remoteDescription);
      run_cascades();
    };
    pc.addEventListener('signalingstatechange', peer._listeners.signaling);

    peer._listeners.connstate = function () {
      var s = pc.connectionState;
      if (s === 'closed' || s === 'failed') close_peer(peer);
    };
    pc.addEventListener('connectionstatechange', peer._listeners.connstate);

    // Ordering-proof heartbeat: stable-webrtc emits a snapshot on EVERY
    // engine transition (negotiation/ice/dc). Conditions-not-history means
    // extra trigger sources are free — if any single event fired before
    // the state it announces was fully committed (e.g. signalingstatechange
    // before remoteDescription lands), the next snapshot re-evaluates and
    // the parked cascade proceeds. Guarded no-op when nothing is pending.
    if (peer.wrapper && typeof peer.wrapper.on === 'function') {
      peer._listeners.wrapperbeat = function () {
        if (peer.closed) return;
        run_cascades();
      };
      peer.wrapper.on('statechange', peer._listeners.wrapperbeat);
    }

    attach_peer_tap(peer);
  }

  function consume(peer, producer_handle, cb, plainOpts) {
    if (state.closed) return cb(new Error('Router closed'));
    if (peer.closed) return cb(new Error('peer closed'));
    var p = null;
    for (var i = 0; i < state.producers.length; i++) {
      if (state.producers[i].handle === producer_handle || state.producers[i].id === producer_handle) {
        p = state.producers[i]; break;
      }
    }
    if (!p || p.closed) return cb(new Error('unknown or closed producer'));
    if (p.peer === peer) return cb(new Error('cannot consume own producer'));

    // NOTE: no special handling for a peer whose pc hasn't been born yet
    // (stable-webrtc creates it async, post-cert). The record simply
    // starts with transceiver:null and cascade 0 creates it once the
    // peer attaches — the parked consume is STATE, visible to the same
    // cascades and the same timeout as everything else, not a private
    // side-loop. (A private setTimeout retry lived here briefly; it was
    // a second, parallel state machine — exactly the drift the reactive
    // doctrine exists to prevent.)
    var c = {
      id: 'consumer-' + (++_ids.consumer),
      peer: peer, producer: p, transceiver: null,
      // A plain peer's outgoing identifiers come from the caller (they
      // are what the app puts in the SDP it hands ffmpeg), so they are
      // known before any cascade runs.
      ssrc: (plainOpts && plainOpts.ssrc != null) ? (plainOpts.ssrc >>> 0) : null,
      pt:   (plainOpts && plainOpts.pt   != null) ? plainOpts.pt : null,
      fwd: null, bound: false, paused: false,
      src_ssrc: null,          // the LAYER this consumer forwards (M3a)
      last_switch_at: 0,       // switch-rate floor (see LAYER_SWITCH_MIN_MS)
      preferred_spatial: null, // ceiling set by the app (M3b); null = best available
      preferred_temporal: null,// TID ceiling (M3c); null = all temporal layers
      current_temporal: null,  // highest TID actually forwarded, for reporting
      up_since: 0, up_target: null,
      last_pli_at: 0, created_at: Date.now(), cb: cb, closed: false, handle: null,
    };
    c.handle = {
      id: c.id, producerId: p.id, peerId: peer.id,
      pause: function () { c.paused = true; },
      resume: function () {
        if (c.closed || !c.bound) return;
        c.paused = false;
        c.fwd.switchSource(0);           // re-base the timeline it slept through
        c.last_pli_at = Date.now();
        if (p.kind === 'video') c.kf_pending = true;   // the debt returns on every resume
        request_keyframe(p, c.src_ssrc); // fresh decode point, from OUR layer
      },
      getStats: function () {
        var s = c.fwd ? c.fwd.getState() : { forwarded: 0 };
        s.bound = c.bound; s.paused = c.paused; s.producerId = p.id;
        s.layerSsrc = c.src_ssrc;
        s.spatialLayer = spatial_index_of(c);
        return s;
      },
      // M3b hook, already honoured by cascade 3c: a ceiling, not a pin —
      // if the requested layer is not carrying, 3c still rides one that is.
      setPreferredSpatialLayer: function (idx) {
        c.preferred_spatial = (idx == null) ? null : Math.max(0, idx | 0);
        run_cascades();
      },
      get currentSpatialLayer() { return spatial_index_of(c); },
      // M3c. Unlike the spatial ceiling this needs no cascade pass and no
      // keyframe: temporal selection is a per-packet decision the very
      // next packet already honours, and every TID at or below the new
      // ceiling was already being decoded.
      setPreferredTemporalLayer: function (idx) {
        var next = (idx == null) ? null : Math.max(0, idx | 0);
        if (next === c.preferred_temporal) return;
        c.preferred_temporal = next;
        _diag(c.id + ' preferred temporal layer → ' + (next == null ? 'all' : next));
        try { ev.emit('consumerlayerschange', c.handle, { ssrc: c.src_ssrc, temporal: next }); }
        catch (e) { _diag('consumerlayerschange listener threw: ' + (e && e.message)); }
      },
      get currentTemporalLayer() {
        // What is actually going out: the ceiling when one is set,
        // otherwise the highest TID we have seen on this route.
        if (c.preferred_temporal != null) return c.preferred_temporal;
        return c.current_temporal;
      },
      close: function () { close_consumer(c); },
    };
    state.consumers.push(c);
    refresh_unbound_flags();
    _diag(c.id + ' consume: record created (' + p.id + ' → ' + peer.id + '), waiting on cascades');

    // The timeout is a cascade CONDITION (5); this timer only guarantees
    // the cascades get a look at it even if no event fires again.
    var t = setTimeout(function () { run_cascades(); }, CONSUME_TIMEOUT_MS + 100);
    if (t.unref) t.unref();

    run_cascades();
  }

  function close_peer(peer) {
    if (peer.closed) return;
    peer.closed = true;
    if (peer.wrapper && peer._listeners.bootstrap) {
      try { peer.wrapper.off('statechange', peer._listeners.bootstrap); } catch (e) {}
    }
    if (peer.wrapper && peer._listeners.wrapperbeat) {
      try { peer.wrapper.off('statechange', peer._listeners.wrapperbeat); } catch (e) {}
    }
    if (peer.attached) {
      try { peer.manager.ev.off('rtp', peer._tap); } catch (e) {}
      try { peer.manager.ev.off('pli', peer._pli); } catch (e) {}
      // 'rtcp' is attached only when feedback tracing is on, so it is
      // easy to forget here — and forgetting it leaves a listener on a
      // transport emitter that outlives the peer. Detach unconditionally;
      // removing a listener that was never added is a no-op.
      try { peer.manager.ev.off('rtcp', peer._rtcp); } catch (e) {}
      try { peer.pc.removeEventListener('track', peer._listeners.track); } catch (e) {}
      try { peer.pc.removeEventListener('signalingstatechange', peer._listeners.signaling); } catch (e) {}
      try { peer.pc.removeEventListener('connectionstatechange', peer._listeners.connstate); } catch (e) {}
    }

    for (var i = state.consumers.length - 1; i >= 0; i--) {
      if (state.consumers[i].peer === peer) close_consumer(state.consumers[i]);
    }
    for (var j = state.producers.length - 1; j >= 0; j--) {
      if (state.producers[j].peer === peer) close_producer(state.producers[j]);
    }
    delete state.routes[peer.id];
    var idx = state.peers.indexOf(peer);
    if (idx >= 0) state.peers.splice(idx, 1);
    ev.emit('peerclose', peer.handle);
  }

  this.close = function () {
    if (state.closed) return;
    state.closed = true;

    // Answer every consume that is still waiting. A parked consume holds
    // an application callback, and the application is entitled to be
    // told the answer is no — dropping it leaves whatever awaited it
    // pending forever, which on a server is a request that never
    // completes rather than one that fails.
    for (var ci = 0; ci < state.consumers.length; ci++) {
      var pending = state.consumers[ci];
      if (pending && pending.cb) {
        var pcb = pending.cb; pending.cb = null;
        try { pcb(new Error('Router closed')); } catch (e) {}
      }
    }
    var peers = state.peers.slice();
    for (var i = 0; i < peers.length; i++) { try { close_peer(peers[i]); } catch (e) {} }
    ev.emit('close');
  };

  // Every other object in this library reports its own lifecycle; the
  // Router did not, so an application holding one had no way to ask
  // whether it was still usable except by calling something and
  // catching.
  Object.defineProperty(this, 'closed', { get: function () { return state.closed; } });
  Object.defineProperty(this, 'peers', {
    get: function () {
      var out = [];
      for (var i = 0; i < state.peers.length; i++) out.push(state.peers[i].handle);
      return out;
    },
  });

  // EventEmitter convenience forwarding — the connection_manager pattern.
  this.on = function (name, fn) { ev.on(name, fn); return this; };
  this.off = function (name, fn) { ev.off(name, fn); return this; };
  this.once = function (name, fn) { ev.once(name, fn); return this; };
}


/* ═══════════════════════════ Room ═══════════════════════════ */
//
// The convenience layer: a Router with the policy every mediasoup app
// hand-builds anyway — "everybody sees everybody", including late
// joiners. Anything fancier (breakout, selective routing, recording
// taps) uses Router directly.

function Room(options) {
  var self = this;
  var router = new Router(options);
  var ev = new EventEmitter();
  var emit_safely = make_safe_emit(ev, _diag);
  var closed = false;

  function pair(peer_handle, producer_handle) {
    peer_handle.consume(producer_handle, function (err, consumer_handle) {
      if (err) { ev.emit('error', err, { peer: peer_handle.id, producer: producer_handle.id }); return; }
      emit_safely('consumer', consumer_handle);
    });
  }

  router.on('producer', function (producer_handle, owner_handle) {
    var peers = router.peers;
    for (var i = 0; i < peers.length; i++) {
      if (peers[i].id !== owner_handle.id) pair(peers[i], producer_handle);
    }
    emit_safely('producer', producer_handle);
  });

  router.on('peer', function (peer_handle) {
    // late joiner consumes everything already flowing
    var peers = router.peers;
    for (var i = 0; i < peers.length; i++) {
      if (peers[i].id === peer_handle.id) continue;
      var prods = peers[i].producers;
      for (var j = 0; j < prods.length; j++) pair(peer_handle, prods[j]);
    }
  });

  this.join = function (peer_or_pc) {
    if (closed) throw new Error('Room closed');
    return router.addPeer(peer_or_pc);
  };
  this.close = function () {
    if (closed) return;
    closed = true;
    router.close();
    ev.emit('close');
  };
  Object.defineProperty(this, 'participants', { get: function () { return router.peers; } });
  Object.defineProperty(this, 'router', { get: function () { return router; } });
  this.on = function (name, fn) { ev.on(name, fn); return this; };
  this.off = function (name, fn) { ev.off(name, fn); return this; };
  this.once = function (name, fn) { ev.once(name, fn); return this; };
}


export { Router, Room, unwrap_pc, is_keyframe, packet_tid };