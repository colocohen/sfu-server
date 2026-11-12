/**
 * mediasoup_compat.js — a drop-in mediasoup v3 SERVER API over sfu-server.
 *
 * PURPOSE
 * -------
 * An existing mediasoup application should run here by changing its
 * import and nothing else. Its browser keeps using an unmodified
 * mediasoup-client — that half is already proven live (M4 spike:
 * device.load → createSendTransport → connect → produce → consume →
 * rendered video, with a mediasoup transport sitting in the native Room
 * as a first-class peer).
 *
 * This file absorbs the former src/mediasoup_transport.js in full,
 * comments included. Everything that file learned the hard way is still
 * here, marked; nothing was re-derived.
 *
 * HOUSE PATTERNS, AND THE ONE DECLARED EXCEPTION
 * ----------------------------------------------
 * Constructor functions + closure state (no ES6 classes), plain records
 * in one `state` object, snake_case internals / camelCase public surface,
 * EventEmitter throughout — all unchanged.
 *
 * EXCEPTION, deliberate and recorded: this layer is Promise-based,
 * because mediasoup is `await` from end to end and a drop-in replacement
 * that returns callbacks is not a drop-in replacement. The ENGINE stays
 * callbacks; only this wrapper awaits. That is exactly the line ROADMAP
 * decision 1 already draws — the core never depends on a wrapper, and a
 * wrapper is free to speak its consumers' dialect.
 *
 * WHY THIS BUILDS ON Router AND NEVER ON Room
 * -------------------------------------------
 * mediasoup is APP-driven: the application decides who consumes what,
 * one producer at a time. Room is POLICY-driven: it decides "everybody
 * sees everybody" and adds transceivers by itself. Those two fight over
 * the same transceivers. Router's peer.consume(producer, cb) is already
 * the exact per-producer primitive transport.consume() needs, so the
 * compat layer talks to Router directly and Room stays a native-path
 * convenience.
 *
 * THE TWO MID NAMESPACES (the silent-black-video lesson, M4c)
 * ----------------------------------------------------------
 * mediasoup-client numbers its m-lines with ITS OWN ordinals and Chrome
 * demuxes by the MID header extension FIRST. Our internal transceiver
 * mids therefore must never reach the wire. Enforcement is structural:
 * we wrap manager.registerOutboundStream so ANY registration for a
 * deliverable ssrc, from any code path at any time, gets the client
 * ordinal — an invariant by construction rather than by ordering luck.
 */

import { EventEmitter } from 'node:events';
import crypto from 'node:crypto';
import dgram from 'node:dgram';
import os from 'node:os';

import { RTCPeerConnection, RTCSessionDescription, WebRTCRouter } from 'webrtc-server';

import { Router as SfuRouter, is_keyframe } from './sfu.js';
import { readVideoOrientation, remapExtensionIds } from 'rtp-packet';
import { getRouterRtpCapabilities } from './rtp_capabilities.js';
import {
  canConsume as ortc_can_consume,
  getConsumerRtpParameters,
  validateProducerRtpParameters,
  filterCapabilitiesByMediaCodecs,
} from './ortc.js';

var _DIAG = !!(typeof process !== 'undefined' && process.env &&
               (process.env.SFU_DEBUG || process.env.WEBRTC_DEBUG));
function _diag(msg) {
  if (_DIAG && typeof console !== 'undefined') console.log('[ms-compat] ' + msg);
}

function uuid() { return crypto.randomUUID ? crypto.randomUUID() : crypto.randomBytes(16).toString('hex'); }

/* ── DEGRADATION POLICY (drop-in discipline) ──────────────────────
 * A drop-in replacement is judged by whether UNMODIFIED application
 * code runs, and real mediasoup apps call more of the API than they
 * strictly need. The official demo, for example, calls
 * createAudioLevelObserver() while building a room and
 * setMaxIncomingBitrate() on every transport — a rejecting stub there
 * doesn't degrade the app, it prevents the room from existing.
 *
 * So unimplemented surface is split by CONSEQUENCE, not by effort:
 *
 *   POLICY / QUALITY (bitrate caps, preferred layers, priority)
 *       → resolve as a no-op, warn once. Media still flows correctly;
 *         only tuning is absent.
 *   INFORMATIONAL (observers, trace events)
 *       → return a real object that simply never emits. The app gets
 *         the shape it expected and its listeners stay harmless.
 *   DATA DELIVERY / CORRECTNESS (produceData, consumeData,
 *   pipeToRouter, plain/pipe transports)
 *       → reject loudly. Silent success here would have the app
 *         believe messages are being delivered when nothing is.
 */
var _warned = {};
// mediasoup's dump() returns the worker's internal view of an entity. We
// have no worker process, so ours returns the same shape from the state
// we actually hold — enough for the introspection apps use it for
// (health endpoints, debugging, tests), without inventing fields we
// cannot know.
// TRACE EVENTS. mediasoup's enableTraceEvent(types) turns on a firehose
// of per-event telemetry (rtp/keyframe/nack/pli/fir/bwe/probation…),
// delivered as 'trace' on the entity and its observer. Ours was a stub
// that resolved and emitted nothing — an app that builds its monitoring
// on trace saw silence and had no way to tell that from "no traffic".
// So: a small recorder each entity owns. Only the types the app asked
// for are emitted, which keeps the hot path honest — an entity nobody
// traced does one boolean test per event.
function TraceHub(emitEntity, emitObserver) {
  var enabled = {};
  return {
    enable: function (types) {
      enabled = {};
      (types || []).forEach(function (t) { enabled[t] = true; });
      return Promise.resolve();
    },
    enabledTypes: function () { return Object.keys(enabled); },
    on: function (type) { return !!enabled[type]; },
    fire: function (type, direction, info) {
      if (!enabled[type]) return;
      var ev = {
        type: type, timestamp: Date.now(),
        direction: direction || 'out', info: info || {},
      };
      try { emitEntity(ev); } catch (e) {}
      try { emitObserver(ev); } catch (e) {}
    },
  };
}

// The engine hands us the raw extension byte; rtp-packet's reader takes
// the extension payload, so wrap it once rather than at each call site.
var _cvoScratch = Buffer.alloc(1);
function _cvoByte(b) { _cvoScratch[0] = b & 0xff; return _cvoScratch; }

// mediasoup's default SCTP send buffer (WebRtcTransportOptions
// .sctpSendBufferSize). Used as the pressure threshold when the app
// does not set one.
var SCTP_SEND_BUFFER_LIMIT = 262144;

function _dumpOf(obj, extra) {
  var out = { id: obj.id };
  if (extra) for (var k in extra) out[k] = extra[k];
  return Promise.resolve(out);
}

function warn_once(key, msg) {
  if (_warned[key]) return;
  _warned[key] = true;
  if (typeof console !== 'undefined' && console.warn) {
    console.warn('[sfu-server/mediasoup-compat] ' + msg);
  }
}

/* ── RFC 6464 level reading ─────────────────────────────────────────
 * webrtc-server caches per-SSRC audio levels from the ssrc-audio-level
 * header extension, readable via the W3C surface:
 * receiver.getSynchronizationSources() → [{source, audioLevel, …}].
 * W3C audioLevel is linear 0..1; mediasoup speaks dBov (-127..0).
 */
function read_producer_volume(producer) {
  try {
    var pc = producer._state.transport.pc;
    if (!pc) return null;
    var want = {};
    (producer.rtpParameters.encodings || []).forEach(function (e) {
      if (e.ssrc != null) want[e.ssrc >>> 0] = true;
    });
    var receivers = pc.getReceivers ? pc.getReceivers() : [];
    var fallback = null, matched = null, audioSources = 0;
    for (var i = 0; i < receivers.length; i++) {
      var r = receivers[i];
      if (r.track && r.track.kind !== 'audio') continue;
      var srcs = (r.getSynchronizationSources && r.getSynchronizationSources()) || [];
      for (var j = 0; j < srcs.length; j++) {
        var sc = srcs[j];
        if (sc.audioLevel == null) continue;
        audioSources++;
        if (want[sc.source >>> 0]) matched = sc.audioLevel;
        else fallback = sc.audioLevel;
      }
    }
    // Exact ssrc match wins; with exactly one audio source on the pc the
    // ambiguity is zero and we accept it even under SSRC drift.
    var lin = matched != null ? matched : (audioSources === 1 ? fallback : null);
    if (lin == null) return null;
    if (lin <= 0) return -127;
    return Math.max(-127, Math.min(0, Math.round(20 * Math.log10(lin))));
  } catch (e) { return null; }
}

/**
 * The real RtpObserver — AudioLevelObserver ('audiolevel') and
 * ActiveSpeakerObserver ('activespeaker') share the level-reading core;
 * they differ only in what they emit per tick.
 */
function RtpObserver(type, options, router) {
  var self = this, ev = new EventEmitter(), observer = new EventEmitter();
  options = options || {};
  var st = {
    id: uuid(), closed: false, paused: false,
    producers: {},                          // id → Producer
    threshold:  options.threshold != null ? options.threshold : -80,
    maxEntries: options.maxEntries || 1,
    interval:   options.interval || (type === 'activespeaker' ? 300 : 800),
    hadSound:   false,
    ema:        {},                         // id → smoothed dBov (activespeaker)
    dominant:   null,
    appData:    options.appData || {},
    timer:      null,
  };

  Object.defineProperty(this, 'id',       { get: function () { return st.id; } });
  Object.defineProperty(this, 'type',     { get: function () { return type; } });
  Object.defineProperty(this, 'closed',   { get: function () { return st.closed; } });
  Object.defineProperty(this, 'paused',   { get: function () { return st.paused; } });
  Object.defineProperty(this, 'observer', { get: function () { return observer; } });
  Object.defineProperty(this, 'appData', {
    get: function () { return st.appData; }, set: function (v) { st.appData = v; } });

  function tick() {
    if (st.closed || st.paused) return;
    var readings = [];
    for (var id in st.producers) {
      var p = st.producers[id];
      if (p.closed) { delete st.producers[id]; delete st.ema[id]; continue; }
      if (p.paused || p.kind !== 'audio') continue;
      var vol = read_producer_volume(p);
      if (vol != null) readings.push({ producer: p, volume: vol });
    }
    if (type === 'audiolevel') {
      var loud = readings.filter(function (r) { return r.volume >= st.threshold; })
                         .sort(function (a, b) { return b.volume - a.volume; })
                         .slice(0, st.maxEntries);
      if (loud.length) {
        st.hadSound = true;
        ev.emit('volumes', loud);
        observer.emit('volumes', loud);
      } else if (st.hadSound) {
        st.hadSound = false;
        ev.emit('silence');
        observer.emit('silence');
      }
    } else {  // activespeaker: EMA per producer, dominant on change
      var best = null;
      readings.forEach(function (r) {
        var id2 = r.producer.id;
        var prev = st.ema[id2] != null ? st.ema[id2] : -127;
        var ema = prev * 0.7 + r.volume * 0.3;
        st.ema[id2] = ema;
        if (ema >= st.threshold && (!best || ema > best.ema)) best = { producer: r.producer, ema: ema };
      });
      if (best && (!st.dominant || st.dominant !== best.producer.id)) {
        st.dominant = best.producer.id;
        ev.emit('dominantspeaker', { producer: best.producer });
        observer.emit('dominantspeaker', { producer: best.producer });
      }
    }
  }
  st.timer = setInterval(tick, st.interval);
  if (st.timer.unref) st.timer.unref();

  this.addProducer = function (opts) {
    var p = null;
    if (opts && opts.producerId) p = router._producerById(opts.producerId);
    else if (opts && opts.producer) p = opts.producer;
    if (!p) return Promise.reject(NotFoundError('addProducer: producer not found'));
    st.producers[p.id] = p;
    observer.emit('addproducer', p);
    return Promise.resolve();
  };
  this.removeProducer = function (opts) {
    var id = opts && (opts.producerId || (opts.producer && opts.producer.id));
    if (id && st.producers[id]) {
      var gone = st.producers[id];
      delete st.producers[id]; delete st.ema[id];
      observer.emit('removeproducer', gone);   // mirrors addproducer
    }
    return Promise.resolve();
  };
  this.pause  = function () { st.paused = true;  observer.emit('pause');  return Promise.resolve(); };
  this.resume = function () { st.paused = false; observer.emit('resume'); return Promise.resolve(); };
  this.close  = function () {
    if (st.closed) return;
    st.closed = true;
    clearInterval(st.timer);
    observer.emit('close');
  };
  this.on   = function (n, f) { ev.on(n, f);   return self; };
  this.off  = function (n, f) { ev.off(n, f);  return self; };
  this.once = function (n, f) { ev.once(n, f); return self; };
}

/** An RtpObserver-shaped object that is inert: correct shape, no events. */

/* ── mediasoup error classes (apps catch these by name) ───────────── */

// Real prototype chains (constructor functions, no ES6 classes — house
// rule): apps catch these both by e.name AND by instanceof, and a plain
// Error with a renamed .name fails the second. Callable with or without
// `new`, since our own call sites use the bare form.
function make_error(name) {
  function E(message) {
    if (!(this instanceof E)) return new E(message);
    var e = Error.call(this, message);
    this.message = message;
    this.name = name;
    if (Error.captureStackTrace) Error.captureStackTrace(this, E);
    else this.stack = e.stack;
  }
  E.prototype = Object.create(Error.prototype);
  E.prototype.constructor = E;
  return E;
}
export var UnsupportedError  = make_error('UnsupportedError');
export var NotFoundError     = make_error('NotFoundError');
export var WorkerClosedError = make_error('WorkerClosedError');
export var InvalidStateError = make_error('InvalidStateError');


/* ═══════════════════════════════════════════════════════════════════
 *  SDP synthesis — the mediasoup↔W3C boundary
 *  (absorbed from mediasoup_transport.js; comments preserved verbatim
 *  where they record a live finding)
 * ═══════════════════════════════════════════════════════════════════ */

/** All routable IPv4 addresses of this host — the candidate ips. */
function host_ipv4_addresses() {
  var out = [];
  var ifs = os.networkInterfaces();
  for (var name in ifs) {
    var list = ifs[name] || [];
    for (var i = 0; i < list.length; i++) {
      var a = list[i];
      if (a.family === 'IPv4' && !a.internal) out.push(a.address);
    }
  }
  return out.length ? out : ['127.0.0.1'];
}

// mediasoup's PortManager: start at a random port of the range and take the
// next one while a port is in use, so a firewall opened for the range holds.
function bind_udp_in_range(range, cb) {
  var size = range.max - range.min + 1;
  var start = Math.floor(Math.random() * size);
  (function attempt(n) {
    if (n === size) {
      return cb(new Error('no more available ports [protocol:udp, portRange:' +
                          range.min + '-' + range.max + ']'));
    }
    var sock = dgram.createSocket('udp4');
    var on_error = function (err) {
      sock.close();
      if (err.code === 'EADDRINUSE') attempt(n + 1);
      else cb(err);
    };
    sock.once('error', on_error);
    sock.bind({ address: '0.0.0.0', port: range.min + (start + n) % size }, function () {
      sock.removeListener('error', on_error);
      cb(null, sock);
    });
  })(0);
}

// A syntactically valid, never-verified fingerprint. Safe by the gate
// invariant: inbound stays held until connect() installs the client's
// REAL fingerprint, so no DTLS byte is ever checked against this.
var PLACEHOLDER_DTLS = {
  role: 'client',
  fingerprints: [{ algorithm: 'sha-256',
    value: 'AA:AA:AA:AA:AA:AA:AA:AA:AA:AA:AA:AA:AA:AA:AA:AA:AA:AA:AA:AA:AA:AA:AA:AA:AA:AA:AA:AA:AA:AA:AA:AA' }],
};

var MEDIASOUP_DTLS_TO_SETUP = {
  // The synthesized answer describes THE REMOTE (the mediasoup client),
  // so its a=setup is the CLIENT's own posture. client role 'client'
  // (they act as DTLS client) → THEIR setup is 'active'.
  //
  // THE CROSSED-KEYS BUG (found live, the hard way): the first version
  // mapped client→'passive' — inverting which side our state believed
  // it was. The DTLS handshake still completed (lemon answers whoever
  // hails it), but SRTP key derivation picks OUR write-key half by the
  // recorded role — so we encrypted with the CLIENT's write key, and
  // Chrome auth-failed 100% of 4,281 delivered packets while every
  // layer below reported healthy. a=setup describes the SPEAKER of the
  // SDP; never map it from our own perspective.
  client: 'active',
  server: 'passive',
  auto:   'active',    // browsers overwhelmingly take the client role
};

function parse_own_offer(sdp) {
  var out = { ufrag: null, pwd: null, fingerprint: null };
  var mU = /a=ice-ufrag:([^\r\n]+)/.exec(sdp);
  var mP = /a=ice-pwd:([^\r\n]+)/.exec(sdp);
  var mF = /a=fingerprint:(\S+) ([^\r\n]+)/.exec(sdp);
  if (mU) out.ufrag = mU[1].trim();
  if (mP) out.pwd = mP[1].trim();
  if (mF) out.fingerprint = { algorithm: mF[1], value: mF[2].trim() };
  return out;
}

function fingerprint_lines(clientDtls) {
  var fps = (clientDtls && clientDtls.fingerprints) || [];
  return fps.map(function (f) {
    return 'a=fingerprint:' + f.algorithm + ' ' + f.value;
  });
}

function build_bootstrap_answer(clientDtls) {
  // Minimal, honest answer to OUR m=application bootstrap offer. The
  // client side never sees this SDP — it configures OUR stack only:
  // the client's fingerprint (so DTLS verifies THEIR cert) and our
  // setup role derived from THEIR declared role.
  var role  = (clientDtls && clientDtls.role) || 'auto';
  var setup = MEDIASOUP_DTLS_TO_SETUP[role] || 'passive';
  return [
    'v=0',
    'o=- ' + Date.now() + ' 2 IN IP4 127.0.0.1',
    's=-', 't=0 0',
    'a=group:BUNDLE 0',
    'a=msid-semantic: WMS',
    'm=application 9 UDP/DTLS/SCTP webrtc-datachannel',
    'c=IN IP4 0.0.0.0',
    'a=mid:0',
    'a=ice-ufrag:msclient',                 // lite agent never uses these —
    'a=ice-pwd:msclientmsclientmsclient',   // structural placeholders
  ].concat(fingerprint_lines(clientDtls))
   .concat(['a=setup:' + setup, 'a=sctp-port:5000', ''])
   .join('\r\n');
}

/**
 * Synthesize the client's ANSWER to OUR renegotiation offer: mirror
 * every m-line by type+mid; application stays as-is, media flips to
 * recvonly (the client only receives). Client's fingerprint + stored
 * role; ice creds structural (lite agent ignores them).
 */
function synth_recv_answer(ourOfferSdp, clientDtls) {
  var role  = (clientDtls && clientDtls.role) || 'client';
  var setup = MEDIASOUP_DTLS_TO_SETUP[role] || 'passive';
  var fpLines = fingerprint_lines(clientDtls);

  var sections = ourOfferSdp.split(/\r?\n(?=m=)/);
  var mids = [], media = [];
  for (var i = 1; i < sections.length; i++) {
    var sec = sections[i];
    var mMid = /a=mid:(\S+)/.exec(sec);
    var mid = mMid ? mMid[1] : String(i - 1);
    mids.push(mid);
    var mline = sec.split(/\r?\n/)[0];   // e.g. m=video 9 UDP/TLS/RTP/SAVPF 96 97
    var isApp = mline.indexOf('m=application') === 0;
    var lines = [mline, 'c=IN IP4 0.0.0.0', 'a=mid:' + mid,
                 'a=ice-ufrag:msclient', 'a=ice-pwd:msclientmsclientmsclient']
      .concat(fpLines)
      .concat(['a=setup:' + setup]);
    if (isApp) {
      lines.push('a=sctp-port:5000');
    } else {
      lines.push('a=recvonly', 'a=rtcp-mux');
      // echo payload maps so our side keeps its negotiated pts
      var rt = sec.match(/a=rtpmap:[^\r\n]+/g) || [];
      var fm = sec.match(/a=fmtp:[^\r\n]+/g) || [];
      var fb = sec.match(/a=rtcp-fb:[^\r\n]+/g) || [];
      var ex = sec.match(/a=extmap:[^\r\n]+/g) || [];
      lines = lines.concat(ex).concat(rt).concat(fb).concat(fm);
    }
    media.push(lines.join('\r\n'));
  }
  return ['v=0', 'o=- ' + Date.now() + ' 2 IN IP4 127.0.0.1', 's=-', 't=0 0',
          'a=group:BUNDLE ' + mids.join(' '), 'a=msid-semantic: WMS']
    .concat(media).join('\r\n') + '\r\n';
}

/**
 * Synthesize the OFFER a producing client would have sent us.
 *
 * The client hands us its FULL media truth (rtpParameters: codecs with
 * the pts it adopted from OUR device caps, its OWN header-extension ids,
 * its chosen ssrcs) and starts sending immediately. We build the matching
 * remote offer — the client as offerer, renegotiating on the connected
 * pc — and let the existing stack stand up the receive pipeline. No wire
 * SDP is ever exchanged.
 *
 * Critical subtlety: the extmap ids here MUST be the CLIENT's ids (from
 * rtpParameters.headerExtensions) — that is what it stamps on the wire,
 * and our parser learns ids from this SDP.
 */
function synth_produce_offer(kind, rp, mediaMid, clientDtls, priorMids) {
  var enc   = (rp.encodings && rp.encodings[0]) || {};
  var cname = (rp.rtcp && rp.rtcp.cname) || 'mediasoup';

  var pts = [], codecLines = [];
  (rp.codecs || []).forEach(function (c) {
    var pt = c.payloadType;
    pts.push(pt);
    var name = String(c.mimeType || '').split('/')[1] || 'VP8';
    codecLines.push('a=rtpmap:' + pt + ' ' + name + '/' + c.clockRate +
      (c.channels && c.channels > 1 ? '/' + c.channels : ''));
    (c.rtcpFeedback || []).forEach(function (f) {
      codecLines.push('a=rtcp-fb:' + pt + ' ' + f.type + (f.parameter ? ' ' + f.parameter : ''));
    });
    var params = c.parameters || {};
    var kv = Object.keys(params).map(function (k) { return k + '=' + params[k]; });
    if (kv.length) codecLines.push('a=fmtp:' + pt + ' ' + kv.join(';'));
  });

  var extLines = (rp.headerExtensions || []).map(function (e) {
    return 'a=extmap:' + e.id + ' ' + e.uri;
  });

  // Stream identification lines. Two dialects (RFC 8853):
  //  - ssrc-based: a=ssrc(-group:FID) per encoding
  //  - rid-based (Chrome's send-simulcast default): a=rid + a=simulcast,
  //    no ssrc lines — the wire carries RID header extensions instead,
  //    which webrtc-server already parses (ROADMAP: 'already receives
  //    simulcast').
  var ssrcLines = [];
  var encs = rp.encodings || [];
  var rids = encs.filter(function (e) { return e.rid != null; })
                 .map(function (e) { return e.rid; });
  if (rids.length) {
    rids.forEach(function (r) { ssrcLines.push('a=rid:' + r + ' send'); });
    ssrcLines.push('a=simulcast:send ' + rids.join(';'));
  } else {
    encs.forEach(function (e) {
      if (e.ssrc == null) return;
      if (e.rtx && e.rtx.ssrc != null) {
        ssrcLines.push('a=ssrc-group:FID ' + e.ssrc + ' ' + e.rtx.ssrc);
      }
      ssrcLines.push('a=ssrc:' + e.ssrc + ' cname:' + cname);
      if (e.rtx && e.rtx.ssrc != null) {
        ssrcLines.push('a=ssrc:' + e.rtx.ssrc + ' cname:' + cname);
      }
    });
  }

  var fpLines = fingerprint_lines(clientDtls);
  var bundle = ['0'].concat(priorMids || []).concat([mediaMid]).join(' ');

  return [
    'v=0',
    'o=- ' + Date.now() + ' 3 IN IP4 127.0.0.1',
    's=-', 't=0 0',
    'a=group:BUNDLE ' + bundle,
    'a=msid-semantic: WMS',
    // m-line 1: the bootstrap application section, restated
    'm=application 9 UDP/DTLS/SCTP webrtc-datachannel',
    'c=IN IP4 0.0.0.0',
    'a=mid:0',
    'a=ice-ufrag:msclient',
    'a=ice-pwd:msclientmsclientmsclient',
  ].concat(fpLines).concat([
    'a=setup:active',
    'a=sctp-port:5000',
    // m-line 2: the client's media, sendonly toward us
    'm=' + kind + ' 9 UDP/TLS/RTP/SAVPF ' + pts.join(' '),
    'c=IN IP4 0.0.0.0',
    'a=mid:' + mediaMid,
    'a=sendonly',
    'a=rtcp-mux',
    'a=ice-ufrag:msclient',
    'a=ice-pwd:msclientmsclientmsclient',
  ]).concat(fpLines).concat([
    'a=setup:active',
  ]).concat(extLines).concat(codecLines).concat(ssrcLines)
   .concat(['']).join('\r\n');
}


/* ═══════════════════════════════════════════════════════════════════
 *  Producer
 * ═══════════════════════════════════════════════════════════════════ */

function Producer(opts) {
  var self = this;
  var ev = new EventEmitter();
  var observer = new EventEmitter();

  var state = {
    id:            opts.id,
    kind:          opts.kind,
    rtpParameters: opts.rtpParameters,
    transport:     opts.transport,
    sfuHandle:     null,      // sfu.js producer handle (arrives async)
    paused:        !!opts.paused,
    closed:        false,
    appData:       opts.appData || {},
  };

  Object.defineProperty(this, 'id',            { get: function () { return state.id; } });
  Object.defineProperty(this, 'kind',          { get: function () { return state.kind; } });
  Object.defineProperty(this, 'rtpParameters', { get: function () { return state.rtpParameters; } });
  Object.defineProperty(this, 'closed',        { get: function () { return state.closed; } });
  Object.defineProperty(this, 'paused',        { get: function () { return state.paused; } });
  Object.defineProperty(this, 'type',          { get: function () {
    // "simple" | "simulcast" | "svc". Until layer selection lands (M3)
    // every producer is forwarded as one continuous stream.
    return (state.rtpParameters.encodings || []).length > 1 ? 'simulcast' : 'simple';
  } });
  Object.defineProperty(this, 'score',         { get: function () { return state.score || []; } });
  // RFC-style Coordination of Video Orientation (urn:3gpp:video-orientation):
  // one byte per packet carrying camera/flip/rotation. mediasoup surfaces
  // only the CHANGES, which is what a UI needs to rotate a <video>.
  // Subscribing is what turns the parsing on. mediasoup can afford to
  // read CVO unconditionally in C++; here the tap is JavaScript on the
  // packet path, so the work exists only while someone wants it.
  observer.on('newListener', function (n) {
    if (n === 'videoorientationchange') armVideoOrientation();
  });

  function armVideoOrientation() {
    if (state.cvoArmed) return;
    state.cvoArmed = true;
    var h = state.sfuHandle;
    if (h && typeof h.watchVideoOrientation === 'function') {
      try { h.watchVideoOrientation(true); } catch (e) {}
    }
  }

  this._videoOrientation = function (byteVal) {
    if (state.closed || state.kind !== 'video') return;
    // The byte's layout is CVO's business, not ours: rtp-packet reads it
    // beside readAudioLevel and readAbsSendTime. What belongs here is the
    // mediasoup contract — fire only on CHANGE, because that is what a UI
    // rotating a <video> needs.
    var o = readVideoOrientation(_cvoByte(byteVal));
    if (!o) return;
    var prev = state.videoOrientation;
    if (prev && prev.camera === o.camera && prev.flip === o.flip &&
        prev.rotation === o.rotation) return;
    state.videoOrientation = o;
    ev.emit('videoorientationchange', o);
    observer.emit('videoorientationchange', o);
  };

  this._setScore = function (sc) { state.score = sc; };
  Object.defineProperty(this, 'observer',      { get: function () { return observer; } });
  Object.defineProperty(this, 'appData', {
    get: function () { return state.appData; },
    set: function (v) { state.appData = v; },
  });

  this._state = state;   // internal: transport/router reach in

  this.close = function () {
    if (state.closed) return;
    state.closed = true;
    if (state.sfuHandle) { try { state.sfuHandle.close(); } catch (e) {} }
    state.transport._onProducerClosed(self);
    observer.emit('close');
  };

  // Called by the transport when the router/transport goes away — same
  // teardown, different event, exactly as mediasoup documents.
  this.transportClosed = function () { return self._transportClosed(); };
  this._transportClosed = function () {
    if (state.closed) return;
    state.closed = true;
    ev.emit('transportclose');
    observer.emit('close');
  };

  // PRODUCER-LEVEL PAUSE — an honest approximation, documented.
  // mediasoup pauses at the router: the producer's RTP simply stops
  // being forwarded anywhere. Our engine's pause lives per consumer
  // (with timeline re-basing on resume), so pausing a producer means
  // pausing every consumer of it. The observable behaviour for every
  // endpoint is identical; only the internal bookkeeping differs.
  this.pause = function () {
    if (state.closed || state.paused) return Promise.resolve();
    state.paused = true;
    state.transport._router._forEachConsumerOf(state.id, function (c) { c._producerPaused(); });
    observer.emit('pause');
    return Promise.resolve();
  };

  this.resume = function () {
    if (state.closed || !state.paused) return Promise.resolve();
    state.paused = false;
    state.transport._router._forEachConsumerOf(state.id, function (c) { c._producerResumed(); });
    observer.emit('resume');
    return Promise.resolve();
  };

  this.getStats = function () {
    // Not every transport is a peer connection: a PlainTransport has no
    // WebRTC stats surface, and asking it for one is how a getStats()
    // call on a perfectly healthy producer used to throw.
    if (typeof state.transport._statsForSsrc !== 'function') {
      return Promise.resolve([{
        type: 'inbound-rtp', timestamp: Date.now(), kind: state.kind,
        ssrc: (state.rtpParameters.encodings || [{}])[0].ssrc,
      }]);
    }
    return state.transport._statsForSsrc('inbound-rtp', state.rtpParameters);
  };
  var trace = TraceHub(function (e) { ev.emit('trace', e); },
                       function (e) { observer.emit('trace', e); });
  this.enableTraceEvent = function (types) {
    var r = trace.enable(types);
    // Some trace types cost work on the RTCP path, so the engine only
    // does that work once an app has asked for them.
    try {
      var wantFeedback = (types || []).some(function (t) {
        return t === 'nack' || t === 'fir';
      });
      if (wantFeedback && state.transport && state.transport._router &&
          state.transport._router._sfuRouter) {
        state.transport._router._sfuRouter.watchFeedback(true);
      }
    } catch (e) {}
    return r;
  };
  this._trace = trace;
  this.dump = function () {
    return _dumpOf(self, {
      kind: state.kind, type: self.type, rtpParameters: state.rtpParameters,
      paused: state.paused, score: state.score || [],
      rtpMapping: {}, rtpStreams: [], traceEventTypes: '',
    });
  };

  // Subscription IS the switch: an event that costs work on the packet
  // path is not produced until someone asks for it.
  this.on = function (n, f) {
    if (n === 'videoorientationchange') armVideoOrientation();
    ev.on(n, f); return self;
  };
  this.once = function (n, f) {
    if (n === 'videoorientationchange') armVideoOrientation();
    ev.once(n, f); return self;
  };
  this.off  = function (n, f) { ev.off(n, f);  return self; };
  this._emit = function () { ev.emit.apply(ev, arguments); };
  this._setSfuHandle = function (h) {
    state.sfuHandle = h;
    // An app that subscribed to 'videoorientationchange' before the
    // engine bound must still get the parsing armed. (This slot used to
    // hold the CONSUMER's preferred-layers logic, misplaced onto
    // Producer where state.preferredLayers never exists — dead on this
    // side and missing on the other. The audit found it; the layer test
    // had only checked that the source text existed somewhere.)
    if (h && state.cvoArmed && typeof h.watchVideoOrientation === 'function') {
      try { h.watchVideoOrientation(true); } catch (e) {}
    }
  };
  // Producer has had this since M2; Consumer never did, and the M3b
  // layerschange bridge called it anyway — a TypeError thrown from
  // inside the engine's RTP tap, tens of thousands of times a second,
  // each one routed through node's uncaught-exception path. That storm
  // was the "fourth tab cannot connect" bug: the process kept running
  // and kept serving media, but its microtask queue never drained
  // again, so every await in the process — including the demo's join —
  // hung forever.
  this._sfuHandle = function () { return state.sfuHandle; };
}


/* ═══════════════════════════════════════════════════════════════════
 *  Consumer
 * ═══════════════════════════════════════════════════════════════════ */

function Consumer(opts) {
  var self = this;
  var ev = new EventEmitter();
  var observer = new EventEmitter();

  var state = {
    id:             opts.id,
    producerId:     opts.producerId,
    kind:           opts.kind,
    rtpParameters:  opts.rtpParameters,
    transport:      opts.transport,
    sfuHandle:      null,          // arrives when the engine binds
    paused:         !!opts.paused,
    producerPaused: !!opts.producerPaused,
    priority:       1,
    preferredLayers: null,   // { spatialLayer, temporalLayer } as the app asked
    // currentLayers is deliberately absent: it is computed from the
    // engine on read (see the getter), never stored.
    closed:         false,
    appData:        opts.appData || {},
  };

  Object.defineProperty(this, 'id',             { get: function () { return state.id; } });
  Object.defineProperty(this, 'producerId',     { get: function () { return state.producerId; } });
  Object.defineProperty(this, 'kind',           { get: function () { return state.kind; } });
  Object.defineProperty(this, 'rtpParameters',  { get: function () { return state.rtpParameters; } });
  // What the router forwards downstream, as opposed to what the peer
  // sent: the same codecs and extensions with a single consumable
  // encoding. mediasoup exposes it mainly for pipeToRouter and tests.
  Object.defineProperty(this, 'consumableRtpParameters', {
    get: function () {
      var rp = state.rtpParameters || {};
      var enc = (rp.encodings && rp.encodings[0]) || {};
      return {
        mid: undefined,
        codecs: (rp.codecs || []).slice(),
        headerExtensions: (rp.headerExtensions || []).slice(),
        encodings: [{ ssrc: enc.ssrc, maxBitrate: enc.maxBitrate }],
        rtcp: rp.rtcp || { cname: undefined, reducedSize: true },
      };
    },
  });
  Object.defineProperty(this, 'closed',         { get: function () { return state.closed; } });
  Object.defineProperty(this, 'paused',         { get: function () { return state.paused; } });
  Object.defineProperty(this, 'producerPaused', { get: function () { return state.producerPaused; } });
  Object.defineProperty(this, 'priority',       { get: function () { return state.priority; } });
  Object.defineProperty(this, 'type', {
    get: function () {
      if (state.kind !== 'video') return 'simple';
      var pr = state.transport && state.transport._router &&
               state.transport._router._producerById(state.producerId);
      var enc = pr && pr.rtpParameters && pr.rtpParameters.encodings;
      return (enc && enc.length > 1) ? 'simulcast' : 'simple';
    },
  });
  Object.defineProperty(this, 'observer',       { get: function () { return observer; } });
  Object.defineProperty(this, 'score',           { get: function () { return state.score || { score: 10, producerScore: 10 }; } });
  this._setScore = function (sc) { state.score = sc; };

  // Fired by the transport when the engine reports this consumer moved
  // layers. mediasoup emits 'layerschange' with the new layers, or with
  // undefined when the stream stops carrying — the demo's UI reads it to
  // show the live quality, so the undefined case matters as much.
  // mediasoup emits every forwarded packet to the app when it asks —
  // used for recording, forensics and custom relays. Off unless the app
  // actually subscribes, because copying every packet to a listener
  // that does not exist is pure cost.
  this._wantsRtp = function () { return !!state.wantRtp || trace.on('rtp'); };
  this._rtpOut = function (buf) {
    if (state.closed) return;
    if (state.wantRtp) {
      try { ev.emit('rtp', buf); } catch (e) {}
    }
    if (trace.on('rtp')) trace.fire('rtp', 'out', { ssrc: state.rtpParameters &&
      state.rtpParameters.encodings && state.rtpParameters.encodings[0] &&
      state.rtpParameters.encodings[0].ssrc });
  };

  this._layersChanged = function (layers) {
    if (state.closed) return;
    // NOTE: currentLayers is NOT cached here. The getter asks the engine
    // every time, because the engine can switch layers between events —
    // a cached value would be stale exactly when an app checks it after
    // hearing 'layerschange'. The field was written and never read.
    ev.emit('layerschange', layers || undefined);
    observer.emit('layerschange', layers || undefined);
  };
  // M3b: layers are real now. mediasoup's contract:
  //   preferredLayers — what the app asked for (undefined until it does)
  //   currentLayers   — what is actually being sent, or null when nothing
  //                     is (paused consumer / producer not carrying)
  // 'type' answers 'simulcast' once the producer really has several
  // encodings, because apps branch on it before touching layers at all.
  Object.defineProperty(this, 'preferredLayers', {
    get: function () { return state.preferredLayers || undefined; },
  });
  Object.defineProperty(this, 'currentLayers', {
    get: function () {
      if (state.closed || state.paused || state.producerPaused) return null;
      var h = state.sfuHandle;
      if (!h) return null;
      var sp = null, tp = null;
      try { sp = h.currentSpatialLayer; } catch (e) {}
      try { tp = h.currentTemporalLayer; } catch (e) {}
      if (sp == null) return null;
      // mediasoup always reports a temporal layer for a live consumer;
      // 0 is the honest answer for a stream with no temporal structure.
      return { spatialLayer: sp, temporalLayer: tp == null ? 0 : tp };
    },
  });
  Object.defineProperty(this, 'appData', {
    get: function () { return state.appData; },
    set: function (v) { state.appData = v; },
  });

  this._state = state;

  // The engine binds asynchronously (it must see the producer's media
  // flow before it can key the route). Any pause requested before that
  // is remembered here and applied the moment the handle exists — the
  // same conditions-not-history discipline the engine itself uses.
  this._setSfuHandle = function (h) {
    state.sfuHandle = h;
    // A preference set before the engine bound (mediasoup lets an app
    // call setPreferredLayers, or pass preferredLayers to consume(),
    // immediately) would otherwise be lost — the handle is the only
    // thing that can carry it down.
    if (h && state.preferredLayers) {
      if (typeof h.setPreferredSpatialLayer === 'function') {
        try { h.setPreferredSpatialLayer(state.preferredLayers.spatialLayer); } catch (e) {}
      }
      if (typeof h.setPreferredTemporalLayer === 'function') {
        try { h.setPreferredTemporalLayer(state.preferredLayers.temporalLayer); } catch (e) {}
      }
    }
    apply_flow();   // one rule, every entry point
  };

  function apply_flow() {
    if (!state.sfuHandle) return;
    // DTLS-GATED FLOW (found live, first full browser run of the compat
    // layer): the consumer was resumed at 42.8s, the forwarder emitted
    // seq 61918-61920 at 42.9s, and DTLS reached 'connected' only at
    // 43.1s — packets sent before the SRTP context exists are garbage
    // to the receiver, and worse, the engine's PROACTIVE KEYFRAME for
    // the late joiner was burned inside them. Chrome never got a decode
    // anchor and never will ask (it PLIs only streams it has already
    // attributed). Flow is therefore a CONDITION over three inputs —
    // paused, producerPaused, AND the transport's dtlsState — re-applied
    // whenever any of them changes, not an action taken at resume() time.
    var shouldFlow = !state.paused && !state.producerPaused &&
                     state.transport.dtlsState === 'connected';
    try {
      if (shouldFlow) state.sfuHandle.resume();
      else            state.sfuHandle.pause();
    } catch (e) {}
    // KEYFRAME ON FLOW-START (harvest #23 — closes the coverage gap in
    // #14, found when an EXISTING tab could not see a late joiner until
    // refresh): #14 anchors video consumers when DTLS reaches
    // 'connected' — which covers consumers created BEFORE connect (the
    // late joiner's own view). A consumer created on an
    // already-connected transport (veteran tab consuming a NEW
    // producer) never sees that transition; its only anchor was the
    // engine's proactive keyframe at BIND, which lands while the
    // consumer is still paused (mediasoup's own paused:true practice)
    // and is dropped in the pause window. Nobody asks again; the
    // veteran's decoder waits forever. So: EVERY transition into
    // flowing on a video consumer requests a fresh anchor — this is
    // exactly what mediasoup itself does on consumer.resume(). The flip
    // detection keeps it one request per transition, not per re-apply.
    if (shouldFlow && !state.wasFlowing && state.kind === 'video') {
      _diag('consumer ' + state.id.slice(0, 8) + ' FLOW-START → keyframe request (#23 path)');
      try { self.requestKeyFrame(); } catch (e) {}
    }
    state.wasFlowing = shouldFlow;
  }
  this._applyFlow = apply_flow;   // transport re-applies on dtlsstatechange

  this.pause = function () {
    if (state.closed || state.paused) return Promise.resolve();
    state.paused = true;
    apply_flow();
    observer.emit('pause');
    return Promise.resolve();
  };

  this.resume = function () {
    if (state.closed || !state.paused) return Promise.resolve();
    state.paused = false;
    apply_flow();
    observer.emit('resume');
    return Promise.resolve();
  };

  this.requestKeyFrame = function () {
    // keyFrameRequestDelay (ProducerOptions): mediasoup refuses to ask
    // the producer again inside this window. Ours is honoured on the
    // Producer that owns the delay, so several consumers of one producer
    // cannot combine into a PLI storm.
    var prod = router._producerById(state.producerId);
    if (prod && prod._kfDelay > 0) {
      var nowKf = Date.now();
      if (prod._lastKfAt && nowKf - prod._lastKfAt < prod._kfDelay) return Promise.resolve();
      prod._lastKfAt = nowKf;
    }
    var pr = router._producerById(state.producerId);
    _diag('consumer ' + state.id.slice(0, 8) + ' requestKeyFrame: producer=' +
          (pr ? 'found' : 'MISSING') + ' handle=' + (pr && pr._sfuHandle() ? 'yes' : 'NO'));
    if (state.closed) return Promise.resolve();
    if (state.kind !== 'video') return Promise.resolve();   // mediasoup: video only
    var p = state.transport._router._producerById(state.producerId);
    if (p && p._sfuHandle()) { try { p._sfuHandle().requestKeyframe(); } catch (e) {} }
    return Promise.resolve();
  };

  // QUALITY surface: recorded, not acted on. Until layer selection lands
  // (native M3) every consumer receives the one continuous stream the
  // forwarder produces — which is exactly what a single-layer producer
  // would give anyway, so the app's media is correct, just not tuned.
  this.setPreferredLayers = function (layers) {
    // ENFORCED (M3b). The engine treats the spatial index as a CEILING,
    // not a pin: if the requested layer is not carrying, cascade 3c
    // still rides one that is, which is both mediasoup's behaviour and
    // the only way a viewer never freezes on an idle layer.
    // temporalLayer is accepted and reported back, but has no effect
    // until temporal (TID) selection lands — that stays a declared gap
    // rather than a silent lie.
    layers = layers || {};
    var sp = (layers.spatialLayer == null) ? null : (layers.spatialLayer | 0);
    var tp = (layers.temporalLayer == null) ? null : (layers.temporalLayer | 0);
    if (state.closed) return Promise.reject(InvalidStateError('Consumer closed'));
    state.preferredLayers = { spatialLayer: sp, temporalLayer: tp };
    var h = state.sfuHandle;
    if (h && typeof h.setPreferredSpatialLayer === 'function') {
      try { h.setPreferredSpatialLayer(sp); } catch (e) {}
    }
    if (h && typeof h.setPreferredTemporalLayer === 'function') {
      try { h.setPreferredTemporalLayer(tp); } catch (e) {}
    }
    return Promise.resolve();
  };

  this.setPriority = function (priority) {
    warn_once('setPriority', 'setPriority() is recorded but bandwidth allocation is not implemented.');
    state.priority = priority;
    return Promise.resolve();
  };
  this.unsetPriority = function () { state.priority = 1; return Promise.resolve(); };

  this.getStats = function () {
    if (typeof state.transport._statsForSsrc !== 'function') {
      return Promise.resolve([{
        type: 'outbound-rtp', timestamp: Date.now(), kind: state.kind,
        ssrc: (state.rtpParameters.encodings || [{}])[0].ssrc,
      }]);
    }
    return state.transport._statsForSsrc('outbound-rtp', state.rtpParameters);
  };
  var trace = TraceHub(function (e) { ev.emit('trace', e); },
                       function (e) { observer.emit('trace', e); });
  this.enableTraceEvent = function (types) {
    var r = trace.enable(types);
    // Some trace types cost work on the RTCP path, so the engine only
    // does that work once an app has asked for them.
    try {
      var wantFeedback = (types || []).some(function (t) {
        return t === 'nack' || t === 'fir';
      });
      if (wantFeedback && state.transport && state.transport._router &&
          state.transport._router._sfuRouter) {
        state.transport._router._sfuRouter.watchFeedback(true);
      }
    } catch (e) {}
    return r;
  };
  this._trace = trace;

  this.close = function () {
    if (state.closed) return;
    state.closed = true;
    if (state.sfuHandle) { try { state.sfuHandle.close(); } catch (e) {} }
    state.transport._onConsumerClosed(self);
    observer.emit('close');
  };

  /* ── events driven from the router/transport ── */
  this._producerClosed = function () {
    if (state.closed) return;
    state.closed = true;
    if (state.sfuHandle) { try { state.sfuHandle.close(); } catch (e) {} }
    state.transport._onConsumerClosed(self);
    ev.emit('producerclose');
    observer.emit('close');
  };
  this.transportClosed = function () { return self._transportClosed(); };
  this._transportClosed = function () {
    if (state.closed) return;
    state.closed = true;
    ev.emit('transportclose');
    observer.emit('close');
  };
  this._producerPaused = function () {
    if (state.closed || state.producerPaused) return;
    state.producerPaused = true;
    apply_flow();
    ev.emit('producerpause');
    observer.emit('pause');
  };
  this._producerResumed = function () {
    if (state.closed || !state.producerPaused) return;
    state.producerPaused = false;
    apply_flow();
    ev.emit('producerresume');
    observer.emit('resume');
  };

  // Producer has had this since M2. Consumer's copy was accidentally
  // pasted onto Producer twice instead, so every caller reaching for a
  // consumer's engine handle got undefined — the layerschange bridge
  // only survived because it type-checks first.
  this._sfuHandle = function () { return state.sfuHandle; };

  this.on = function (n, f) {
    if (n === 'rtp') state.wantRtp = true;   // see _rtpOut / the sendRtp wrapper
    ev.on(n, f); return self;
  };
  this.once = function (n, f) {
    if (n === 'rtp') state.wantRtp = true;
    ev.once(n, f); return self;
  };
  this.off  = function (n, f) { ev.off(n, f);  return self; };
  this._emit = function () { ev.emit.apply(ev, arguments); };
}


/* ═══════════════════════════════════════════════════════════════════
 *  DataProducer / DataConsumer  (round 3 — DataChannels through the router)
 *
 *  MECHANICS: mediasoup data channels are NEGOTIATED (no DCEP) — client
 *  and server pre-agree the SCTP stream id, each side opens its channel
 *  with { negotiated: true, id: streamId }. webrtc-server's SCTP stack
 *  carries them over the association the bootstrap channel already
 *  established. Routing lives in the compat Router: a message arriving
 *  on a DataProducer's channel is fanned out to every DataConsumer of
 *  it whose channel is open.
 * ═══════════════════════════════════════════════════════════════════ */

function DataProducer(opts) {
  var self = this, ev = new EventEmitter(), observer = new EventEmitter();
  var state = {
    id: opts.id, label: opts.label || '', protocol: opts.protocol || '',
    sctpStreamParameters: opts.sctpStreamParameters,
    transport: opts.transport, channel: opts.channel,
    paused: false, closed: false, appData: opts.appData || {},
    messagesReceived: 0, bytesReceived: 0,
  };
  Object.defineProperty(this, 'id',       { get: function () { return state.id; } });
  Object.defineProperty(this, 'type',     { get: function () { return 'sctp'; } });
  Object.defineProperty(this, 'label',    { get: function () { return state.label; } });
  Object.defineProperty(this, 'protocol', { get: function () { return state.protocol; } });
  Object.defineProperty(this, 'sctpStreamParameters', { get: function () { return state.sctpStreamParameters; } });
  Object.defineProperty(this, 'closed',   { get: function () { return state.closed; } });
  Object.defineProperty(this, 'paused',   { get: function () { return state.paused; } });
  Object.defineProperty(this, 'observer', { get: function () { return observer; } });
  Object.defineProperty(this, 'appData', {
    get: function () { return state.appData; }, set: function (v) { state.appData = v; } });
  this._state = state;

  this.pause  = function () {
    state.paused = true;  observer.emit('pause');
    // Consumers surface the producer's state as dataProducerPaused, the
    // same contract Consumer/Producer already honour for media.
    try {
      state.transport._router._forEachDataConsumerOf(state.id, function (dc) {
        if (dc._dataProducerPaused) dc._dataProducerPaused(true);
      });
    } catch (e) {}
    return Promise.resolve();
  };
  this.resume = function () {
    state.paused = false; observer.emit('resume');
    try {
      state.transport._router._forEachDataConsumerOf(state.id, function (dc) {
        if (dc._dataProducerPaused) dc._dataProducerPaused(false);
      });
    } catch (e) {}
    return Promise.resolve();
  };

  // DirectTransport path: the app injects messages from server code
  // (the official demo's chat bot does exactly this) — same fan-out as
  // a wire message, no channel involved.
  this.send = function (message, ppid, subchannels, requiredSubchannel, ignoredSubchannel) {
    if (state.closed || state.paused) return;
    state.messagesReceived++;
    state.bytesReceived += (typeof message === 'string') ? Buffer.byteLength(message) : (message.byteLength || message.length || 0);
    state.transport._router._fanOutData(state.id, message, {
      subchannels: subchannels,
      requiredSubchannel: requiredSubchannel,
      ignoredSubchannel: ignoredSubchannel,
    });
  };
  var trace = TraceHub(function (e) { ev.emit('trace', e); },
                       function (e) { observer.emit('trace', e); });
  this.enableTraceEvent = function (types) {
    var r = trace.enable(types);
    // Some trace types cost work on the RTCP path, so the engine only
    // does that work once an app has asked for them.
    try {
      var wantFeedback = (types || []).some(function (t) {
        return t === 'nack' || t === 'fir';
      });
      if (wantFeedback && state.transport && state.transport._router &&
          state.transport._router._sfuRouter) {
        state.transport._router._sfuRouter.watchFeedback(true);
      }
    } catch (e) {}
    return r;
  };
  this._trace = trace;

  this.getStats = function () {
    return Promise.resolve([{
      type: 'data-producer', timestamp: Date.now(),
      label: state.label, protocol: state.protocol,
      messagesReceived: state.messagesReceived, bytesReceived: state.bytesReceived,
    }]);
  };
  this.close = function () {
    if (state.closed) return;
    state.closed = true;
    try { state.channel && state.channel.close(); } catch (e) {}
    state.transport._onDataProducerClosed(self);
    observer.emit('close');
  };
  this.transportClosed = function () { return self._transportClosed(); };
  this._transportClosed = function () {
    if (state.closed) return;
    state.closed = true;
    try { state.channel && state.channel.close(); } catch (e) {}
    ev.emit('transportclose');
    observer.emit('close');
  };
  this.on   = function (n, f) { ev.on(n, f);   return self; };
  this.off  = function (n, f) { ev.off(n, f);  return self; };
  this.once = function (n, f) { ev.once(n, f); return self; };
}

function DataConsumer(opts) {
  var self = this, ev = new EventEmitter(), observer = new EventEmitter();
  var state = {
    id: opts.id, dataProducerId: opts.dataProducerId,
    label: opts.label || '', protocol: opts.protocol || '',
    sctpStreamParameters: opts.sctpStreamParameters,
    transport: opts.transport, channel: opts.channel,
    closed: false, appData: opts.appData || {},
    messagesSent: 0, bytesSent: 0,
    pendingSends: [],   // messages sent server-side before the SCTP stream opened
    // SUBCHANNELS (mediasoup 3.13+): a DataConsumer subscribes to a set
    // of numeric subchannels, and a DataProducer message addressed to a
    // subchannel reaches only the consumers subscribed to it. [] means
    // "everything", which is the default and what every existing app
    // relies on.
    subchannels: (opts.subchannels || []).slice(),
    dataProducerPaused: !!opts.dataProducerPaused,
    bufferedAmountLowThreshold: 0,
    bufferedLowFired: true,
  };
  Object.defineProperty(this, 'id',             { get: function () { return state.id; } });
  Object.defineProperty(this, 'dataProducerId', { get: function () { return state.dataProducerId; } });
  Object.defineProperty(this, 'type',           { get: function () { return 'sctp'; } });
  Object.defineProperty(this, 'label',          { get: function () { return state.label; } });
  Object.defineProperty(this, 'protocol',       { get: function () { return state.protocol; } });
  Object.defineProperty(this, 'sctpStreamParameters', { get: function () { return state.sctpStreamParameters; } });
  Object.defineProperty(this, 'closed',         { get: function () { return state.closed; } });
  Object.defineProperty(this, 'observer',       { get: function () { return observer; } });
  Object.defineProperty(this, 'subchannels',    { get: function () { return state.subchannels.slice(); } });
  Object.defineProperty(this, 'dataProducerPaused', { get: function () { return state.dataProducerPaused; } });
  // Live SCTP send-buffer occupancy, straight from the channel.
  Object.defineProperty(this, 'bufferedAmount', {
    get: function () {
      try { return (state.channel && state.channel.bufferedAmount) || 0; } catch (e) { return 0; }
    },
  });
  Object.defineProperty(this, 'bufferedAmountLowThreshold', {
    get: function () { return state.bufferedAmountLowThreshold; },
    set: function (v) { self.setBufferedAmountLowThreshold(v); },
  });
  Object.defineProperty(this, 'appData', {
    get: function () { return state.appData; }, set: function (v) { state.appData = v; } });
  this._state = state;

  this.getBufferedAmount = function () {
    return Promise.resolve(self.bufferedAmount);
  };

  // mediasoup mirrors the W3C threshold semantics: when the buffer drains
  // to at or below the threshold after having been above it, one
  // 'bufferedamountlow' fires. The edge-trigger (not level) is what makes
  // it usable for flow control rather than a per-message firehose.
  this.setBufferedAmountLowThreshold = function (v) {
    state.bufferedAmountLowThreshold = v | 0;
    try {
      if (state.channel) {
        state.channel.bufferedAmountLowThreshold = state.bufferedAmountLowThreshold;
        if (!state._bufLowHooked) {
          state._bufLowHooked = true;
          var onLow = function () {
            if (state.closed) return;
            ev.emit('bufferedamountlow', self.bufferedAmount);
          };
          try { state.channel.addEventListener('bufferedamountlow', onLow); }
          catch (e) { try { state.channel.onbufferedamountlow = onLow; } catch (e2) {} }
        }
      }
    } catch (e) {}
    return Promise.resolve();
  };

  this.setSubchannels = function (subchannels) {
    if (state.closed) return Promise.reject(InvalidStateError('DataConsumer closed'));
    state.subchannels = (subchannels || []).slice();
    return Promise.resolve();
  };
  this.addSubchannel = function (subchannel) {
    if (state.closed) return Promise.reject(InvalidStateError('DataConsumer closed'));
    var n = subchannel | 0;
    if (state.subchannels.indexOf(n) < 0) state.subchannels.push(n);
    return Promise.resolve();
  };
  this.removeSubchannel = function (subchannel) {
    if (state.closed) return Promise.reject(InvalidStateError('DataConsumer closed'));
    var i = state.subchannels.indexOf(subchannel | 0);
    if (i >= 0) state.subchannels.splice(i, 1);
    return Promise.resolve();
  };
  // Router fan-out asks this before delivering: no subchannels means the
  // consumer takes everything (the default), and a message with no
  // subchannel likewise goes to everyone.
  this._wantsSubchannel = function (sub) {
    if (sub == null) return true;
    if (!state.subchannels.length) return true;
    return state.subchannels.indexOf(sub | 0) >= 0;
  };
  this._dataProducerPaused = function (paused) {
    if (state.closed || state.dataProducerPaused === !!paused) return;
    state.dataProducerPaused = !!paused;
    ev.emit(paused ? 'dataproducerpause' : 'dataproducerresume');
    observer.emit(paused ? 'dataproducerpause' : 'dataproducerresume');
  };

  // Called by the Router's fan-out; guarded on channel readiness so a
  // consumer whose SCTP stream hasn't opened yet drops (SCTP itself is
  // reliable per-message once open; pre-open messages have nowhere to go
  // and mediasoup drops them identically).
  this._forward = function (data) {
    if (state.closed) return;
    var ch = state.channel;
    if (ch) {
      // webrtc transport: out over the negotiated SCTP stream
      if (ch.readyState !== 'open') return;
      try {
        ch.send(data);
        state.messagesSent++;
        state.bytesSent += (typeof data === 'string') ? Buffer.byteLength(data) : (data.byteLength || data.length || 0);
      } catch (e) {}
    } else {
      // DirectTransport: deliver to server code as a 'message' event —
      // mediasoup's signature is (message, ppid); 51 = string WebRTC ppid.
      state.messagesSent++;
      state.bytesSent += (typeof data === 'string') ? Buffer.byteLength(data) : (data.byteLength || data.length || 0);
      ev.emit('message', data, typeof data === 'string' ? 51 : 53);
    }
  };
  var trace = TraceHub(function (e) { ev.emit('trace', e); },
                       function (e) { observer.emit('trace', e); });
  this.enableTraceEvent = function (types) {
    var r = trace.enable(types);
    // Some trace types cost work on the RTCP path, so the engine only
    // does that work once an app has asked for them.
    try {
      var wantFeedback = (types || []).some(function (t) {
        return t === 'nack' || t === 'fir';
      });
      if (wantFeedback && state.transport && state.transport._router &&
          state.transport._router._sfuRouter) {
        state.transport._router._sfuRouter.watchFeedback(true);
      }
    } catch (e) {}
    return r;
  };
  this._trace = trace;

  // dataConsumer.send() — DISCOVERED LIVE by the official demo's bot
  // (harvest: 'TypeError: botDataConsumer.send is not a function'): a
  // real mediasoup method for server-side injection of a message to
  // THIS consumer only (the bot's personal welcome). The welcome is
  // sent the instant the consumer is created — before the client-side
  // channel has opened — so sends are queued and flushed on open; SCTP
  // would have buffered the same way.
  this.send = function (message) {
    if (state.closed) return Promise.reject(InvalidStateError('DataConsumer closed'));
    var ch = state.channel;
    if (ch && ch.readyState === 'open') {
      try {
        ch.send(message);
        state.messagesSent++;
        state.bytesSent += (typeof message === 'string') ? Buffer.byteLength(message) : (message.byteLength || message.length || 0);
      } catch (e) { return Promise.reject(e); }
      return Promise.resolve();
    }
    if (!ch) {
      // DirectTransport consumer: deliver as a message event (server code
      // listening on its own direct consumer)
      self._forward(message);
      return Promise.resolve();
    }
    // sctpsendbufferfull. mediasoup fires this when the association's
    // send buffer cannot take more, so an app can back off instead of
    // growing memory. Previously this fired whenever a message queued
    // before the channel opened — which is normal startup, not
    // pressure. Now it is measured against the channel's ACTUAL
    // buffered amount, and fires once per episode rather than per
    // message, so an app can distinguish "still filling" from "full
    // again".
    var buffered = 0;
    try { buffered = (state.channel && state.channel.bufferedAmount) || 0; } catch (e) {}
    var limit = state.sctpSendBufferSize || SCTP_SEND_BUFFER_LIMIT;
    if (buffered >= limit) {
      if (!state.bufferFullFired) {
        state.bufferFullFired = true;
        ev.emit('sctpsendbufferfull');
      }
    } else if (buffered === 0) {
      state.bufferFullFired = false;   // drained: the next episode is new
    }
    state.pendingSends.push(message);
    if (!state.flushHooked) {
      state.flushHooked = true;
      var flush = function () {
        var q = state.pendingSends; state.pendingSends = [];
        q.forEach(function (m) {
          try {
            ch.send(m);
            state.messagesSent++;
            state.bytesSent += (typeof m === 'string') ? Buffer.byteLength(m) : (m.byteLength || m.length || 0);
          } catch (e) {}
        });
      };
      try { ch.addEventListener('open', flush); } catch (e) { try { ch.onopen = flush; } catch (e2) {} }
    }
    return Promise.resolve();
  };

  this.getStats = function () {
    return Promise.resolve([{
      type: 'data-consumer', timestamp: Date.now(),
      label: state.label, protocol: state.protocol,
      messagesSent: state.messagesSent, bytesSent: state.bytesSent,
    }]);
  };
  this.close = function () {
    if (state.closed) return;
    state.closed = true;
    try { state.channel && state.channel.close(); } catch (e) {}
    state.transport._onDataConsumerClosed(self);
    observer.emit('close');
  };
  this._dataProducerClosed = function () {
    if (state.closed) return;
    state.closed = true;
    try { state.channel && state.channel.close(); } catch (e) {}
    state.transport._onDataConsumerClosed(self);
    ev.emit('dataproducerclose');
    observer.emit('close');
  };
  this.transportClosed = function () { return self._transportClosed(); };
  this._transportClosed = function () {
    if (state.closed) return;
    state.closed = true;
    try { state.channel && state.channel.close(); } catch (e) {}
    ev.emit('transportclose');
    observer.emit('close');
  };
  this.on   = function (n, f) { ev.on(n, f);   return self; };
  this.off  = function (n, f) { ev.off(n, f);  return self; };
  this.once = function (n, f) { ev.once(n, f); return self; };
}


/* ═══════════════════════════════════════════════════════════════════
 *  WebRtcTransport
 * ═══════════════════════════════════════════════════════════════════ */

function WebRtcTransport(opts, cb) {
  var self = this;
  var ev = new EventEmitter();
  var observer = new EventEmitter();

  var router       = opts.router;         // compat Router
  var webRtcServer = opts.webRtcServer;   // optional shared-port server

  var state = {
    id:              uuid(),
    pc:              null,
    ownSocket:       null,     // only when we bind our own port
    ownWebrtcRouter: null,     // only when we own the demuxer
    port:            null,
    iceParameters:   null,
    iceCandidates:   [],
    dtlsParameters:  null,
    iceState:        'new',
    dtlsState:       'new',
    clientDtls:      null,
    inboundGated:    true,
    clientMidBySsrc: {},       // ssrc → client-facing ordinal mid
    extRemapBySsrc:  {},       // ssrc → { internalExtId: clientExtId } (see wrapper)
    consumerBySsrc:  {},       // outgoing ssrc → Consumer (hot-path index)
    nextProduceMid:  1,        // internal mid namespace (0 = bootstrap DC)
    produceMids:     [],
    nextClientMid:   0,        // client-facing ordinal namespace
    sfuPeer:         null,     // sfu.js peer handle
    producers:       [],       // compat Producer objects
    consumers:       [],       // compat Consumer objects
    closed:          false,
    appData:         opts.appData || {},
    statSamples:     {},       // ssrc → { t, bytes, packets, lost }  (bitrate + score deltas)
    enableSctp:      opts.enableSctp !== false,   // mediasoup default is false, but our
                                                  // bootstrap ALWAYS builds the association —
                                                  // advertising it costs nothing and the demo
                                                  // passes enableSctp:true anyway
    numSctpStreams:  opts.numSctpStreams || { OS: 1024, MIS: 1024 },
    sctpState:       'new',
    dataProducers:   [],
    dataConsumers:   [],
    negotiationChain: Promise.resolve(),   // serializes all SDP work on this pc (see chained)
    nextDataStreamId: 100,     // negotiated-id space for OUR consumers; starts
                               // above the DCEP-assigned ids (bootstrap channel)
                               // so the two allocation worlds can't collide
    scoreTimer:      null,
    orphanTimer:     null,     // legacy path: ICE 'disconnected' grace (full mode only)
    lastInboundAt:   0,        // any datagram from the peer (lite liveness)
    livenessTimer:   null,
  };

  /* ── public surface ── */
  Object.defineProperty(this, 'id',             { get: function () { return state.id; } });
  Object.defineProperty(this, 'closed',         { get: function () { return state.closed; } });
  Object.defineProperty(this, 'type',           { get: function () { return 'webrtc'; } });
  Object.defineProperty(this, 'iceRole',        { get: function () { return 'controlled'; } });
  Object.defineProperty(this, 'iceParameters',  { get: function () { return state.iceParameters; } });
  Object.defineProperty(this, 'iceCandidates',  { get: function () { return state.iceCandidates; } });
  Object.defineProperty(this, 'dtlsParameters', { get: function () { return state.dtlsParameters; } });
  Object.defineProperty(this, 'iceState',       { get: function () { return state.iceState; } });
  // The tuple ICE actually settled on. mediasoup reports it as soon as a
  // pair is nominated and null before that; the app uses it for logging
  // and for telling relayed clients apart from direct ones.
  Object.defineProperty(this, 'iceSelectedTuple', {
    get: function () {
      if (state.iceSelectedTuple) return state.iceSelectedTuple;
      try {
        var sel = state.pc && state.pc._manager && state.pc._manager.state &&
                  state.pc._manager.state.iceAgent &&
                  state.pc._manager.state.iceAgent.selectedPair;
        if (!sel) return undefined;
        var L = sel.local || {}, R = sel.remote || {};
        return {
          localIp: L.address || L.ip, localPort: L.port,
          remoteIp: R.address || R.ip, remotePort: R.port,
          protocol: (R.protocol || L.protocol || 'udp').toLowerCase(),
        };
      } catch (e) { return undefined; }
    },
  });
  // The peer's certificate as verified during the DTLS handshake.
  Object.defineProperty(this, 'dtlsRemoteCert', {
    get: function () { return state.dtlsRemoteCert; },
  });
  // What SCTP actually agreed on (mediasoup fills this after the
  // association establishes; undefined while SCTP is not negotiated).
  Object.defineProperty(this, 'sctpNegotiatedCapabilities', {
    get: function () {
      if (!state.sctpParameters) return undefined;
      return { numStreams: {
        OS:  state.sctpParameters.OS,
        MIS: state.sctpParameters.MIS,
      } };
    },
  });
  Object.defineProperty(this, 'dtlsState',      { get: function () { return state.dtlsState; } });
  Object.defineProperty(this, 'sctpParameters', { get: function () {
    if (!state.enableSctp) return undefined;
    return {
      port: 5000,
      OS:  state.numSctpStreams.OS,
      MIS: state.numSctpStreams.MIS,
      maxMessageSize: 262144,
      isDataChannel: true,      // mediasoup-internal flag their client reads
    };
  } });
  Object.defineProperty(this, 'sctpState',      { get: function () { return state.enableSctp ? state.sctpState : undefined; } });
  Object.defineProperty(this, 'observer',       { get: function () { return observer; } });
  Object.defineProperty(this, 'pc',             { get: function () { return state.pc; } });
  Object.defineProperty(this, 'appData', {
    get: function () { return state.appData; },
    set: function (v) { state.appData = v; },
  });

  this._router = router;
  this._state  = state;

  /* ── construction ────────────────────────────────────────────────
   * Two topologies:
   *
   *  (a) webRtcServer given — the shared-port model. WebRTCRouter owns
   *      one socket for every transport, demuxing by 5-tuple (fast) or
   *      STUN USERNAME (first packet). This is mediasoup's WebRtcServer,
   *      and webrtc-server's version is a superset (RFC 9443 lets it
   *      share the port with TURN/QUIC).
   *
   *  (b) no webRtcServer — a port per transport. A real mediasoup server
   *      opens a UDP port per transport and announces it, and lite mode
   *      was DESIGNED for exactly that shape ("any external socket ⇒
   *      lite"): without a socket, lite has nothing to gather (found
   *      live: created → 0 candidates → the client had nowhere to send
   *      checks and hung silently).
   */
  if (webRtcServer) {
    state.pc = new RTCPeerConnection({ router: webRtcServer._webrtcRouter });
    state.port = webRtcServer.port;
    // INBOUND GATE, mode (a): the socket is shared, so we cannot hold
    // the line for one transport without stalling every other peer on
    // the port. See the note on the gate below — this is a VERIFY item.
    state.inboundGated = false;
    after_pc_ready(webRtcServer.announcedAddresses);
  } else {
    bind_udp_in_range(opts.portRange, function (err, sock) {
      if (err) return cb(err);
      state.ownSocket = sock;
      state.port = sock.address().port;

      // INBOUND ROUTING — the piece the first cut missed (found live:
      // connecting→failed with zero rx diag). An external socket is a
      // SEND path for the stack; nobody subscribes to its 'message'
      // events — in the designed topology that's the demuxer's job.
      // So run the designed topology: a WebRTCRouter owns the demux and
      // we feed it every datagram. Same battle-tested path the
      // multi-peer harness uses — not a bespoke shortcut.
      var wr = new WebRTCRouter({ socket: sock });
      state.ownWebrtcRouter = wr;

      // RACE KILLER (found live as flaky media/disconnects): Chrome
      // fires its DTLS ClientHello the instant ICE completes, sometimes
      // BEFORE our connect-time renegotiation installs the client's REAL
      // fingerprint — so verification ran against the placeholder and the
      // handshake died (or survived only via retransmit luck). Chrome
      // cannot finish ICE without OUR check responses, so we simply hold
      // ALL inbound until the real fingerprint is in place. STUN/DTLS
      // retransmissions make the dropped packets free.
      sock.on('message', function (msg, rinfo) {
        state.lastInboundAt = Date.now();        // lite liveness (see below)
        if (state.inboundGated) return;          // pre-connect: hold the line
        wr.dispatch(msg, rinfo);
      });

      state.pc = new RTCPeerConnection({ mode: 'lite', router: wr });
      after_pc_ready(opts.announcedAddresses);
    });
  }

  function after_pc_ready(announced) {
    var pc = state.pc;

    // STRUCTURAL MID ENFORCEMENT (supersedes the timer-based restamp,
    // which lost whenever a bind landed after the last timer): this
    // layer owns the client-facing mid namespace, so it wraps the
    // manager's registerOutboundStream — ANY registration for a
    // deliverable ssrc, from any code path at any time, gets its mid
    // overridden to the client ordinal. Invariant by construction; no
    // ordering games.
    try {
      var mgr = pc._manager;
      var origReg = mgr.registerOutboundStream.bind(mgr);
      mgr.registerOutboundStream = function (ssrc, info) {
        var mapped = state.clientMidBySsrc[ssrc];
        if (mapped != null && info) info = Object.assign({}, info, { mid: mapped });
        return origReg(ssrc, info);
      };
    } catch (e) {}

    // STRUCTURAL EXTENSION-ID ENFORCEMENT (found live by the full-ext
    // wire probe, and it is the black-window bug): the stamper writes
    // header extensions with OUR session's ids while the client parses
    // with ITS OWN — the probe showed the TWCC counter at id 4 and the
    // mid at id 9, while the client's answer negotiated mid=4/twcc=3.
    // Chrome reads id 4 expecting MID, sees an incrementing binary
    // counter ("\x01î", "\x01ï", …), finds no such m-line, and drops
    // 100% of packets — the direct sibling of M2 extmap bug #9, one
    // layer up: two extension-id namespaces and nobody translating.
    //
    // The mid VALUE was already fixed by the wrapper above; this fixes
    // the ID PLANE the same structural way: sendRtp sees plaintext RTP
    // (the SRTP boundary is deeper), so for deliverable ssrcs we rewrite
    // each one-byte-profile element id from our namespace to the
    // client's, and blank (0x00-pad — legal padding the parser skips)
    // any element whose URI the client never declared. Per-ssrc maps are
    // built at consume() time from our localDescription × consumerCaps.
    //
    // ROOT-FIX CANDIDATE for webrtc-server (record in ROADMAP): a
    // forwarded stream lives in the CONSUMER's extension namespace, so
    // registerOutboundStream should accept a per-stream extension-id
    // map and the stamper should honor it. This wrapper is the compat
    // layer's structural stopgap until then.
    try {
      var origSendRtp = mgr.sendRtp.bind(mgr);
      mgr.sendRtp = function (buf) {
        try {
          if (buf && buf.length > 16 && (buf[0] & 0x10)) {
            var ssrc = ((buf[8] << 24) | (buf[9] << 16) | (buf[10] << 8) | buf[11]) >>> 0;
            var remap = state.extRemapBySsrc[ssrc];
            if (remap) {
              // The id translation is RTP knowledge, and rtp-packet owns
              // it: rewrite in place, blank anything the receiver has no
              // number for, and hand back where the payload starts.
              var payloadStart = remapExtensionIds(buf, remap);
              if (payloadStart < 0) payloadStart = 12;

              // KEYFRAME NOTIFICATION. Two things want to know that a
              // keyframe just left: the diagnostic log, and the app's
              // trace stream. Only parse when one of them is actually
              // listening — and never by throwing: the previous version
              // used `if (!_DIAG) throw 0` as a jump, which raised and
              // caught an exception FOR EVERY OUTGOING PACKET.
              var kfC = state.consumerBySsrc ? state.consumerBySsrc[ssrc] : null;
              // consumer.rtp — the app receives the exact bytes we send.
              // The flag is set at subscription time, so an unsubscribed
              // consumer costs one property read.
              if (kfC && kfC._wantsRtp && kfC._wantsRtp()) kfC._rtpOut(buf);
              var wantKf = _DIAG || (kfC && kfC._trace && kfC._trace.on('keyframe'));
              // Only ask about a VIDEO consumer whose codec we actually
              // know. The previous expression evaluated to `false` for
              // audio and to '' for an unindexed ssrc, both of which the
              // dispatcher then read as "assume VP8".
              var kfCodec = (kfC && kfC.kind === 'video') ? kfC._codecName : null;
              if (wantKf && kfCodec && is_keyframe(buf, kfCodec)) {
                var now2 = Date.now();
                if (!state._kfLastLog) state._kfLastLog = {};
                var kfKey = 'kf' + ssrc;
                if (!state._kfLastLog[kfKey] || now2 - state._kfLastLog[kfKey] > 1000) {
                  state._kfLastLog[kfKey] = now2;
                  if (_DIAG) {
                    console.log('[ms-kf ' + state.id.slice(0, 8) + '] KEYFRAME on wire → ssrc=' + ssrc);
                  }
                }
                // The trace event is NOT rate limited: an app asked to be
                // told about keyframes, and silently dropping some would
                // make the stream unusable for measuring.
                if (kfC && kfC._trace) kfC._trace.fire('keyframe', 'out', { ssrc: ssrc });
              }
            }
          }
        } catch (e) {}
        return origSendRtp(buf);
      };
    } catch (e) {}

    // Surface ICE/DTLS state the way mediasoup apps expect. They log
    // these and close transports on 'failed'.
    state.onIceStateChange = function () {
      var s = pc.iceConnectionState;
      // FULL vocabulary mapping (harvest #18, found live the first time
      // the engine's lite-liveness reached 'failed'): the original map
      // had no case for 'failed', so it aliased to 'new' — and the
      // orphan-grace logic below reads any non-disconnected transition
      // as recovery and CANCELED the timer seconds before it would have
      // fired. The fallback then closed at 62s with a message blaming
      // the engine, which had in fact worked perfectly (disconnected at
      // 30s, failed at 45s, exactly per its thresholds).
      //   - mediasoup's iceState has no 'failed'; the closest honest
      //     word in their vocabulary is 'disconnected', and mapping it
      //     there also keeps the grace timer armed (same mapped value →
      //     the early-return leaves the timer alone).
      //   - unknown states keep the PREVIOUS value rather than aliasing
      //     to 'new': a reset-looking default is the worst possible
      //     fallback for state that gates cleanup.
      var mapped = s === 'connected' ? 'connected'
                 : s === 'completed' ? 'completed'
                 : s === 'disconnected' ? 'disconnected'
                 : s === 'failed' ? 'disconnected'
                 : s === 'closed' ? 'closed'
                 : s === 'new' || s === 'checking' ? 'new'
                 : state.iceState;
      if (mapped === state.iceState) return;
      state.iceState = mapped;

      // ORPHAN CLEANUP path 1 (kept as a bonus): ICE 'disconnected'
      // held for a grace period. NOTE (found by reading the agent
      // source after this never fired live): in LITE mode this state is
      // UNREACHABLE — consent freshness (RFC 7675) is the only producer
      // of 'disconnected'/'failed' verdicts and cascade 2.8 gates it on
      // mode !== 'lite'. A vanished peer therefore stays 'connected'
      // forever in lite. Correct for a pure receiver, wrong for an SFU
      // that SENDS from the lite side. Path 2 below (datagram-silence
      // liveness) is the one that actually fires here; this path stays
      // for any future full-mode transport.
      // ROOT-FIX CANDIDATE for turn-server [S]: passive lite liveness —
      // browsers send binding-request keepalives every few seconds on a
      // live pair, so stamp lastSeen on every inbound check and declare
      // 'disconnected' after N quiet seconds. Observation, not consent;
      // RFC-clean.
      if (mapped === 'disconnected') {
        if (!state.orphanTimer) {
          state.orphanTimer = setTimeout(function () {
            if (state.closed || state.iceState !== 'disconnected') return;
            _diag('transport ' + state.id + ' orphaned (ice disconnected > 20s) — closing');
            self.close();
          }, 20000);
          if (state.orphanTimer.unref) state.orphanTimer.unref();
        }
      } else if (state.orphanTimer) {
        clearTimeout(state.orphanTimer);
        state.orphanTimer = null;
      }

      ev.emit('icestatechange', mapped);
      observer.emit('icestatechange', mapped);
    };
    pc.addEventListener('iceconnectionstatechange', state.onIceStateChange);

    state.onConnStateChange = function () {
      var s = pc.connectionState;
      var mapped = s === 'connected' ? 'connected'
                 : s === 'connecting' ? 'connecting'
                 : s === 'failed' ? 'failed'
                 : s === 'closed' ? 'closed' : 'new';
      if (mapped === state.dtlsState) return;
      state.dtlsState = mapped;
      if (mapped === 'connected' && state.enableSctp && state.sctpState !== 'connected') {
        state.sctpState = 'connected';
        ev.emit('sctpstatechange', 'connected');
        observer.emit('sctpstatechange', 'connected');
      }
      if (mapped === 'connected') {
        // The keys exist NOW. Re-run every consumer's flow condition
        // (any resume() that arrived early starts here), and hand each
        // video decoder a fresh anchor — the proactive keyframe the
        // engine sent at bind predates the SRTP context and is lost.
        state.consumers.forEach(function (c) {
          try { c._applyFlow(); } catch (e) {}
          try { if (c.kind === 'video') {
            _diag('consumer ' + c.id.slice(0, 8) + ' DTLS-CONNECTED → keyframe request (#14 path)');
            c.requestKeyFrame();
          } } catch (e) {}
        });
      }
      ev.emit('dtlsstatechange', mapped);
      observer.emit('dtlsstatechange', mapped);
    };
    pc.addEventListener('connectionstatechange', state.onConnStateChange);

    // Bootstrap: a datachannel gives the offer its m-line — this drives
    // credential + certificate materialization through the PUBLIC api,
    // so we parse our own SDP for the params rather than reaching into
    // private state.
    try { pc.createDataChannel('ms-bootstrap'); } catch (e) {}

    // ORPHAN CLEANUP path 2 — datagram-silence FALLBACK. History: this
    // was the primary while lite mode had no producer of 'disconnected'
    // at all (turn-server harvest #17). The root fix (passive lite
    // liveness in ice_agent, RFC 8445 §11 keepalives as evidence) makes
    // path 1 the primary: agent declares disconnected at ~30s, path 1
    // closes after a further 20s grace (~50s total). This fallback is
    // therefore set BEYOND that horizon — 60s — so it can only fire when
    // the engine mechanism did NOT, which makes every log line
    // self-attributing:
    //   'orphaned (ice disconnected > 20s)'  → primary path, engine fix working
    //   'orphaned (NNs datagram silence)'    → REGRESSION: engine liveness
    //     failed to fire (old turn-server on npm, or a lite-liveness bug)
    // It also still covers installs running a pre-fix turn-server, where
    // path 1 is structurally dead. Shared-port (webRtcServer) transports
    // can't observe per-peer datagrams from here and ride on the root
    // fix alone.
    state.livenessTimer = setInterval(function () {
      if (state.closed || state.dtlsState !== 'connected') return;
      if (!state.ownSocket) return;                 // shared-port: see note
      if (!state.lastInboundAt) return;
      var quiet = Date.now() - state.lastInboundAt;
      if (quiet > 60000) {
        _diag('transport ' + state.id + ' orphaned (' + Math.round(quiet / 1000) +
              's datagram silence — FALLBACK fired; engine lite-liveness did not) — closing');
        self.close();
      }
    }, 5000);
    if (state.livenessTimer.unref) state.livenessTimer.unref();

    // SCORER + BITRATE SAMPLER (round 2): one getStats() sweep per tick
    // serves every producer/consumer on this transport. Scores are
    // mediasoup's 0-10 from the loss ratio over the sample window;
    // deltas are kept per-ssrc and reused by _statsForSsrc for bitrate.
    state.scoreTimer = setInterval(function () {
      if (state.closed || !state.pc || state.dtlsState !== 'connected') return;
      // BWE trace. The estimate is read on the tick we already run, and
      // reported only when it MOVES by a margin worth an app's attention
      // — a trace stream that repeats the same number every 2.5s is
      // noise, and mediasoup's own bwe traces are change-driven too.
      try {
        var anyBwe = state.consumers.some(function (c) {
          return c._trace && c._trace.on('bwe');
        });
        if (anyBwe && state.pc && state.pc._manager &&
            typeof state.pc._manager.getEstimatedBandwidth === 'function') {
          var bps = state.pc._manager.getEstimatedBandwidth();
          var prev = state.lastBweReported;
          if (bps && (!prev || Math.abs(bps - prev) / prev > 0.15)) {
            state.lastBweReported = bps;
            state.consumers.forEach(function (c) {
              if (c._trace) {
                c._trace.fire('bwe', 'out', {
                  availableBitrate: bps,
                  previousBitrate: prev || undefined,
                });
              }
            });
          }
        }
      } catch (e) {}

      // [ms-fwd] — per-consumer forwarded-packet counts from the ENGINE
      // handle (the same proof-of-life the old harness printed). A
      // consumer whose count stays 0 while media flows is an unbound or
      // silent route; a growing count with a black window is a decode
      // (keyframe) problem. One line discriminates the two families.
      try {
        var parts = [];
        state.consumers.forEach(function (c) {
          var h = c._state && c._state.sfuHandle;
          var n = 0;
          try { n = (h && h.getStats && h.getStats().forwarded) || 0; } catch (e) {}
          parts.push(c.kind.slice(0, 1) + ':' + c.id.slice(0, 8) +
                     '=' + n + (c.paused ? '(paused)' : '') + (h ? '' : '(NO-HANDLE)'));
        });
        if (parts.length && _DIAG) console.log('[ms-fwd ' + state.id.slice(0, 8) + '] ' + parts.join('  '));
      } catch (e) {}
      state.pc.getStats().then(function (report) {
        var now = {};   // ssrc → {t, bytes, packets, lost, inbound}
        report.forEach(function (r) {
          if (r.type !== 'inbound-rtp' && r.type !== 'outbound-rtp' &&
              r.type !== 'remote-inbound-rtp') return;
          var key = r.ssrc >>> 0;
          var e = now[key] = now[key] || { t: r.timestamp || Date.now() };
          if (r.type === 'inbound-rtp') {
            e.bytes = r.bytesReceived || 0; e.packets = r.packetsReceived || 0;
            e.lost = r.packetsLost || 0; e.inbound = true;
          } else if (r.type === 'outbound-rtp') {
            e.bytes = r.bytesSent || 0; e.packets = r.packetsSent || 0;
            e.inbound = false;
          } else {  // remote-inbound-rtp: the viewer's report on OUR sending
            e.lost = r.packetsLost || 0;
          }
        });
        function score_for(ssrc) {
          var cur = now[ssrc >>> 0], prev = state.statSamples[ssrc >>> 0];
          if (!cur || !prev || cur.packets == null || prev.packets == null) return 10;
          var dPkts = Math.max(0, cur.packets - prev.packets);
          var dLost = Math.max(0, (cur.lost || 0) - (prev.lost || 0));
          if (dPkts + dLost === 0) return 10;
          var ratio = dLost / (dPkts + dLost);
          // mediasoup-flavored: 10 down to 0 as loss climbs past ~50%
          return Math.max(0, Math.min(10, Math.round(10 - ratio * 20)));
        }
        state.producers.forEach(function (p) {
          var enc = (p.rtpParameters.encodings || [])[0] || {};
          if (enc.ssrc == null) return;
          var sc = [{ ssrc: enc.ssrc >>> 0, score: score_for(enc.ssrc) }];
          var prev = (p.score && p.score[0] && p.score[0].score);
          p._setScore(sc);
          if (prev !== sc[0].score) { p._emit('score', sc); p.observer.emit('score', sc); }
        });
        state.consumers.forEach(function (c) {
          var enc = (c.rtpParameters.encodings || [])[0] || {};
          if (enc.ssrc == null) return;
          var v = score_for(enc.ssrc);
          var sc = { score: v, producerScore: v, producerScores: [v] };
          var prev = c.score && c.score.score;
          c._setScore(sc);
          if (prev !== v) { c._emit('score', sc); c.observer.emit('score', sc); }
        });
        state.statSamples = now;
      }).catch(function () {});
    }, 2500);
    if (state.scoreTimer.unref) state.scoreTimer.unref();

    // THE BRIDGE: this pc joins the SFU router as a first-class peer.
    // Its track events auto-create engine producers and its transceivers
    // are what consumers are built on. One engine, two front doors.
    state.sfuPeer = router._sfuRouter.addPeer(pc);

    run_bootstrap(announced);
  }

  function run_bootstrap(announced) {
    var pc = state.pc;
    pc.createOffer().then(function (offer) {
      return pc.setLocalDescription(offer);
    }).then(function () {
      var sdp = pc.localDescription && pc.localDescription.sdp;
      if (!sdp) throw new Error('bootstrap offer produced no localDescription');
      var p = parse_own_offer(sdp);
      if (!p.ufrag || !p.pwd || !p.fingerprint) {
        throw new Error('bootstrap offer missing ice/dtls essentials');
      }

      // Candidates are BUILT here (mediasoup-style), not parsed — one
      // host candidate per routable address, all on the bound port.
      var addrs = (announced && announced.length) ? announced : host_ipv4_addresses();
      state.iceCandidates = addrs.map(function (ip, idx) {
        return {
          foundation: 'udpcandidate' + idx,
          protocol:   'udp',
          priority:   (1 << 24) * 126 + (1 << 8) * (65535 - idx) + 255,
          ip:         ip,        // mediasoup v3 historical name…
          address:    ip,        // …and the newer alias; ship both
          port:       state.port,
          type:       'host',
        };
      });
      state.iceParameters  = { usernameFragment: p.ufrag, password: p.pwd, iceLite: true };
      state.dtlsParameters = { role: 'auto', fingerprints: [p.fingerprint] };

      // Reach STABLE at birth (found live: a transport waiting in
      // have-local-offer starves cascade 0 — consumer records existed but
      // no transceivers were ever added). Real mediasoup has no SDP state
      // machine to block on; we emulate that by answering our own
      // bootstrap with a PLACEHOLDER fingerprint. The client's REAL
      // fingerprint replaces it at connect, and DTLS verifies against the
      // LATEST remote description at handshake time — always after
      // connect. setup:active here fixes our role early (we stay
      // passive/server) so the later renegotiation agrees with it.
      return pc.setRemoteDescription(new RTCSessionDescription({
        type: 'answer', sdp: build_bootstrap_answer(PLACEHOLDER_DTLS),
      }));
    }).then(function () {
      _diag('transport ' + state.id + ' ready — ' + state.iceCandidates.length + ' candidates');
      cb(null, self);
    }).catch(function (err) { cb(err); });
  }

  /* ── internal renegotiation ──────────────────────────────────────
   * our-offer → synthesized client answer (recvonly media, the client's
   * stored fingerprint). The engine's cascades fire off the resulting
   * signaling events and bind the consumers.
   */
  // NEGOTIATION SERIALIZER (harvest #20, found by reading the demo's
  // Room code after tab-2 heard but never saw): mediasoup-demo fires
  // createConsumer for every existing producer WITHOUT awaiting — audio
  // and video consume() ran CONCURRENTLY on the same pc, each driving
  // its own createOffer/setLocalDescription. The second started while
  // the first sat at have-local-offer; the video m-line never got a
  // committed answer, sender.ssrc stayed null, and the video consume
  // rejected with 'no outgoing ssrc' — one WARN line drowned in the
  // flood. The spike never hit this because it awaited one consume at a
  // time. Every SDP-touching operation on this transport now runs
  // through one promise chain: same behavior for polite sequential
  // callers, correctness for concurrent ones.
  function chained(fn) {
    var run = state.negotiationChain.then(fn, fn);
    state.negotiationChain = run.then(function () {}, function () {});
    return run;
  }
  // Public form: every external caller is serialized automatically.
  // renegotiateRaw is for code ALREADY running inside chained() — calling
  // the public form from there would wait on itself (classic
  // non-reentrant-mutex deadlock).
  function renegotiate() { return chained(renegotiateRaw); }

  function renegotiateRaw() {
    var pc = state.pc;
    if (state.closed || !pc) return Promise.reject(InvalidStateError('transport closed'));
    // PRE-CONNECT RENEGOTIATION IS THE NORMAL mediasoup FLOW (found live,
    // first browser run of the compat layer): a recv transport's
    // consume() happens BEFORE connect() — mediasoup-client only fires
    // its 'connect' event once the app calls recvTransport.consume()
    // with the parameters this very renegotiation produces. Blocking on
    // clientDtls here deadlocked the entire receive direction. So:
    // before connect, the synthesized answer carries the same
    // PLACEHOLDER fingerprint the bootstrap uses — safe under the gate
    // invariant (no inbound byte is processed until the real
    // fingerprint is installed), and connect() re-renegotiates with the
    // real one the moment it arrives.
    var dtls = state.clientDtls || PLACEHOLDER_DTLS;
    return pc.createOffer()
      .then(function (offer) { return pc.setLocalDescription(offer); })
      .then(function () {
        var answer = synth_recv_answer(pc.localDescription.sdp, dtls);
        return pc.setRemoteDescription(new RTCSessionDescription({ type: 'answer', sdp: answer }));
      });
  }
  this._renegotiate = renegotiate;

  /* ── connect ─────────────────────────────────────────────────── */

  this.connect = function (options) {
    var dtls = options && options.dtlsParameters;
    if (state.closed) return Promise.reject(InvalidStateError('transport closed'));
    if (state.clientDtls) return Promise.resolve();          // idempotent
    if (!dtls) return Promise.reject(new TypeError('connect: dtlsParameters required'));
    state.clientDtls = dtls;

    // The pc is already stable (placeholder bootstrap). Run a full
    // renegotiation that (a) carries any transceivers added meanwhile
    // and (b) installs the client's REAL fingerprint — and only THEN
    // open the inbound gate, so the first DTLS byte we ever process
    // verifies against the real thing. Deterministic by construction:
    // no ICE completion → no ClientHello → no race.
    return renegotiate().then(function () {
      state.inboundGated = false;
      _diag('transport ' + state.id + ' connected — gate open');
    });
  };

  /* ── produce ─────────────────────────────────────────────────── */

  this.produce = function (options) {
    options = options || {};
    var kind = options.kind;
    var rp   = options.rtpParameters;

    if (state.closed) return Promise.reject(InvalidStateError('transport closed'));
    // Send transports really do connect first (mediasoup-client fires
    // 'connect' before 'produce'), so this guard is correct — unlike the
    // one that used to live in renegotiate().
    if (!state.clientDtls) return Promise.reject(InvalidStateError('produce before connect'));
    if (kind !== 'audio' && kind !== 'video') {
      return Promise.reject(new TypeError('produce: kind must be "audio" or "video"'));
    }
    // SIMULCAST TOLERANCE: mediasoup-client sends layered video as
    // multiple encodings — ssrc-based (each with its own ssrc + FID
    // rtx) or RID-based (rid strings, no ssrcs; Chrome's default for
    // send simulcast). Until layer SELECTION lands (native M3) we
    // accept both shapes and forward one continuous stream — the
    // engine keys on whichever inbound ssrc it observes, and the
    // consumer never learns there were layers (RtpForwarder's rewrite
    // guarantees that). Rejecting here would fail the official demo at
    // its default settings.
    var encodings = (rp && rp.encodings) || [];
    var enc = encodings[0] || {};
    var ridBased = enc.ssrc == null && encodings.some(function (e) { return e.rid != null; });
    if (enc.ssrc == null && !ridBased) {
      return Promise.reject(new TypeError(
        'produce: rtpParameters.encodings needs ssrc or rid'));
    }
    if (encodings.length > 1) {
      warn_once('simulcast',
        'simulcast producer accepted (' + encodings.length + ' encodings) — one layer is forwarded until layer selection lands (M3).');
    }
    try { validateProducerRtpParameters(rp, router.rtpCapabilities); }
    catch (e) { return Promise.reject(UnsupportedError(e.message)); }

    // codecOptions → codec parameters. Nearly every real app passes
    // these (opusStereo/opusDtx/videoGoogleStartBitrate…), and mediasoup
    // folds them into the codec's parameters — which matters twice for
    // us: they shape the produce offer's fmtp, AND getConsumerRtpParameters
    // copies parameters from the PRODUCER, so consumers inherit them.
    var co = options.codecOptions;
    if (co) {
      var OPT_TO_PARAM = {
        opusStereo: 'stereo', opusFec: 'useinbandfec', opusDtx: 'usedtx',
        opusMaxPlaybackRate: 'maxplaybackrate', opusMaxAverageBitrate: 'maxaveragebitrate',
        opusPtime: 'ptime', opusNack: 'nack',
        videoGoogleStartBitrate: 'x-google-start-bitrate',
        videoGoogleMaxBitrate: 'x-google-max-bitrate',
        videoGoogleMinBitrate: 'x-google-min-bitrate',
      };
      rp = Object.assign({}, rp, { codecs: (rp.codecs || []).map(function (c) {
        if (/\/rtx$/i.test(c.mimeType || '')) return c;
        var params = Object.assign({}, c.parameters || {});
        for (var k in OPT_TO_PARAM) {
          if (co[k] === undefined) continue;
          var v = co[k];
          params[OPT_TO_PARAM[k]] = (typeof v === 'boolean') ? (v ? 1 : 0) : v;
        }
        return Object.assign({}, c, { parameters: params });
      }) });
    }

    var mediaMid = String(state.nextProduceMid++);
    var priorMids = state.produceMids.slice();
    state.produceMids.push(mediaMid);

    var offerSdp = synth_produce_offer(kind, rp, mediaMid, state.clientDtls, priorMids);
    var pc = state.pc;

    return pc.setRemoteDescription(new RTCSessionDescription({ type: 'offer', sdp: offerSdp }))
      .then(function () { return pc.createAnswer(); })
      .then(function (answer) { return pc.setLocalDescription(answer); })
      .then(function () {
        var producer = new Producer({
          id: options.id || uuid(), kind: kind, rtpParameters: rp,
          transport: self, paused: !!options.paused, appData: options.appData,
        });
        // ProducerOptions.keyFrameRequestDelay — the producer owns it, so
        // every consumer's request passes through one throttle.
        producer._kfDelay = Number(options.keyFrameRequestDelay) || 0;
        state.producers.push(producer);
        router._registerProducer(producer);

        // The engine creates its own producer record from the pc's
        // 'track' event. Claim it when it appears — the correlation is
        // by (our peer, kind, not-yet-claimed), which is unambiguous
        // because produce() is serialized per transport.
        router._claimSfuProducer(state.sfuPeer, kind, function (sfuHandle) {
          producer._setSfuHandle(sfuHandle);
          _diag('producer ' + producer.id + ' ↔ engine ' + sfuHandle.id);
        });

        observer.emit('newproducer', producer);
        return producer;
      });
  };

  /* ── consume ─────────────────────────────────────────────────────
   * The structural piece. mediasoup is per-producer and app-driven, so
   * this maps 1:1 onto the engine's peer.consume(producer, cb) — the
   * primitive Room bypasses with its own policy.
   *
   * Ordering matters and is not arbitrary:
   *   1. engine consume()  → cascade 0 adds the sendonly transceiver
   *   2. renegotiate()     → the m-line lands, mid/ssrc are committed,
   *                          and the synthesized answer gives cascade 2
   *                          the payload type it resolves against
   *   3. read ssrc/rtxSsrc off the new transceiver
   *   4. claim the client-facing ordinal mid BEFORE anything registers
   *      an outbound stream, so the structural wrapper stamps it
   *   5. build rtpParameters in the CONSUMER's dialect (ortc)
   */
  this.consume = function (options) {
    options = options || {};
    var producerId = options.producerId;
    var caps       = options.rtpCapabilities;

    if (state.closed) return Promise.reject(InvalidStateError('transport closed'));
    var producer = router._producerById(producerId);
    if (!producer) return Promise.reject(NotFoundError('Producer not found: ' + producerId));
    if (!caps) return Promise.reject(new TypeError('consume: rtpCapabilities required'));
    if (!ortc_can_consume(producer.rtpParameters, caps)) {
      return Promise.reject(UnsupportedError(
        'cannot consume producer ' + producerId + ' with the given rtpCapabilities'));
    }
    var sfuProducer = producer._sfuHandle();
    if (!sfuProducer) {
      return Promise.reject(InvalidStateError(
        'producer ' + producerId + ' has no media yet — consume once its track exists'));
    }

    var consumer = null;
    var pendingHandle = null;

    return chained(function () {
      // snapshot INSIDE the chain: the pairing (engine transceiver ↔ this
      // consume) is only unambiguous while no sibling consume interleaves.
      var before = snapshot_transceivers();

      // Fire the engine's consume. Its callback lands when the route
      // actually binds — which can be EARLIER than our Consumer object
      // exists (harvest #24, read straight off the run log: the answer's
      // setRemoteDescription fires signalingstatechange, the engine's
      // cascades run SYNCHRONOUSLY inside it, and BIND + this callback
      // land before our own .then creates the Consumer). The old
      // `if (consumer)` guard silently dropped the handle in that
      // ordering — for EVERY consumer, always — which made pause/resume
      // decorative no-ops on a null pointer and starved the late
      // joiner: the engine's resume() is what re-bases the timeline and
      // requests the fresh keyframe, and it never ran. Buffer the
      // handle; whichever side arrives second completes the attach.
      state.sfuPeer.consume(sfuProducer, function (err, handle) {
        if (err) { _diag('engine consume failed: ' + err.message); return; }
        if (consumer && !consumer.closed) consumer._setSfuHandle(handle);
        else pendingHandle = handle;
      });

      return renegotiateRaw().then(function () {
      var t = new_transceiver_since(before);
      if (!t) throw new Error('consume: engine added no transceiver');

      var ssrc = null, rtxSsrc = null;
      try { ssrc    = t.sender.ssrc; }    catch (e) {}
      try { rtxSsrc = t.sender.rtxSsrc; } catch (e) {}
      if (ssrc == null) throw new Error('consume: no outgoing ssrc after negotiation');

      // THE MID REMAP. mediasoup-client numbers its consumer m-lines
      // with its OWN ordinals regardless of the mid we send, and Chrome
      // demuxes by the MID header extension FIRST — stamping our
      // internal mid routed video into its audio m-line and audio into
      // the void (100% silent discard, zero inbound-rtp rows). The
      // client-facing mid is therefore an ordinal per transport, claimed
      // here and enforced structurally in registerOutboundStream.
      var clientMid = String(state.nextClientMid++);
      state.clientMidBySsrc[ssrc >>> 0] = clientMid;
      if (rtxSsrc != null) state.clientMidBySsrc[rtxSsrc >>> 0] = clientMid;

      // Extension-ID translation table for this stream: OUR id (from the
      // pc's committed local description) → the CLIENT's id (from its
      // rtpCapabilities), joined on URI. Unmapped URIs are blanked by
      // the sendRtp wrapper.
      var ourExt = {};    // uri → internal id
      try {
        var sdp = state.pc.localDescription && state.pc.localDescription.sdp || '';
        var m; var re = /a=extmap:(\d+)(?:\/\w+)? (\S+)/g;
        while ((m = re.exec(sdp))) ourExt[m[2]] = parseInt(m[1], 10);
      } catch (e) {}
      var idMap = {};
      (caps.headerExtensions || []).forEach(function (ce) {
        var internal = ourExt[ce.uri];
        var clientId = ce.preferredId != null ? ce.preferredId : ce.id;
        if (internal != null && clientId != null) idMap[internal] = clientId;
      });
      state.extRemapBySsrc[ssrc >>> 0] = idMap;
      if (rtxSsrc != null) state.extRemapBySsrc[rtxSsrc >>> 0] = idMap;
      _diag('consumer ext-id remap for ssrc ' + (ssrc >>> 0) + ': ' + JSON.stringify(idMap));
      try {
        state.pc._manager.registerOutboundStream(ssrc, { mid: clientMid, rtxSsrc: rtxSsrc });
      } catch (e) {}

      var rtpParameters = getConsumerRtpParameters({
        producerRtpParameters: producer.rtpParameters,
        consumerCaps:          caps,
        kind:                  producer.kind,
        ssrc:                  ssrc >>> 0,
        rtxSsrc:               rtxSsrc != null ? (rtxSsrc >>> 0) : undefined,
        mid:                   clientMid,
      });

      // OPTIONS AT CREATION. mediasoup lets an app state its layer
      // preference and RTX choice in consume() itself rather than in a
      // follow-up call — apps rely on the consumer never having sent a
      // single packet above the requested layer, which a later
      // setPreferredLayers cannot promise.
      if (options.enableRtx === false && rtpParameters.codecs) {
        rtpParameters.codecs = rtpParameters.codecs.filter(function (cd) {
          return !/\/rtx$/i.test(cd.mimeType || '');
        });
      }
      consumer = new Consumer({
        id: uuid(), producerId: producerId, kind: producer.kind,
        rtpParameters: rtpParameters, transport: self,
        // mediasoup strongly recommends paused:true here, and the reason
        // applies to us identically: an RTP packet arriving before the
        // remote consumer exists gets bound to the wrong m= section.
        paused: !!options.paused,
        producerPaused: producer.paused,
        appData: options.appData,
      });
      if (options.preferredLayers) {
        try { consumer.setPreferredLayers(options.preferredLayers); } catch (e) {}
      }
      state.consumers.push(consumer);
      // Index by outgoing ssrc: the sendRtp wrapper is per packet and
      // must reach the consumer without scanning.
      state.consumerBySsrc[ssrc >>> 0] = consumer;
      consumer._codecName = String((rtpParameters.codecs[0] || {}).mimeType || '')
                              .split('/')[1] || '';
      router._registerConsumer(consumer);
      if (pendingHandle) {           // the engine got there first (the common case)
        consumer._setSfuHandle(pendingHandle);
        pendingHandle = null;
      }
      observer.emit('newconsumer', consumer);
      _diag('consumer ' + consumer.id + ' ssrc=' + ssrc + ' clientMid=' + clientMid +
            ' pt=' + rtpParameters.codecs[0].payloadType);
      return consumer;
      });
    });
  };

  function snapshot_transceivers() {
    var seen = [];
    try { seen = (state.pc.getTransceivers() || []).slice(); } catch (e) {}
    return seen;
  }
  function new_transceiver_since(before) {
    var now = snapshot_transceivers();
    for (var i = 0; i < now.length; i++) {
      if (before.indexOf(now[i]) < 0) return now[i];
    }
    return null;
  }

  /* ── the rest of the Transport surface ───────────────────────── */

  this.dump = function () {
    return _dumpOf(self, {
      direct: false,
      producerIds:     state.producers.map(function (p) { return p.id; }),
      consumerIds:     state.consumers.map(function (c) { return c.id; }),
      dataProducerIds: state.dataProducers.map(function (p) { return p.id; }),
      dataConsumerIds: state.dataConsumers.map(function (c) { return c.id; }),
      iceRole: 'controlled',
      iceParameters: state.iceParameters,
      iceCandidates:  state.iceCandidates,
      iceState: state.iceState,
      iceSelectedTuple: self.iceSelectedTuple,
      dtlsParameters: state.dtlsParameters,
      dtlsState: state.dtlsState,
      sctpParameters: state.sctpParameters,
      sctpState: state.sctpState,
      recvRtpHeaderExtensions: {},
      rtpListener: { ssrcTable: [], midTable: [], ridTable: [] },
    });
  };

  this.restartIce = function () {
    if (state.closed) return Promise.reject(InvalidStateError('transport closed'));
    var beforeUfrag = state.iceParameters && state.iceParameters.usernameFragment;
    try { state.pc.restartIce(); } catch (e) { return Promise.reject(e); }
    return renegotiate().then(function () {
      var p = parse_own_offer(state.pc.localDescription.sdp);
      state.iceParameters = { usernameFragment: p.ufrag, password: p.pwd, iceLite: true };
      // The whole point of restartIce is fresh credentials — if the
      // engine did not rotate them, surface it instead of silently
      // handing back stale params the client will treat as new.
      if (beforeUfrag && p.ufrag === beforeUfrag) {
        warn_once('restartIceRotate',
          'restartIce(): engine did not rotate ice-ufrag — restart is likely a no-op (stack finding to verify).');
      }
      return state.iceParameters;
    });
  };

  this.getStats = function () {
    if (state.closed || !state.pc) return Promise.resolve([]);
    return state.pc.getStats().then(function (report) {
      var out = [];
      report.forEach(function (s) {
        if (s.type !== 'transport' && s.type !== 'candidate-pair') return;
        out.push({
          type:            'webrtc-transport',
          transportId:     state.id,
          timestamp:       s.timestamp,
          bytesReceived:   s.bytesReceived || 0,
          bytesSent:       s.bytesSent || 0,
          iceState:        state.iceState,
          dtlsState:       state.dtlsState,
          iceRole:         'controlled',
        });
      });
      return out;
    }).catch(function () { return []; });
  };

  // Shared by Producer.getStats / Consumer.getStats: pull the W3C rows
  // for a known ssrc and reshape into mediasoup's flatter stat objects.
  // mediasoup's documented ProducerStat/ConsumerStat shape, translated
  // from the W3C report. Fields we can't derive yet are present with
  // honest zeros — dashboards read keys, and a missing key breaks them
  // where a zero merely reads as quiet.
  this._statsForSsrc = function (wantType, rtpParameters) {
    var want = null;
    (rtpParameters.encodings || []).some(function (e) {
      if (e.ssrc != null) { want = e.ssrc >>> 0; return true; }
      return false;
    });
    if (state.closed || !state.pc) return Promise.resolve([]);
    return state.pc.getStats().then(function (report) {
      var out = [];
      report.forEach(function (s) {
        if (s.type !== wantType) return;
        if (want != null && (s.ssrc >>> 0) !== want) return;
        var inbound = wantType === 'inbound-rtp';
        var packets = inbound ? (s.packetsReceived || 0) : (s.packetsSent || 0);
        var bytes   = inbound ? (s.bytesReceived  || 0) : (s.bytesSent  || 0);
        out.push({
          // ── documented mediasoup stat keys ──
          type:                 inbound ? 'inbound-rtp' : 'outbound-rtp',
          timestamp:            s.timestamp,
          ssrc:                 (s.ssrc >>> 0),
          rtxSsrc:              undefined,
          kind:                 s.kind,
          mimeType:             (rtpParameters.codecs[0] || {}).mimeType,
          packetsLost:          s.packetsLost || 0,
          fractionLost:         s.fractionLost || 0,
          packetsDiscarded:     s.packetsDiscarded || 0,
          packetsRetransmitted: s.retransmittedPacketsSent || 0,
          packetsRepaired:      0,
          nackCount:            s.nackCount || 0,
          nackPacketCount:      s.nackCount || 0,
          pliCount:             s.pliCount || 0,
          firCount:             s.firCount || 0,
          score:                10,
          packetCount:          packets,
          byteCount:            bytes,
          bitrate:              (function () {
            var prev = state.statSamples[s.ssrc >>> 0];
            if (!prev || prev.bytes == null) return 0;
            var dt = ((s.timestamp || Date.now()) - prev.t) / 1000;
            if (dt <= 0) return 0;
            return Math.max(0, Math.round((bytes - prev.bytes) * 8 / dt));
          })(),
          roundTripTime:        s.roundTripTime || 0,
          jitter:               s.jitter || 0,
        });
      });
      return out;
    }).catch(function () { return []; });
  };

  // POLICY surface: accepted and remembered, not enforced. Bandwidth
  // policy is a native milestone; until then media flows untuned rather
  // than not at all.
  function bitrate_noop(name) {
    return function (bitrate) {
      warn_once(name, name + '() is accepted but not enforced yet — media flows untuned.');
      state[name] = bitrate;
      return Promise.resolve();
    };
  }
  this.setMaxIncomingBitrate = bitrate_noop('setMaxIncomingBitrate');
  this.setMaxOutgoingBitrate = bitrate_noop('setMaxOutgoingBitrate');
  this.setMinOutgoingBitrate = bitrate_noop('setMinOutgoingBitrate');

  /* ── produceData / consumeData (round 3 — the real thing) ──────
   * The client's produceData creates a NEGOTIATED channel on its side
   * with a streamId it chose; we open the matching receiving end with
   * the same id. consumeData allocates a fresh id from OUR space and
   * hands the client sctpStreamParameters so IT opens the matching end.
   * Fan-out lives in the Router (one producer → N consumers across
   * transports), guarded per-consumer on channel readiness.
   */
  this.produceData = function (options) {
    options = options || {};
    if (state.closed) return Promise.reject(InvalidStateError('transport closed'));
    if (!state.enableSctp) return Promise.reject(UnsupportedError('transport created without enableSctp'));
    var sp = options.sctpStreamParameters;
    if (!sp || sp.streamId == null) {
      return Promise.reject(new TypeError('produceData: sctpStreamParameters.streamId required'));
    }
    var dcOpts = { negotiated: true, id: sp.streamId };
    if (sp.ordered === false) dcOpts.ordered = false;
    if (sp.maxPacketLifeTime != null) dcOpts.maxPacketLifeTime = sp.maxPacketLifeTime;
    if (sp.maxRetransmits != null)    dcOpts.maxRetransmits    = sp.maxRetransmits;
    var channel;
    try { channel = state.pc.createDataChannel(options.label || '', dcOpts); }
    catch (e) { return Promise.reject(e); }

    var dp = new DataProducer({
      id: uuid(), label: options.label, protocol: options.protocol,
      sctpStreamParameters: sp, transport: self, channel: channel,
      appData: options.appData,
    });
    state.dataProducers.push(dp);
    router._registerDataProducer(dp);

    // The fan-out source: every message on the producer's channel goes
    // to the Router for distribution. ONE listener style only (found
    // live: every chat message arrived twice). The M1 #1 lesson was
    // about a stack emitting to only ONE style — and its FIX made
    // webrtc-server emit to BOTH, so dual registration here received
    // every event twice and doubled the fan-out. Registering both
    // styles is the right defense against a stack you don't know;
    // against our own stack, whose contract is emit-to-both, it is a
    // guaranteed duplicate.
    var onMsg = function (evData) {
      if (dp.closed || dp.paused) return;
      var data = evData && evData.data !== undefined ? evData.data : evData;
      dp._state.messagesReceived++;
      dp._state.bytesReceived += (typeof data === 'string') ? Buffer.byteLength(data) : (data.byteLength || data.length || 0);
      router._fanOutData(dp.id, data);
    };
    var registered = false;
    try { channel.addEventListener('message', onMsg); registered = true; } catch (e) {}
    if (!registered) { try { channel.onmessage = onMsg; } catch (e) {} }

    observer.emit('newdataproducer', dp);
    _diag('dataProducer ' + dp.id.slice(0, 8) + ' streamId=' + sp.streamId + ' label=' + (options.label || ''));
    return Promise.resolve(dp);
  };

  this.consumeData = function (options) {
    options = options || {};
    if (state.closed) return Promise.reject(InvalidStateError('transport closed'));
    if (!state.enableSctp) return Promise.reject(UnsupportedError('transport created without enableSctp'));
    var dp = router._dataProducerById(options.dataProducerId);
    if (!dp) return Promise.reject(NotFoundError('DataProducer not found: ' + options.dataProducerId));

    var streamId = state.nextDataStreamId++;
    var src = dp.sctpStreamParameters || {};
    var sp = { streamId: streamId };
    // reliability follows the producer: an unordered/partial-reliability
    // chat stream stays that way end to end
    if (src.ordered === false) sp.ordered = false; else sp.ordered = true;
    if (src.maxPacketLifeTime != null) sp.maxPacketLifeTime = src.maxPacketLifeTime;
    if (src.maxRetransmits != null)    sp.maxRetransmits    = src.maxRetransmits;

    var dcOpts = { negotiated: true, id: streamId };
    if (sp.ordered === false) dcOpts.ordered = false;
    if (sp.maxPacketLifeTime != null) dcOpts.maxPacketLifeTime = sp.maxPacketLifeTime;
    if (sp.maxRetransmits != null)    dcOpts.maxRetransmits    = sp.maxRetransmits;
    var channel;
    try { channel = state.pc.createDataChannel(dp.label || '', dcOpts); }
    catch (e) { return Promise.reject(e); }

    var dc = new DataConsumer({
      id: uuid(), dataProducerId: dp.id, label: dp.label, protocol: dp.protocol,
      sctpStreamParameters: sp, transport: self, channel: channel,
      appData: options.appData,
    });
    state.dataConsumers.push(dc);
    router._registerDataConsumer(dc);
    observer.emit('newdataconsumer', dc);
    _diag('dataConsumer ' + dc.id.slice(0, 8) + ' → producer ' + dp.id.slice(0, 8) + ' streamId=' + streamId);
    return Promise.resolve(dc);
  };
  var trace = TraceHub(function (e) { ev.emit('trace', e); },
                       function (e) { observer.emit('trace', e); });
  this.enableTraceEvent = function (types) {
    var r = trace.enable(types);
    // Some trace types cost work on the RTCP path, so the engine only
    // does that work once an app has asked for them.
    try {
      var wantFeedback = (types || []).some(function (t) {
        return t === 'nack' || t === 'fir';
      });
      if (wantFeedback && state.transport && state.transport._router &&
          state.transport._router._sfuRouter) {
        state.transport._router._sfuRouter.watchFeedback(true);
      }
    } catch (e) {}
    return r;
  };
  this._trace = trace;

  /* ── teardown ────────────────────────────────────────────────── */

  this._onDataProducerClosed = function (dp) {
    var i = state.dataProducers.indexOf(dp);
    if (i >= 0) state.dataProducers.splice(i, 1);
    router._forEachDataConsumerOf(dp.id, function (dc) { dc._dataProducerClosed(); });
    router._unregisterDataProducer(dp);
  };
  this._onDataConsumerClosed = function (dc) {
    var i = state.dataConsumers.indexOf(dc);
    if (i >= 0) state.dataConsumers.splice(i, 1);
    router._unregisterDataConsumer(dc);
  };
  this._onProducerClosed = function (producer) {
    var i = state.producers.indexOf(producer);
    if (i >= 0) state.producers.splice(i, 1);
    router._unregisterProducer(producer);
  };
  this._onConsumerClosed = function (consumer) {
    var i = state.consumers.indexOf(consumer);
    if (i >= 0) state.consumers.splice(i, 1);
    router._unregisterConsumer(consumer);
  };

  function teardown(reason) {
    if (state.closed) return;
    state.closed = true;
    // CHURN CASCADE ORDER (found by review while building the churn
    // test, before it could bite live): consumers of THIS transport's
    // producers living on OTHER transports must hear 'producerclose' —
    // that is the event apps forward so the remote client tears down its
    // <video>. The engine's own producerclose event can't deliver it
    // here: by the time sfuPeer.close() emits, we'd have already removed
    // the producer from producersById and the Router handler's lookup
    // would find nothing. So: notify ACROSS transports first, then
    // unwind our own.
    state.producers.slice().forEach(function (p) {
      router._forEachConsumerOf(p.id, function (c) { c._producerClosed(); });
    });
    state.dataProducers.slice().forEach(function (dp) {
      router._forEachDataConsumerOf(dp.id, function (dc) { dc._dataProducerClosed(); });
    });
    state.producers.slice().forEach(function (p) {
      router._unregisterProducer(p); p._transportClosed();
    });
    state.consumers.slice().forEach(function (c) {
      router._unregisterConsumer(c); c._transportClosed();
    });
    state.dataProducers.slice().forEach(function (dp) {
      router._unregisterDataProducer(dp); dp._transportClosed();
    });
    state.dataConsumers.slice().forEach(function (dc) {
      router._unregisterDataConsumer(dc); dc._transportClosed();
    });
    state.producers = []; state.consumers = [];
    state.dataProducers = []; state.dataConsumers = [];
    if (state.scoreTimer)    { clearInterval(state.scoreTimer);    state.scoreTimer = null; }
    if (state.orphanTimer)   { clearTimeout(state.orphanTimer);    state.orphanTimer = null; }
    if (state.livenessTimer) { clearInterval(state.livenessTimer); state.livenessTimer = null; }
    // Release our own listeners before closing the pc. Closing it does
    // drop them today, but that couples this teardown to another
    // object's implementation — and the pc is not always ours to close.
    try {
      if (state.pc && state.onIceStateChange) {
        state.pc.removeEventListener('iceconnectionstatechange', state.onIceStateChange);
      }
      if (state.pc && state.onConnStateChange) {
        state.pc.removeEventListener('connectionstatechange', state.onConnStateChange);
      }
    } catch (e) {}
    if (state.sfuPeer) { try { state.sfuPeer.close(); } catch (e) {} }
    try { state.pc && state.pc.close(); } catch (e) {}
    if (state.ownWebrtcRouter) { try { state.ownWebrtcRouter.close(); } catch (e) {} }
    if (state.ownSocket) { try { state.ownSocket.close(); } catch (e) {} }
    state.iceState = 'closed'; state.dtlsState = 'closed';
    if (reason) ev.emit(reason);
    observer.emit('close');
    router._onTransportClosed(self);
  }

  this.close       = function () { teardown(null); };
  this.routerClosed = function () {
    ev.emit('routerclose');
    teardown('routerclose');
  };
  this.listenServerClosed = function () {
    ev.emit('listenserverclose');
    teardown('listenserverclose');
  };
  this._routerClosed = function () { teardown('routerclose'); };

  this.on   = function (n, f) { ev.on(n, f);   return self; };
  this.off  = function (n, f) { ev.off(n, f);  return self; };
  this.once = function (n, f) { ev.once(n, f); return self; };
}


/* ═══════════════════════════════════════════════════════════════════
 *  WebRtcServer — the shared UDP port
 * ═══════════════════════════════════════════════════════════════════ */

function WebRtcServer(options, cb) {
  var self = this;
  var ev = new EventEmitter();
  var observer = new EventEmitter();

  var infos = (options && options.listenInfos) || [];
  var udp = infos.filter(function (i) { return !i.protocol || i.protocol === 'udp'; })[0] || {};
  var announced = [];
  infos.forEach(function (i) {
    if (i.announcedAddress) announced.push(i.announcedAddress);
    else if (i.announcedIp) announced.push(i.announcedIp);
    else if (i.ip && i.ip !== '0.0.0.0' && i.ip !== '::') announced.push(i.ip);
  });
  if (!announced.length) announced = host_ipv4_addresses();

  var state = {
    id: uuid(), closed: false, port: udp.port || 0,
    announcedAddresses: announced,
    webrtcRouter: null,
    transports: [],
    appData: (options && options.appData) || {},
  };

  Object.defineProperty(this, 'id',       { get: function () { return state.id; } });
  Object.defineProperty(this, 'closed',   { get: function () { return state.closed; } });
  Object.defineProperty(this, 'port',     { get: function () { return state.port; } });
  Object.defineProperty(this, 'observer', { get: function () { return observer; } });
  Object.defineProperty(this, 'announcedAddresses', { get: function () { return state.announcedAddresses; } });
  Object.defineProperty(this, 'appData', {
    get: function () { return state.appData; },
    set: function (v) { state.appData = v; },
  });

  this._webrtcRouter = null;

  // webrtc-server's WebRTCRouter IS mediasoup's WebRtcServer, with more:
  // one socket, 5-tuple fast path, STUN USERNAME slow path for a new
  // peer's first packet, ICE-restart survival, auto-cleanup on close —
  // plus RFC 9443 coexistence with TURN/QUIC on the same port, which
  // mediasoup does not offer.
  var wr = new WebRTCRouter({ announcedAddresses: state.announcedAddresses });
  Promise.resolve(wr.listen(state.port || 0)).then(function () {
    state.webrtcRouter = wr;
    self._webrtcRouter = wr;
    if (!state.port && wr.address && wr.address()) state.port = wr.address().port;
    _diag('webRtcServer ' + state.id + ' listening on ' + state.port);
    cb(null, self);
  }).catch(function (err) { cb(err); });

  this.close = function () {
    if (state.closed) return;
    state.closed = true;
    state.transports.slice().forEach(function (t) { try { t._routerClosed(); } catch (e) {} });
    try { wr.close(); } catch (e) {}
    observer.emit('close');
  };

  this.dump = function () {
    return _dumpOf(self, {
      udpSockets: [{ ip: '0.0.0.0', port: state.port }],
      tcpServers: [],
      webRtcTransportIds: state.transports.map(function (t) { return t.id; }),
      localIceUsernameFragments: [],
      tupleHashes: [],
    });
  };
  // mediasoup's WebRtcServer hands a transport to the shared port.
  this.handleWebRtcTransport = function (t) { self._attach(t); };
  this._unhandleWebRtcTransport = function (t) {
    var i = state.transports.indexOf(t);
    if (i >= 0) state.transports.splice(i, 1);
    observer.emit('webrtctransportunhandled', t);
  };
  this.workerClosed = function () { self.close(); };
  this._attach = function (t) {
    state.transports.push(t);
    observer.emit('webrtctransporthandled', t);
  };
  this.on   = function (n, f) { ev.on(n, f);   return self; };
  this.off  = function (n, f) { ev.off(n, f);  return self; };
  this.once = function (n, f) { ev.once(n, f); return self; };
}


/* ═══════════════════════════════════════════════════════════════════
 *  Router
 * ═══════════════════════════════════════════════════════════════════ */

function Router(options, workerSettings) {
  var self = this;
  var ev = new EventEmitter();
  var observer = new EventEmitter();

  var fullCaps = getRouterRtpCapabilities();
  var caps = filterCapabilitiesByMediaCodecs(fullCaps, (options && options.mediaCodecs) || null);

  var state = {
    id: uuid(),
    closed: false,
    caps: caps,
    sfuRouter: new SfuRouter(),
    transports: [],
    producersById: {},          // mediasoup producer id → Producer
    consumersById: {},          // mediasoup consumer id → Consumer
    dataProducersById: {},     // id → DataProducer
    pipePairs:         {},     // destination router id → pipe transport pair
    dataConsumersById: {},      // id → DataConsumer
    claimed: [],                // engine producer handles already claimed
    pendingClaims: [],          // { peerId, kind, cb }
    appData: (options && options.appData) || {},
  };

  Object.defineProperty(this, 'id',              { get: function () { return state.id; } });
  Object.defineProperty(this, 'closed',          { get: function () { return state.closed; } });
  Object.defineProperty(this, 'rtpCapabilities', { get: function () { return state.caps; } });
  Object.defineProperty(this, 'observer',        { get: function () { return observer; } });
  Object.defineProperty(this, 'appData', {
    get: function () { return state.appData; },
    set: function (v) { state.appData = v; },
  });

  this._sfuRouter = state.sfuRouter;

  /* ── engine → compat wiring ──────────────────────────────────── */

  // Engine producers appear from the pc's 'track' event, asynchronously
  // relative to our produce() call. Correlation is by (peer, kind,
  // unclaimed) — unambiguous because produce() is serialized per
  // transport. Late arrivals are matched against parked claims, so the
  // order of the two events does not matter.
  state.sfuRouter.on('producer', function (handle, ownerHandle) {
    for (var i = 0; i < state.pendingClaims.length; i++) {
      var pc = state.pendingClaims[i];
      if (pc.peerId !== ownerHandle.id || pc.kind !== handle.kind) continue;
      state.pendingClaims.splice(i, 1);
      state.claimed.push(handle);
      pc.cb(handle);
      return;
    }
    state.claimed.push(handle);   // unclaimed for now; a claim may still come
  });

  // M3b: the engine moved a consumer between simulcast layers. Find the
  // Consumer that owns that handle and let it emit 'layerschange' — the
  // official demo's UI reads exactly this to show the live quality, and
  // an app that never hears it thinks the stream is stuck at whatever it
  // first negotiated.
  // EVERY engine listener runs SYNCHRONOUSLY inside the RTP tap, so a
  // throw here does not fail one callback — it escapes into node's
  // uncaught-exception machinery once per packet. Isolation is not
  // defensive style here, it is the difference between a logged bug and
  // a wedged process.
  function safeEngineListener(name, fn) {
    state.sfuRouter.on(name, function () {
      try { fn.apply(null, arguments); }
      catch (e) {
        if (!state._listenerFaults) state._listenerFaults = {};
        if (!state._listenerFaults[name]) {
          state._listenerFaults[name] = true;
          console.error('[sfu-server/mediasoup-compat] listener for "' + name +
                        '" threw and was contained (reported once): ' + (e && e.stack || e));
        }
      }
    });
  }

  function consumerForHandle(handle) {
    for (var id in state.consumersById) {
      var c = state.consumersById[id];
      if (!c || c.closed || typeof c._sfuHandle !== 'function') continue;
      if (c._sfuHandle() === handle) return c;
    }
    return null;
  }
  function producerForHandle(handle) {
    for (var id in state.producersById) {
      var p = state.producersById[id];
      if (!p || p.closed || typeof p._sfuHandle !== 'function') continue;
      if (p._sfuHandle() === handle) return p;
    }
    return null;
  }

  safeEngineListener('consumerlayerschange', function (handle) {
    var c = consumerForHandle(handle);
    if (c) c._layersChanged(c.currentLayers);
  });

  // TRACE WIRING. mediasoup's trace stream carries the feedback events an
  // app needs to see congestion happening: a viewer asking for a keyframe
  // ('pli' inbound on the consumer) and us asking the producer for one
  // ('pli' outbound on the producer). Both come from the engine, which is
  // the only layer that sees RTCP.
  safeEngineListener('consumerpli', function (handle, info) {
    var c = consumerForHandle(handle);
    if (c && c._trace) c._trace.fire('pli', 'in', info);
  });
  safeEngineListener('producervideoorientation', function (handle, byteVal) {
    var p = producerForHandle(handle);
    if (p && p._videoOrientation) p._videoOrientation(byteVal);
  });
  safeEngineListener('consumerfeedback', function (handle, info) {
    var c = consumerForHandle(handle);
    if (c && c._trace) c._trace.fire(info.type, 'in', info);
  });
  safeEngineListener('producerpli', function (handle, info) {
    var p = producerForHandle(handle);
    if (p && p._trace) p._trace.fire('pli', 'out', info);
  });

  state.sfuRouter.on('producerclose', function (handle) {
    // Every consumer of this producer must hear 'producerclose' — that
    // is the event the app forwards so the client tears its <video> down.
    var owner = null;
    for (var id in state.producersById) {
      var p = state.producersById[id];
      if (p._sfuHandle() === handle) { owner = p; break; }
    }
    if (!owner) return;
    self._forEachConsumerOf(owner.id, function (c) { c._producerClosed(); });
    if (!owner.closed) owner.close();
  });

  this._claimSfuProducer = function (peerHandle, kind, cb) {
    // Already arrived?
    for (var i = 0; i < state.claimed.length; i++) {
      var h = state.claimed[i];
      if (h.peerId === peerHandle.id && h.kind === kind && !h._msClaimed) {
        h._msClaimed = true;
        return cb(h);
      }
    }
    state.pendingClaims.push({ peerId: peerHandle.id, kind: kind, cb: cb });
  };

  this._registerDataProducer   = function (dp) { state.dataProducersById[dp.id] = dp; };
  this._unregisterDataProducer = function (dp) { delete state.dataProducersById[dp.id]; };
  this._registerDataConsumer   = function (dc) { state.dataConsumersById[dc.id] = dc; };
  this._unregisterDataConsumer = function (dc) { delete state.dataConsumersById[dc.id]; };
  this._dataProducerById       = function (id) { return state.dataProducersById[id] || null; };
  this._forEachDataConsumerOf = function (dataProducerId, fn) {
    for (var id in state.dataConsumersById) {
      var dc = state.dataConsumersById[id];
      if (dc.dataProducerId === dataProducerId) fn(dc);
    }
  };
  // THE FAN-OUT: one message in → every open consumer of that producer,
  // across all transports. Per-consumer guard lives in _forward.
  // Fan-out with SUBCHANNEL ADDRESSING (mediasoup 3.13+). A plain message
  // reaches every consumer of the producer, as it always did. When the
  // sender names subchannels, only consumers subscribed to one of them
  // get it; requiredSubchannel narrows that further and ignoredSubchannel
  // excludes — which together are how an app does per-room or per-role
  // routing over a single DataProducer.
  this._fanOutData = function (dataProducerId, data, opts) {
    opts = opts || {};
    var subs = opts.subchannels, req = opts.requiredSubchannel, ign = opts.ignoredSubchannel;
    for (var id in state.dataConsumersById) {
      var dc = state.dataConsumersById[id];
      if (dc.dataProducerId !== dataProducerId) continue;
      if (subs && subs.length) {
        var hit = false;
        for (var i = 0; i < subs.length; i++) {
          if (dc._wantsSubchannel(subs[i])) { hit = true; break; }
        }
        if (!hit) continue;
      }
      if (req != null && dc.subchannels.indexOf(req | 0) < 0) continue;
      if (ign != null && dc.subchannels.indexOf(ign | 0) >= 0) continue;
      dc._forward(data);
    }
  };
  this.dump = function () {
    return _dumpOf(self, {
      transportIds:     state.transports.map(function (t) { return t.id; }),
      rtpObserverIds:   (state.rtpObservers || state.observers || []).map(function (o) { return o.id; }),
      mapProducerIdConsumerIds: Object.keys(state.producersById).map(function (pid) {
        var ids = [];
        for (var cid in state.consumersById) {
          if (state.consumersById[cid].producerId === pid) ids.push(cid);
        }
        return [pid, ids];
      }),
      mapDataProducerIdDataConsumerIds: Object.keys(state.dataProducersById).map(function (pid) {
        var ids = [];
        for (var cid in state.dataConsumersById) {
          if (state.dataConsumersById[cid].dataProducerId === pid) ids.push(cid);
        }
        return [pid, ids];
      }),
    });
  };

  this._registerProducer   = function (p) { state.producersById[p.id] = p; };
  this._unregisterProducer = function (p) { delete state.producersById[p.id]; };
  this._registerConsumer   = function (c) { state.consumersById[c.id] = c; };
  this._unregisterConsumer = function (c) { delete state.consumersById[c.id]; };
  this._producerById       = function (id) { return state.producersById[id] || null; };
  this._onTransportClosed  = function (t) {
    var i = state.transports.indexOf(t);
    if (i >= 0) state.transports.splice(i, 1);
  };
  this._forEachConsumerOf = function (producerId, fn) {
    for (var id in state.consumersById) {
      var c = state.consumersById[id];
      if (c.producerId === producerId) fn(c);
    }
  };

  /* ── public API ──────────────────────────────────────────────── */

  this.createWebRtcTransport = function (opts) {
    opts = opts || {};
    if (state.closed) return Promise.reject(InvalidStateError('Router closed'));
    var portRange = port_range_of(opts);
    if (portRange.max < portRange.min) {
      return Promise.reject(new TypeError('createWebRtcTransport: maxPort cannot be less than minPort (' +
                                          portRange.min + '-' + portRange.max + ')'));
    }
    return new Promise(function (resolve, reject) {
      var t = new WebRtcTransport({
        router: self,
        webRtcServer: opts.webRtcServer || null,
        portRange: portRange,
        announcedAddresses: announced_from(opts),
        enableSctp: opts.enableSctp,
        numSctpStreams: opts.numSctpStreams,
        appData: opts.appData,
      }, function (err, transport) {
        if (err) return reject(err);
        state.transports.push(transport);
        if (opts.webRtcServer) {
          opts.webRtcServer._attach(transport);
          // …and detached when it closes. Without this the server's
          // transport list grows for the lifetime of the process, its
          // dump() reports transports that are long gone, and the
          // 'webrtctransportunhandled' observer event never fires —
          // mediasoup emits it as the pair of 'webrtctransporthandled'.
          transport.observer.once('close', function () {
            try { opts.webRtcServer._unhandleWebRtcTransport(transport); } catch (e) {}
          });
        }
        observer.emit('newtransport', transport);
        resolve(transport);
      });
      void t;
    });
  };

  // mediasoup's precedence: the first UDP listen info's portRange, else its
  // port, else the transport's port (listenIps carry it beside them), else
  // the Worker's rtcMinPort-rtcMaxPort (default 10000-59999).
  function port_range_of(opts) {
    var info = (opts.listenInfos || []).filter(function (i) { return !i.protocol || i.protocol === 'udp'; })[0] || {};
    var port = info.port || opts.port;
    if (info.portRange && info.portRange.min && info.portRange.max) return info.portRange;
    if (port) return { min: port, max: port };
    return { min: workerSettings.rtcMinPort || 10000, max: workerSettings.rtcMaxPort || 59999 };
  }

  function announced_from(opts) {
    var out = [];
    var infos = opts.listenInfos || opts.listenIps || [];
    infos.forEach(function (i) {
      if (typeof i === 'string') { if (i !== '0.0.0.0' && i !== '::') out.push(i); return; }
      if (i.announcedAddress) out.push(i.announcedAddress);
      else if (i.announcedIp) out.push(i.announcedIp);
      else if (i.ip && i.ip !== '0.0.0.0' && i.ip !== '::') out.push(i.ip);
    });
    return out;
  }

  this.canConsume = function (opts) {
    opts = opts || {};
    var p = state.producersById[opts.producerId];
    if (!p) return false;
    return ortc_can_consume(p.rtpParameters, opts.rtpCapabilities);
  };

  this.updateMediaCodecs = function (mediaCodecs) {
    state.caps = filterCapabilitiesByMediaCodecs(fullCaps, mediaCodecs);
    return true;
  };

  // PlainTransport: plain RTP/RTCP over UDP — no ICE, no DTLS, no
  // negotiation. This is the door to ffmpeg and GStreamer, and through
  // them to recording and RTMP: mediasoup itself records nothing and
  // speaks no RTMP; it hands an external process an RTP stream, and the
  // external process does the rest.
  //
  // Egress (router → ffmpeg) is implemented: consume() picks the
  // outgoing ssrc/pt the app writes into the SDP it feeds ffmpeg, and
  // the engine treats the socket as a peer, so producer churn, layer
  // switching and keyframe debt apply unchanged. Ingress (ffmpeg →
  // router) is not implemented yet and says so loudly rather than
  // accepting a produce() that would silently carry nothing.
  this.createPlainTransport = function (options) {
    return createPlainLike(options, 'plain');
  };
  this.createPipeTransport = function (options) {
    return createPlainLike(options, 'pipe');
  };

  /* ── pipeToRouter ─────────────────────────────────────────────
   * Make a producer that lives on THIS router consumable on ANOTHER
   * one. That is what turns a single-process SFU into a scalable one:
   * routers can sit in different workers (different processes, in
   * mediasoup's case) with participants split between them, and a
   * participant on router B still sees a participant on router A.
   *
   * The mechanism is a pair of PipeTransports facing each other over
   * UDP. This router consumes the producer into its side of the pipe;
   * the far router produces the arriving RTP as a local producer. Both
   * halves already exist — plain egress and plain ingress — so this is
   * the wiring, not new machinery.
   *
   * The transport PAIR is cached per destination router: a room with
   * twenty producers must not open twenty socket pairs, and mediasoup
   * caches for exactly the same reason.
   */
  this.pipeToRouter = function (options) {
    options = options || {};
    var target = options.router;
    if (!target) return Promise.reject(new TypeError('pipeToRouter: router is required'));
    if (target === self) {
      return Promise.reject(new TypeError('pipeToRouter: cannot pipe to self'));
    }
    if (options.dataProducerId) {
      return Promise.reject(UnsupportedError(
        'pipeToRouter: SCTP piping (dataProducerId) is not implemented'));
    }
    if (!options.producerId) {
      return Promise.reject(new TypeError('pipeToRouter: producerId is required'));
    }
    var producer = self._producerById(options.producerId);
    if (!producer) {
      return Promise.reject(NotFoundError('Producer not found: ' + options.producerId));
    }

    return pipePairFor(target, options).then(function (pair) {
      // Consume the producer INTO the pipe…
      return pair.local.consume({
        producerId:      producer.id,
        rtpCapabilities: target.rtpCapabilities,
      }).then(function (pipeConsumer) {
        // …and produce what arrives on the far side. The consumer's own
        // rtpParameters describe exactly what will be on the wire, so
        // they are what the far router is told to expect.
        var rp = pipeConsumer.rtpParameters;
        return pair.remote.produce({
          // keepId lets an app address the piped producer by the same id
          // on both routers, which is what makes a room's producer list
          // meaningful across a split.
          id:            options.keepId ? producer.id : undefined,
          kind:          producer.kind,
          rtpParameters: rp,
          paused:        producer.paused,
          appData:       producer.appData,
        }).then(function (pipeProducer) {
          // Lifetime follows the source: when the original producer
          // goes, so do both ends. Without this a closed camera leaves
          // a producer alive on the far router forever.
          producer.observer.once('close', function () {
            try { pipeConsumer.close(); } catch (e) {}
            try { pipeProducer.close(); } catch (e) {}
          });
          // Pause state follows too, so a muted producer stays muted
          // across the pipe.
          producer.observer.on('pause',  function () { pipeProducer.pause(); });
          producer.observer.on('resume', function () { pipeProducer.resume(); });
          return { pipeConsumer: pipeConsumer, pipeProducer: pipeProducer };
        });
      });
    });
  };

  // One transport pair per destination router, created on first use.
  function pipePairFor(target, options) {
    var key = target.id;
    if (state.pipePairs[key]) return state.pipePairs[key];

    var listen = options.listenInfo || options.listenIp || { ip: '127.0.0.1' };
    var mk = function (router) {
      return router.createPipeTransport({
        listenInfo: listen,
        enableSctp: false,          // SCTP piping is refused above
      });
    };

    var p = Promise.all([mk(self), mk(target)]).then(function (t) {
      var local = t[0], remote = t[1];
      // Each side sends to the other's tuple. Both are ours, so there is
      // no negotiation and no ICE — that is the whole point of a pipe.
      return Promise.all([
        local.connect({ ip: remote.tuple.localIp, port: remote.tuple.localPort }),
        remote.connect({ ip: local.tuple.localIp, port: local.tuple.localPort }),
      ]).then(function () {
        _diag('pipe established ' + self.id.slice(0, 8) + ' ↔ ' + target.id.slice(0, 8) +
              ' (' + local.tuple.localPort + ' ↔ ' + remote.tuple.localPort + ')');
        return { local: local, remote: remote };
      });
    });

    state.pipePairs[key] = p;
    return p;
  }

  function createPlainLike(options, kindOfTransport) {
    options = options || {};
    if (state.closed) return Promise.reject(InvalidStateError('router closed'));

    var listen = options.listenInfo || options.listenIp || {};
    var ip = (typeof listen === 'string') ? listen : (listen.ip || '0.0.0.0');
    var announced = (typeof listen === 'object' &&
                     (listen.announcedAddress || listen.announcedIp)) || undefined;
    var wantPort = (typeof listen === 'object' && listen.port) || 0;
    var rtcpMux = options.rtcpMux !== false;      // mediasoup default: true
    var comedia = !!options.comedia;

    if (options.enableSrtp) {
      return Promise.reject(UnsupportedError(
        kindOfTransport + 'Transport: enableSrtp is not implemented (plain RTP only)'));
    }

    var tev = new EventEmitter(), tobs = new EventEmitter();
    var tstate = {
      id: uuid(), closed: false, appData: options.appData || {},
      consumers: [], producers: [], sctpState: 'new',
      remote: null, remoteRtcp: null,
      tuple: undefined, rtcpTuple: undefined,
      nextSsrc: (Math.random() * 0xfffffff) >>> 0,
    };

    var socket = dgram.createSocket(ip.indexOf(':') >= 0 ? 'udp6' : 'udp4');
    var rtcpSocket = rtcpMux ? null
                             : dgram.createSocket(ip.indexOf(':') >= 0 ? 'udp6' : 'udp4');

    function tupleOf(sock, remote) {
      var a = sock.address();
      return {
        localIp:    announced || a.address,
        localPort:  a.port,
        remoteIp:   remote ? remote.ip : undefined,
        remotePort: remote ? remote.port : undefined,
        protocol:   'udp',
      };
    }

    return new Promise(function (resolve, reject) {
      // EVERY path settles. The rtcp socket's bind had no error handler
      // at all: a port conflict or a permission error on it left this
      // promise pending forever, and a createPlainTransport() that never
      // returns is indistinguishable from a hung server — the exact
      // failure mode that cost a day to find elsewhere in this stack.
      var settled = false;
      var fail = function (err) {
        if (settled) return;
        settled = true;
        try { socket.close(); } catch (e) {}
        if (rtcpSocket) { try { rtcpSocket.close(); } catch (e) {} }
        reject(err);
      };
      var done = function (t) { if (!settled) { settled = true; resolve(t); } };

      socket.once('error', fail);
      if (rtcpSocket) rtcpSocket.once('error', fail);

      socket.bind(wantPort, ip === '0.0.0.0' || ip === '::' ? undefined : ip, function () {
        var finish = function () {
          socket.removeListener('error', fail);
          if (rtcpSocket) rtcpSocket.removeListener('error', fail);
          tstate.tuple = tupleOf(socket, null);
          if (rtcpSocket) tstate.rtcpTuple = tupleOf(rtcpSocket, null);
          // The router can close while the socket is still binding — a
          // server tearing down does not wait for a transport somebody
          // asked for a millisecond earlier. buildPlainTransport()
          // registers with the engine and THROWS if the router is gone,
          // and this callback has no caller to catch it: the throw
          // escapes into node's uncaught-exception path. Reject the
          // pending promise instead, which is what the caller can
          // actually handle.
          if (state.closed) {
            return fail(InvalidStateError('router closed while the transport was being created'));
          }
          var built;
          try { built = buildPlainTransport(); }
          catch (e) { return fail(e); }
          done(built);
        };
        if (rtcpSocket) rtcpSocket.bind(0, undefined, finish);
        else finish();
      });
    });

    function buildPlainTransport() {
      // COMEDIA: learn the peer's address from the first packet it sends
      // instead of being told. It is how you point ffmpeg at mediasoup
      // without knowing which port ffmpeg will source from.
      socket.on('message', function (msg, rinfo) {
        if (!tstate.remote && comedia) {
          tstate.remote = { ip: rinfo.address, port: rinfo.port };
          tstate.tuple = tupleOf(socket, tstate.remote);
          _diag('plainTransport ' + tstate.id.slice(0, 8) +
                ' comedia: learned remote ' + rinfo.address + ':' + rinfo.port);
          tobs.emit('tuple', tstate.tuple);
          tev.emit('tuple', tstate.tuple);
        }
      });

      var send = function (buf) {
        var r = tstate.remote;
        if (!r || tstate.closed) return;
        socket.send(buf, 0, buf.length, r.port, r.ip);
      };

      var plainPeer = state.sfuRouter.addPlainPeer({ sendRtp: send });

      var pt = {
        get id() { return tstate.id; },
        get closed() { return tstate.closed; },
        get type() { return kindOfTransport; },
        get observer() { return tobs; },
        get tuple() { return tstate.tuple; },
        get rtcpTuple() { return tstate.rtcpTuple; },
        get srtpParameters() { return undefined; },   // enableSrtp is refused above
        get sctpParameters() { return undefined; },
        get sctpState() { return tstate.sctpState; },
        get sctpNegotiatedCapabilities() { return undefined; },
        get appData() { return tstate.appData; },
        set appData(v) { tstate.appData = v; },
        _router: self,

        connect: function (opts2) {
          if (tstate.closed) return Promise.reject(InvalidStateError('transport closed'));
          // `connect()` with nothing at all silently resolved, leaving a
          // transport that looks connected and has no remote.
          if (!opts2 && !comedia) {
            return Promise.reject(new TypeError('connect: ip and port are required'));
          }
          opts2 = opts2 || {};
          if (opts2.srtpParameters) {
            return Promise.reject(UnsupportedError('PlainTransport: SRTP is not implemented'));
          }
          if (!comedia) {
            if (!opts2.ip || opts2.port == null) {
              return Promise.reject(new TypeError('connect: ip and port are required'));
            }
            // A non-numeric port produced a transport whose tuple read
            // `undefined` and whose packets went nowhere — connected by
            // every appearance, silent in practice.
            if (!Number.isInteger(opts2.port) || opts2.port < 1 || opts2.port > 65535) {
              return Promise.reject(new TypeError(
                'connect: port must be an integer in 1..65535, got ' + opts2.port));
            }
            if (opts2.rtcpPort != null &&
                (!Number.isInteger(opts2.rtcpPort) || opts2.rtcpPort < 1 || opts2.rtcpPort > 65535)) {
              return Promise.reject(new TypeError(
                'connect: rtcpPort must be an integer in 1..65535, got ' + opts2.rtcpPort));
            }
            tstate.remote = { ip: opts2.ip, port: opts2.port };
            tstate.tuple = tupleOf(socket, tstate.remote);
            if (opts2.rtcpPort != null && rtcpSocket) {
              tstate.remoteRtcp = { ip: opts2.ip, port: opts2.rtcpPort };
              tstate.rtcpTuple = tupleOf(rtcpSocket, tstate.remoteRtcp);
            }
          }
          return Promise.resolve();
        },

        consume: function (opts2) {
          opts2 = opts2 || {};
          if (tstate.closed) return Promise.reject(InvalidStateError('transport closed'));
          var producer = self._producerById(opts2.producerId);
          if (!producer) {
            return Promise.reject(NotFoundError('Producer not found: ' + opts2.producerId));
          }
          var sfuProducer = producer._sfuHandle();
          if (!sfuProducer) {
            return Promise.reject(InvalidStateError(
              'producer ' + opts2.producerId + ' has no media yet'));
          }
          // The outgoing identity is ours to choose, and the app needs it
          // verbatim for the SDP it hands the external process.
          var ssrc = (tstate.nextSsrc = (tstate.nextSsrc + 1) >>> 0);
          var srcCodec = (producer.rtpParameters.codecs || [])[0] || {};
          var payloadType = srcCodec.payloadType;

          var consumer = null, pendingHandle = null;
          plainPeer.consume(sfuProducer, function (err, handle) {
            if (err) { _diag('plain consume failed: ' + err.message); return; }
            if (consumer && !consumer.closed) consumer._setSfuHandle(handle);
            else pendingHandle = handle;
          }, { ssrc: ssrc, pt: payloadType });

          var rtpParameters = getConsumerRtpParameters({
            producerRtpParameters: producer.rtpParameters,
            consumerCaps:          opts2.rtpCapabilities || self.rtpCapabilities,
            kind:                  producer.kind,
            ssrc:                  ssrc,
            mid:                   undefined,
          });

          consumer = new Consumer({
            id: uuid(), producerId: producer.id, kind: producer.kind,
            rtpParameters: rtpParameters, transport: pt,
            paused: !!opts2.paused, producerPaused: producer.paused,
            appData: opts2.appData,
          });
          if (pendingHandle) consumer._setSfuHandle(pendingHandle);
          // ConsumerOptions.preferredLayers applies here exactly as it does
          // on a WebRtcTransport — the option belongs to the Consumer, not
          // to the kind of transport that created it.
          if (opts2.preferredLayers) {
            try { consumer.setPreferredLayers(opts2.preferredLayers); } catch (e) {}
          }
          tstate.consumers.push(consumer);
          self._registerConsumer(consumer);
          tobs.emit('newconsumer', consumer);
          _diag('plainTransport ' + tstate.id.slice(0, 8) + ' consumer ssrc=' + ssrc +
                ' pt=' + payloadType + ' → ' +
                (tstate.remote ? tstate.remote.ip + ':' + tstate.remote.port : 'comedia (pending)'));
          return Promise.resolve(consumer);
        },

        // INGRESS: ffmpeg (or any RTP sender) pushes into the room.
        // mediasoup's contract is that the app declares what it is about
        // to send — kind and rtpParameters, the same values it puts in
        // the SDP it hands the external process — and the transport then
        // routes whatever arrives on that ssrc. There is no negotiation
        // to discover it from, which is exactly why the declaration is
        // mandatory here and optional on a WebRtcTransport.
        produce: function (opts2) {
          opts2 = opts2 || {};
          if (tstate.closed) return Promise.reject(InvalidStateError('transport closed'));
          var rp = opts2.rtpParameters;
          if (!opts2.kind || !rp) {
            return Promise.reject(new TypeError('produce: kind and rtpParameters are required'));
          }
          var enc = (rp.encodings && rp.encodings[0]) || {};
          if (enc.ssrc == null) {
            return Promise.reject(new TypeError(
              'produce: rtpParameters.encodings[0].ssrc is required on a PlainTransport ' +
              '(there is no negotiation to learn it from)'));
          }
          // An ssrc is an unsigned 32-bit integer. A negative or
          // fractional one used to be accepted and then never matched a
          // single packet — the producer existed, reported healthy, and
          // silently carried nothing. Reject at the door instead.
          if (!Number.isInteger(enc.ssrc) || enc.ssrc < 0 || enc.ssrc > 0xffffffff) {
            return Promise.reject(new TypeError(
              'produce: ssrc must be an unsigned 32-bit integer, got ' + enc.ssrc));
          }
          if ((rp.encodings || []).length > 1) {
            warn_once('plainSimulcast',
              'PlainTransport.produce(): only the first encoding is ingested — ' +
              'simulcast over plain RTP is not supported.');
          }
          var codec = (rp.codecs || [])[0] || {};
          var codecName = String(codec.mimeType || '').split('/')[1] || '';
          var ssrc = enc.ssrc >>> 0;

          var engineHandle;
          try {
            engineHandle = state.sfuRouter.addPlainProducer({
              peerHandle: plainPeer, kind: opts2.kind,
              ssrc: ssrc, codecName: codecName,
            });
          } catch (e) { return Promise.reject(e); }

          // Route arriving packets for this ssrc into the engine. The
          // socket carries everything the sender emits, so the ssrc is
          // what separates one producer from another on the same port.
          var onPacket = function (msg) {
            if (tstate.closed || msg.length < 12) return;
            if (((msg[0] & 0xc0) >> 6) !== 2) return;             // not RTP version 2
            var pt = msg[1] & 0x7f;
            if (pt >= 72 && pt <= 76) return;                      // RTCP, not media
            if (msg.readUInt32BE(8) >>> 0 !== ssrc) return;        // another producer's stream
            engineHandle.push(msg, { ssrc: ssrc, payloadType: pt,
                                     sequenceNumber: msg.readUInt16BE(2) });
          };
          socket.on('message', onPacket);

          var producer = new Producer({
            id: opts2.id || uuid(), kind: opts2.kind, rtpParameters: rp,
            transport: pt, paused: !!opts2.paused, appData: opts2.appData,
          });
          producer._kfDelay = Number(opts2.keyFrameRequestDelay) || 0;
          producer._setSfuHandle(engineHandle);
          tstate.producers.push(producer);
          self._registerProducer(producer);
          producer.observer.once('close', function () {
            try { socket.removeListener('message', onPacket); } catch (e) {}
          });
          tobs.emit('newproducer', producer);
          _diag('plainTransport ' + tstate.id.slice(0, 8) + ' ingest producer ssrc=' +
                ssrc + ' kind=' + opts2.kind + ' codec=' + (codecName || '?'));
          return Promise.resolve(producer);
        },
        produceData: function () {
          return Promise.reject(UnsupportedError(kindOfTransport + 'Transport: SCTP is not implemented'));
        },
        consumeData: function () {
          return Promise.reject(UnsupportedError(kindOfTransport + 'Transport: SCTP is not implemented'));
        },

        getStats: function () {
          return Promise.resolve([{
            type: kindOfTransport + '-transport', transportId: tstate.id,
            timestamp: Date.now(), tuple: tstate.tuple, rtcpTuple: tstate.rtcpTuple,
          }]);
        },
        enableTraceEvent: function () { return Promise.resolve(); },
        setMaxIncomingBitrate: function () { return Promise.resolve(); },
        setMaxOutgoingBitrate: function () { return Promise.resolve(); },
        dump: function () {
          return _dumpOf(pt, {
            direct: false, tuple: tstate.tuple, rtcpTuple: tstate.rtcpTuple,
            consumerIds: tstate.consumers.map(function (c) { return c.id; }),
            producerIds: tstate.producers.map(function (p) { return p.id; }),
            dataProducerIds: [], dataConsumerIds: [],
          });
        },
        close: function () {
          if (tstate.closed) return;
          tstate.closed = true;
          tstate.consumers.slice().forEach(function (c) {
            self._unregisterConsumer(c);
            try { c._transportClosed(); } catch (e) {}
          });
          tstate.producers.slice().forEach(function (pr) {
            self._forEachConsumerOf(pr.id, function (c2) { c2._producerClosed(); });
            self._unregisterProducer(pr);
            try { pr._transportClosed(); } catch (e) {}
          });
          try { plainPeer.close(); } catch (e) {}
          try { socket.close(); } catch (e) {}
          if (rtcpSocket) { try { rtcpSocket.close(); } catch (e) {} }
          tobs.emit('close');
          tev.emit('close');
        },
        routerClosed: function () { pt.close(); },
        listenServerClosed: function () { pt.close(); },
        _routerClosed: function () { pt.close(); },
        _onConsumerClosed: function (c) {
          var i = tstate.consumers.indexOf(c);
          if (i >= 0) tstate.consumers.splice(i, 1);
          self._unregisterConsumer(c);
        },
        _onProducerClosed: function (pr) {
          var i = tstate.producers.indexOf(pr);
          if (i >= 0) tstate.producers.splice(i, 1);
          self._forEachConsumerOf(pr.id, function (c2) { c2._producerClosed(); });
          self._unregisterProducer(pr);
        },
        on:   function (n, f) { tev.on(n, f);   return pt; },
        off:  function (n, f) { tev.off(n, f);  return pt; },
        once: function (n, f) { tev.once(n, f); return pt; },
      };
      state.transports.push(pt);
      observer.emit('newtransport', pt);
      return pt;
    }
  }
  // DirectTransport (round 3.5, data-only): the official demo creates
  // one at ROOM CREATION for its chat bot — a reject here prevents any
  // room from existing (the same disproportion as the observer lesson,
  // decision 10). Data through it is pure in-process fan-out, which the
  // Router already does; RTP injection stays loudly unsupported.
  this.createDirectTransport = function (options) {
    options = options || {};
    var tev = new EventEmitter(), tobs = new EventEmitter();
    var tstate = { id: uuid(), closed: false, appData: options.appData || {},
                   dataProducers: [], dataConsumers: [] };
    var dt = {
      get id() { return tstate.id; },
      get closed() { return tstate.closed; },
      get type() { return 'direct'; },
      get observer() { return tobs; },
      get appData() { return tstate.appData; },
      set appData(v) { tstate.appData = v; },
      produce:  function () { return Promise.reject(UnsupportedError('DirectTransport RTP injection not implemented')); },
      consume:  function () { return Promise.reject(UnsupportedError('DirectTransport RTP consumption not implemented')); },
      produceData: function (opts2) {
        opts2 = opts2 || {};
        if (tstate.closed) return Promise.reject(InvalidStateError('transport closed'));
        var dp = new DataProducer({
          id: uuid(), label: opts2.label, protocol: opts2.protocol,
          sctpStreamParameters: undefined,   // direct: no SCTP stream
          transport: dt, channel: null, appData: opts2.appData,
        });
        tstate.dataProducers.push(dp);
        self._registerDataProducer(dp);
        tobs.emit('newdataproducer', dp);
        return Promise.resolve(dp);
      },
      consumeData: function (opts2) {
        opts2 = opts2 || {};
        if (tstate.closed) return Promise.reject(InvalidStateError('transport closed'));
        var src = self._dataProducerById(opts2.dataProducerId);
        if (!src) return Promise.reject(NotFoundError('DataProducer not found: ' + opts2.dataProducerId));
        var dc = new DataConsumer({
          id: uuid(), dataProducerId: src.id, label: src.label, protocol: src.protocol,
          sctpStreamParameters: undefined,
          transport: dt, channel: null, appData: opts2.appData,
        });
        tstate.dataConsumers.push(dc);
        self._registerDataConsumer(dc);
        tobs.emit('newdataconsumer', dc);
        return Promise.resolve(dc);
      },
      getStats: function () { return Promise.resolve([{ type: 'direct-transport', transportId: tstate.id, timestamp: Date.now() }]); },
      // RTCP injection belongs to the RTP side of DirectTransport, which
      // stays unimplemented — a silent no-op here would let an app believe
      // its feedback reached the peer.
      sendRtcp: function () { throw UnsupportedError('DirectTransport.sendRtcp not implemented (RTP side unsupported)'); },
      enableTraceEvent: function () { return Promise.resolve(); },
      setMaxIncomingBitrate: function () { return Promise.resolve(); },
      close: function () {
        if (tstate.closed) return;
        tstate.closed = true;
        tstate.dataProducers.slice().forEach(function (dp) {
          self._forEachDataConsumerOf(dp.id, function (dc2) { dc2._dataProducerClosed(); });
          self._unregisterDataProducer(dp); dp._transportClosed();
        });
        tstate.dataConsumers.slice().forEach(function (dc2) {
          self._unregisterDataConsumer(dc2); dc2._transportClosed();
        });
        tobs.emit('close');
      },
      _onDataProducerClosed: function (dp) {
        var i = tstate.dataProducers.indexOf(dp);
        if (i >= 0) tstate.dataProducers.splice(i, 1);
        self._forEachDataConsumerOf(dp.id, function (dc2) { dc2._dataProducerClosed(); });
        self._unregisterDataProducer(dp);
      },
      _onDataConsumerClosed: function (dc2) {
        var i = tstate.dataConsumers.indexOf(dc2);
        if (i >= 0) tstate.dataConsumers.splice(i, 1);
        self._unregisterDataConsumer(dc2);
      },
      _router: self,
      on:   function (n, f) { tev.on(n, f);   return dt; },
      off:  function (n, f) { tev.off(n, f);  return dt; },
      once: function (n, f) { tev.once(n, f); return dt; },
      _routerClosed: function () { dt.close(); },
    };
    state.transports.push(dt);
    observer.emit('newtransport', dt);
    return Promise.resolve(dt);
  };
  // REAL observers (round 2): the RFC 6464 levels were in the stack all
  // along (cached per SSRC, readable via getSynchronizationSources) —
  // this wires them to mediasoup's volumes/silence/dominantspeaker
  // events. addProducer({producerId}) as their API prescribes.
  this.createAudioLevelObserver = function (options) {
    var o = new RtpObserver('audiolevel', options, self);
    observer.emit('newrtpobserver', o);
    return Promise.resolve(o);
  };
  this.createActiveSpeakerObserver = function (options) {
    var o = new RtpObserver('activespeaker', options, self);
    observer.emit('newrtpobserver', o);
    return Promise.resolve(o);
  };

  this.close = function () {
    if (state.closed) return;
    state.closed = true;
    state.transports.slice().forEach(function (t) { try { t._routerClosed(); } catch (e) {} });
    try { state.sfuRouter.close(); } catch (e) {}
    observer.emit('close');
  };

  this._workerClosed = function () {
    if (state.closed) return;
    self.close();
    ev.emit('workerclose');
  };

  this.on   = function (n, f) { ev.on(n, f);   return self; };
  this.off  = function (n, f) { ev.off(n, f);  return self; };
  this.once = function (n, f) { ev.once(n, f); return self; };
}


/* ═══════════════════════════════════════════════════════════════════
 *  Worker
 * ═══════════════════════════════════════════════════════════════════ */

function Worker(settings) {
  var self = this;
  var ev = new EventEmitter();
  var observer = new EventEmitter();

  var state = {
    closed: false, routers: [], servers: [],
    appData: (settings && settings.appData) || {},
    settings: settings || {},
  };

  // No C++ subprocess here — the "worker" is this Node process. pid is
  // reported honestly; 'died' can never fire, and subprocessClosed
  // simply mirrors closed.
  Object.defineProperty(this, 'pid',              { get: function () { return process.pid; } });
  Object.defineProperty(this, 'closed',           { get: function () { return state.closed; } });
  Object.defineProperty(this, 'died',             { get: function () { return false; } });
  Object.defineProperty(this, 'subprocessClosed', { get: function () { return state.closed; } });
  Object.defineProperty(this, 'observer',         { get: function () { return observer; } });
  Object.defineProperty(this, 'appData', {
    get: function () { return state.appData; },
    set: function (v) { state.appData = v; },
  });

  this.createRouter = function (options) {
    if (state.closed) return Promise.reject(WorkerClosedError('Worker closed'));
    var r = new Router(options || {}, state.settings);
    state.routers.push(r);
    observer.emit('newrouter', r);
    return Promise.resolve(r);
  };

  this.createWebRtcServer = function (options) {
    if (state.closed) return Promise.reject(WorkerClosedError('Worker closed'));
    return new Promise(function (resolve, reject) {
      var s = new WebRtcServer(options || {}, function (err, server) {
        if (err) return reject(err);
        state.servers.push(server);
        observer.emit('newwebrtcserver', server);
        resolve(server);
      });
      void s;
    });
  };

  this.getResourceUsage = function () {
    var u = process.resourceUsage ? process.resourceUsage() : {};
    return Promise.resolve({
      ru_utime: Math.round((u.userCPUTime || 0) / 1000),
      ru_stime: Math.round((u.systemCPUTime || 0) / 1000),
      ru_maxrss: u.maxRSS || 0,
      ru_minflt: u.minorPageFault || 0,
      ru_majflt: u.majorPageFault || 0,
    });
  };

  this.dump = function () {
    return _dumpOf({ id: 'worker' }, {
      pid: process.pid,
      webRtcServerIds: state.servers.map(function (w) { return w.id; }),
      routerIds: state.routers.map(function (r) { return r.id; }),
      channelMessageHandlers: { channelRequestHandlers: [], channelNotificationHandlers: [] },
    });
  };

  this.updateSettings = function (s) {
    Object.assign(state.settings, s || {});
    return Promise.resolve();
  };

  this.close = function () {
    if (state.closed) return;
    state.closed = true;
    state.routers.slice().forEach(function (r) { try { r._workerClosed(); } catch (e) {} });
    state.servers.slice().forEach(function (s) { try { s.close(); } catch (e) {} });
    observer.emit('close');
    ev.emit('subprocessclose');
  };

  this.on   = function (n, f) { ev.on(n, f);   return self; };
  this.off  = function (n, f) { ev.off(n, f);  return self; };
  this.once = function (n, f) { ev.once(n, f); return self; };
}


/* ═══════════════════════════════════════════════════════════════════
 *  Module surface — mirrors `import * as mediasoup from "mediasoup"`
 * ═══════════════════════════════════════════════════════════════════ */

export var observer = new EventEmitter();

// mediasoup exposes these at module level; apps import them directly.
//   errors  — the error classes, so `instanceof` works across a boundary
//   ortc    — the codec/capability helpers (matching/negotiation)
//   extras  — miscellaneous helpers
//   setLogEventListeners — routes the library's own logs into an app logger
export const errors = {
  UnsupportedError: UnsupportedError,
  InvalidStateError: InvalidStateError,
  NotFoundError: NotFoundError,
  TypeError: TypeError,
};

// Lazy getters: these helpers are declared further down the module, and a
// const initialiser would capture them before their definitions evaluate.
export const ortc = {
  get getSupportedRtpCapabilities() { return getSupportedRtpCapabilities; },
  get getConsumerRtpParameters()    { return getConsumerRtpParameters; },
  get canConsume()                  { return ortc_can_consume; },
  get parseScalabilityMode()        { return parseScalabilityMode; },
};

export const extras = {
  get parseScalabilityMode() { return parseScalabilityMode; },
};

// mediasoup 3.x lets an application capture the library's own logging
// instead of having it go to debug(). Ours is quieter, but the hook has
// to exist or an app that installs a logger silently loses it.
var _logListeners = null;
export function setLogEventListeners(listeners) {
  _logListeners = listeners || null;
}
export function _emitLog(level, ns, msg) {
  if (!_logListeners) return;
  var fn = _logListeners['on' + level.charAt(0).toUpperCase() + level.slice(1)];
  if (typeof fn === 'function') { try { fn(ns, msg); } catch (e) {} }
}

export function createWorker(settings) {
  var w = new Worker(settings || {});
  observer.emit('newworker', w);
  return Promise.resolve(w);
}

export function getSupportedRtpCapabilities() {
  return getRouterRtpCapabilities();
}

export function parseScalabilityMode(mode) {
  var m = /^[LS]([1-9]\d*)T([1-9]\d*)/.exec(String(mode || ''));
  return {
    spatialLayers:  m ? parseInt(m[1], 10) : 1,
    temporalLayers: m ? parseInt(m[2], 10) : 1,
  };
}

export var version = '0.0.1-sfu-server';
export var workerBin = null;   // pure JS — there is no worker binary

export { Worker, Router, WebRtcServer, WebRtcTransport, Producer, Consumer };