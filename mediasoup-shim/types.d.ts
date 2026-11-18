/**
 * 'mediasoup/types' — declarations that make an UNMODIFIED strict-mode
 * mediasoup application BUILD against the sfu-server drop-in.
 *
 * THE ONE RULE THAT MATTERS (learned by running tsc, not by assuming):
 * calling `.on()` on a value typed `any` does NOT contextually type the
 * callback's parameters — noImplicitAny still flags them. Parameters
 * are only quiet when the RECEIVER has a real `on(event, (...args:
 * any[]) => void)` signature; then each param is CONTEXTUALLY any,
 * which is legal. So every entity here is an Emitter interface, and
 * every entity-returning method is typed to return one — that keeps the
 * demo's unannotated chains (`const producer = await transport.
 * produce(...)`) typed all the way down. Everything else rides an index
 * signature.
 */

export interface EnhancedEventEmitter {
  on(event: string, listener: (...args: any[]) => void): this;
  once(event: string, listener: (...args: any[]) => void): this;
  off(event: string, listener?: (...args: any[]) => void): this;
  addListener(event: string, listener: (...args: any[]) => void): this;
  removeListener(event: string, listener?: (...args: any[]) => void): this;
  removeAllListeners(event?: string): this;
  emit(event: string, ...args: any[]): boolean;
  [key: string]: any;
}

export interface Producer      extends EnhancedEventEmitter { observer: EnhancedEventEmitter; }
export interface Consumer      extends EnhancedEventEmitter { observer: EnhancedEventEmitter; }
export interface DataProducer  extends EnhancedEventEmitter { observer: EnhancedEventEmitter; }
export interface DataConsumer  extends EnhancedEventEmitter { observer: EnhancedEventEmitter; }
export interface AudioLevelObserver    extends EnhancedEventEmitter { observer: EnhancedEventEmitter; }
export interface ActiveSpeakerObserver extends EnhancedEventEmitter { observer: EnhancedEventEmitter; }
export interface WebRtcServer          extends EnhancedEventEmitter { observer: EnhancedEventEmitter; }

export interface Transport extends EnhancedEventEmitter {
  observer: EnhancedEventEmitter;
  produce(options?: any): Promise<Producer>;
  consume(options?: any): Promise<Consumer>;
  produceData(options?: any): Promise<DataProducer>;
  consumeData(options?: any): Promise<DataConsumer>;
}
export interface WebRtcTransport extends Transport {}
export interface PlainTransport  extends Transport {}
export interface PipeTransport   extends Transport {}
export interface DirectTransport extends Transport {}

export interface Router extends EnhancedEventEmitter {
  observer: EnhancedEventEmitter;
  createWebRtcTransport(options?: any): Promise<WebRtcTransport>;
  createPlainTransport(options?: any): Promise<PlainTransport>;
  createPipeTransport(options?: any): Promise<PipeTransport>;
  createDirectTransport(options?: any): Promise<DirectTransport>;
  createAudioLevelObserver(options?: any): Promise<AudioLevelObserver>;
  createActiveSpeakerObserver(options?: any): Promise<ActiveSpeakerObserver>;
}

export interface Worker extends EnhancedEventEmitter {
  observer: EnhancedEventEmitter;
  createRouter(options?: any): Promise<Router>;
  createWebRtcServer(options?: any): Promise<WebRtcServer>;
}

/* ── plain data shapes: any is exactly right ── */
type A = any;
export type WorkerSettings = A; export type WorkerUpdateableSettings = A;
export type WorkerLogLevel = A; export type WorkerLogTag = A; export type WorkerResourceUsage = A;
export type WorkerEvents = A; export type WorkerObserver = A; export type WorkerObserverEvents = A;
export type RouterOptions = A; export type RouterEvents = A;
export type RouterObserver = A; export type RouterObserverEvents = A;
export type PipeToRouterOptions = A; export type PipeToRouterResult = A;
export type TransportListenInfo = A; export type TransportListenIp = A;
export type TransportTuple = A; export type TransportTraceEventData = A; export type TransportEvents = A;
export type TransportObserver = A; export type TransportObserverEvents = A;
export type WebRtcTransportOptions = A; export type WebRtcTransportEvents = A;
export type WebRtcServerOptions = A; export type WebRtcServerEvents = A;
export type PlainTransportOptions = A; export type PipeTransportOptions = A;
export type DirectTransportOptions = A;
export type IceParameters = A; export type IceCandidate = A; export type IceState = A; export type IceRole = A;
export type DtlsParameters = A; export type DtlsState = A; export type DtlsRole = A; export type DtlsFingerprint = A;
export type SctpParameters = A; export type SctpState = A; export type SctpCapabilities = A;
export type NumSctpStreams = A; export type SctpStreamParameters = A;
export type ProducerOptions = A; export type ProducerType = A;
export type ProducerScore = A; export type ProducerStat = A; export type ProducerTraceEventData = A;
export type ProducerEvents = A; export type ProducerObserver = A; export type ProducerObserverEvents = A;
export type ProducerCodecOptions = A;
export type ConsumerOptions = A; export type ConsumerType = A;
export type ConsumerScore = A; export type ConsumerStat = A; export type ConsumerLayers = A;
export type ConsumerTraceEventData = A; export type ConsumerEvents = A;
export type ConsumerObserver = A; export type ConsumerObserverEvents = A;
export type DataProducerOptions = A; export type DataProducerType = A;
export type DataProducerStat = A; export type DataProducerEvents = A; export type DataProducerObserver = A;
export type DataConsumerOptions = A; export type DataConsumerType = A;
export type DataConsumerStat = A; export type DataConsumerEvents = A; export type DataConsumerObserver = A;
export type RtpCapabilities = A; export type RtpCodecCapability = A; export type RtpCodecParameters = A;
export type RtpParameters = A; export type RtpEncodingParameters = A; export type RtpHeaderExtension = A;
export type RtpHeaderExtensionParameters = A; export type RtpHeaderExtensionUri = A;
export type RtcpFeedback = A; export type RtcpParameters = A; export type MediaKind = A;
export type ScalabilityMode = A;
export type RtpObserver = A; export type RtpObserverEvents = A; export type RtpObserverObserver = A;
export type AudioLevelObserverOptions = A; export type AudioLevelObserverVolume = A;
export type AudioLevelObserverEvents = A;
export type ActiveSpeakerObserverOptions = A; export type ActiveSpeakerObserverDominantSpeaker = A;
export type ActiveSpeakerObserverEvents = A;
export type AppData = A; export type BweTraceInfo = A; export type SrtpParameters = A;
export type SrtpCryptoSuite = A;

export declare const UnsupportedError: A; export declare const NotFoundError: A;
export declare const InvalidStateError: A; export declare const WorkerClosedError: A;
