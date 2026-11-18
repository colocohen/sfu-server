/** Declarations for 'mediasoup' (the sfu-server drop-in). See ./types.d.ts
 *  for the contextual-typing rule these are built around. */
import type { Worker, EnhancedEventEmitter } from './types.js';

export declare function createWorker(settings?: any): Promise<Worker>;
export declare function getSupportedRtpCapabilities(): any;
export declare function parseScalabilityMode(scalabilityMode?: any): any;
export declare const observer: EnhancedEventEmitter;
export declare const version: string;
export declare const workerBin: any;
export declare const types: any;
export declare const UnsupportedError: any;
export declare const NotFoundError: any;
export declare const WorkerClosedError: any;
export declare const InvalidStateError: any;
declare const _default: any;
export default _default;
