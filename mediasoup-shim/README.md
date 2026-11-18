# sfu-server-mediasoup

Run an existing **mediasoup v3** server application on [sfu-server](https://github.com/colocohen/sfu-server): pure JavaScript, no worker binary, no C++ toolchain. No application code changes.

## Install

Install it under the name `mediasoup` with a package alias:

```bash
npm i mediasoup@npm:sfu-server-mediasoup
# pnpm add mediasoup@npm:sfu-server-mediasoup
```

which writes this to your `package.json`:

```json
"dependencies": {
  "mediasoup": "npm:sfu-server-mediasoup@^0.1.0"
}
```

Your code keeps importing `mediasoup`:

```js
import * as mediasoup from 'mediasoup';

const worker = await mediasoup.createWorker();
const router = await worker.createRouter({ mediaCodecs });
```

Clients keep using `mediasoup-client` unchanged.

## TypeScript

`mediasoup` and `mediasoup/types` come with declarations that let an unmodified strict-mode mediasoup application compile: every entity (Worker, Router, Transport, Producer, Consumer...) is typed as an event emitter, and the methods that create them return their types, so unannotated chains such as `const producer = await transport.produce(...)` stay typed. Plain data shapes (RtpCapabilities, DtlsParameters...) are `any`.

## What is supported

Everything here is sfu-server's mediasoup compatibility layer (`src/mediasoup_compat.js` in this repository). See [sfu-server](https://github.com/colocohen/sfu-server#readme) for what it covers and what it does not.

To use the API without the alias, import it from sfu-server directly:

```js
import { mediasoup } from 'sfu-server';
```

## Publishing

This folder is its own npm package and is not part of the `sfu-server` tarball. Publish it from here:

```bash
cd mediasoup-shim
npm publish
```

## License

Apache-2.0
