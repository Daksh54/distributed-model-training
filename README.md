# Distributed Browser Compute Prototype

This project implements the first working slice of a browser-based volunteer compute network:

- Node.js WebSocket signaling server using `ws`
- Peer registry with node IDs, capacity score, heartbeat timestamps, and assigned chunk IDs
- Stale peer eviction after a missed heartbeat window
- Server-side chunk queue with `pending`, `in-flight`, and `done` states
- Reassignment when peers disconnect or chunk assignments time out
- SHA-256 chunk checksums and Merkle root helpers
- Browser demo for two or more tabs using WebRTC `RTCDataChannel`
- Worker-based compute path with a JavaScript fallback kernel, ready for Rust/Wasm
- Optional Redis snapshot backing via `REDIS_URL`
- Native volunteer agent for training-style work assignment
- Gradient averaging and synchronization barrier for data-parallel ML steps

## Run

```bash
npm install
npm start
```

Open `http://localhost:3000` in two browser tabs. Both tabs register as volunteer nodes by default. In one tab, click **Find Peers**, then **Run Demo Task**. The task text is converted into tiny supervised training batches, assigned by the server, sent directly over WebRTC data channels, processed in a Web Worker, and confirmed back to the server by result hash.

## Browser Wasm Kernel

The browser worker now runs a real forward/backward pass for a tiny single-neuron MSE model instead of the old byte-sum placeholder. It accepts a JSON mini-batch with `features`, `labels`, `weights`, and `bias`, then returns loss and gradients.

By default this runs through `public/browserTrainingKernel.js`, so the demo works without a Rust toolchain. To use the Rust/Wasm path:

```bash
cargo install wasm-pack
npm run wasm:build
npm start
```

`npm run wasm:build` compiles `wasm-kernel/` and writes the generated module to `public/wasm_kernel/`. `public/worker.js` loads `/wasm_kernel/distributed_ml_kernel.js` when present and falls back to the JavaScript kernel if it is missing.

## Native Training Foundation

The native-agent path is the first slice of the distributed ML plan. It keeps the existing signaling server and chunk queue, but moves compute into a terminal process that can run local JavaScript kernels now and Python/PyTorch kernels later.

Terminal 1:

```bash
npm start
```

Terminal 2:

```bash
npm run agent -- --server ws://localhost:3000
```

Terminal 3:

```bash
npm run coordinator -- --server ws://localhost:3000 --min-peers 1 --peers 1
```

The coordinator submits a toy data-parallel linear-regression step. The native agent receives a chunk assignment, computes gradients, sends a `chunkResult`, then reports `gradientReady`. The server waits at the training barrier, averages gradients, and emits `barrierReached`.

The default native kernel is dependency-free so agents can run immediately. To exercise the Python subprocess path, send an assignment with `kernel: { "runner": "python" }`; `agent/kernels/train_chunk.py` implements the same tiny gradient kernel and can be replaced with a PyTorch model.

## Test

```bash
npm test
```

## Environment

| Variable | Default | Purpose |
| --- | --- | --- |
| `PORT` | `3000` | HTTP and WebSocket server port |
| `HEARTBEAT_INTERVAL_MS` | `10000` | Browser heartbeat cadence |
| `STALE_PEER_MS` | `15000` | Peer eviction window |
| `CHUNK_TIMEOUT_MS` | `30000` | Reassignment timeout for in-flight chunks |
| `REDIS_URL` | unset | Enables Redis-backed queue snapshots |
| `INVITE_TOKENS` | unset | Optional comma-separated invite tokens. If set, `register` messages must include one. |
| `TRAINING_MODE` | `data_parallel` | Planned training mode: `data_parallel`, `pipeline`, or `tensor` |
| `GRADIENT_COMPRESSION` | `float16` | Planned transport compression setting |
| `CHECKPOINT_EVERY` | `100` | Planned checkpoint cadence for coordinators |
| `CHECKPOINT_DIR` | `./checkpoints` | Planned checkpoint directory |
| `BARRIER_TIMEOUT_MS` | `120000` | Training barrier timeout |
| `MIN_AGENTS_FOR_STEP` | `1` | Minimum agents required to start a training step |
| `WEIGHT_CACHE_DIR` | `./weight_cache` | Native agent weight cache directory |
| `TLS_CERT` / `TLS_KEY` / `TLS_CA` | unset | Reserved for mTLS deployment |

## Protocol

All WebSocket messages are JSON.

Client to server:

- `register`: updates role and capacity score
- `heartbeat`: refreshes liveness
- `requestPeers`: asks for available volunteer peer IDs
- `signal`: relays SDP/ICE messages to another peer
- `submitTask`: registers chunk metadata and checksums
- `assignChunk`: marks a chunk in-flight for a target peer
- `chunkResult`: confirms a result hash
- `taskStatus`: asks for current chunk state
- `registerCapabilities`: native agent hardware report
- `startTrainingStep`: coordinator creates a synchronization barrier
- `gradientReady`: agent reports inline gradients for aggregation
- `gradientChunk`: placeholder ack path for future large tensor streaming
- `stepComplete`: agent confirms it applied an aggregate
- `taskKernel` / `weightSync`: coordinator relays kernel or cached weight metadata to an agent

Server to client:

- `welcome`: assigns or confirms the node ID
- `registered`: acknowledges peer metadata
- `peerList`: returns available peers
- `signal`: relayed WebRTC signaling payload
- `taskAccepted`: confirms chunk metadata
- `chunkAssignment` / `chunkAssigned`: assignment control messages
- `chunkResultAck`: confirms result-hash checkpointing
- `taskComplete`: includes ordered result hashes and Merkle root
- `peerEvicted`, `chunkExpired`, `cancelChunk`: lifecycle notifications
- `capabilitiesRegistered`: acknowledges native hardware metadata
- `trainingStepStarted`: acknowledges barrier creation
- `gradientReadyAck`: acknowledges gradient receipt
- `aggregatedGradient`: averaged gradient for the step
- `barrierReached`: synchronization barrier completed
- `stepRollback`: barrier timed out and the coordinator should retry

## Wasm Slot

The checked-in Rust source is intentionally small: it proves the browser can execute ML-shaped training work over Wasm. It is not yet a general PyTorch replacement or a large-model trainer. The next step is extending the payload schema from a single neuron to multi-layer tensors, then adding the consent UI and TURN relay before public deployment.
