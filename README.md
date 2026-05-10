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

## Run

```bash
npm install
npm start
```

Open `http://localhost:3000` in two browser tabs. Both tabs register as volunteer nodes by default. In one tab, click **Find Peers**, then **Run Demo Task**. The task payload is chunked locally, assigned by the server, sent directly over WebRTC data channels, processed in a Web Worker, and confirmed back to the server by result hash.

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

## Wasm Slot

The browser worker currently runs a trivial sum kernel so the WebRTC and chunking path can be tested without a Rust toolchain. Replace the fallback in `public/worker.js` with a `wasm-pack` generated module when the Rust kernel is ready.
