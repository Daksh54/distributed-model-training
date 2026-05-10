import fs from "node:fs/promises";
import http from "node:http";
import path from "node:path";
import { fileURLToPath } from "node:url";
import WebSocket, { WebSocketServer } from "ws";
import { ChunkQueue } from "./chunkQueue.js";
import { config } from "./config.js";
import { buildMerkleRoot } from "./hash.js";
import { PeerRegistry } from "./peerRegistry.js";
import { createStateStore } from "./stateStore.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const publicDirectory = path.resolve(__dirname, "..", "public");

const contentTypes = new Map([
  [".html", "text/html; charset=utf-8"],
  [".css", "text/css; charset=utf-8"],
  [".js", "text/javascript; charset=utf-8"],
  [".json", "application/json; charset=utf-8"],
  [".wasm", "application/wasm"]
]);

const registry = new PeerRegistry({ stalePeerMs: config.stalePeerMs });
const chunkQueue = new ChunkQueue({ assignmentTimeoutMs: config.chunkTimeoutMs });
const stateStore = await createStateStore({
  redisUrl: config.redisUrl,
  redisKeyPrefix: config.redisKeyPrefix
});

const restoredQueue = await stateStore.loadQueueSnapshot();
chunkQueue.restore(restoredQueue);

async function persistQueue() {
  await stateStore.saveQueueSnapshot(chunkQueue.snapshot());
}

function send(socket, message) {
  if (socket?.readyState === WebSocket.OPEN) {
    socket.send(JSON.stringify(message));
  }
}

function sendToPeer(nodeId, message) {
  const peer = registry.get(nodeId);

  if (!peer) {
    return false;
  }

  send(peer.socket, message);
  return true;
}

function parseMessage(raw) {
  try {
    return JSON.parse(raw.toString("utf8"));
  } catch {
    throw new Error("Invalid JSON message");
  }
}

function errorMessage(requestId, error) {
  return {
    type: "error",
    requestId,
    message: error instanceof Error ? error.message : String(error)
  };
}

function taskStatusMessage(taskId) {
  const status = chunkQueue.getTaskStatus(taskId);
  const completeResultHashes = status.resultHashes.every(Boolean) ? status.resultHashes : null;

  return {
    type: "taskStatus",
    ...status,
    merkleRoot: completeResultHashes ? buildMerkleRoot(completeResultHashes) : null
  };
}

function publishTaskCompletion(taskId) {
  const status = taskStatusMessage(taskId);

  if (!status.complete) {
    return;
  }

  const owner = status.chunks[0]?.submittedBy;

  if (owner) {
    sendToPeer(owner, {
      type: "taskComplete",
      taskId,
      resultHashes: status.resultHashes,
      merkleRoot: status.merkleRoot,
      counts: status.counts
    });
  }
}

function validateChunkResultHash(resultHash) {
  if (typeof resultHash !== "string" || !/^[a-f0-9]{64}$/i.test(resultHash)) {
    throw new Error("resultHash must be a SHA-256 hex string");
  }
}

async function handleMessage(peer, raw) {
  const message = parseMessage(raw);
  const requestId = message.requestId;

  switch (message.type) {
    case "register": {
      const updatedPeer = registry.update(peer.nodeId, {
        capacityScore: message.capacityScore,
        role: message.role
      });

      send(peer.socket, {
        type: "registered",
        requestId,
        nodeId: updatedPeer.nodeId,
        role: updatedPeer.role,
        capacityScore: updatedPeer.capacityScore
      });
      break;
    }

    case "heartbeat": {
      const updatedPeer = registry.heartbeat(peer.nodeId, {
        capacityScore: message.capacityScore
      });

      send(peer.socket, {
        type: "heartbeatAck",
        requestId,
        nodeId: updatedPeer.nodeId,
        lastHeartbeat: updatedPeer.lastHeartbeat
      });
      break;
    }

    case "requestPeers": {
      send(peer.socket, {
        type: "peerList",
        requestId,
        taskId: message.taskId,
        peers: registry.getAvailablePeers({
          limit: message.limit ?? 8,
          excludeNodeIds: [peer.nodeId, ...(message.excludeNodeIds ?? [])],
          role: message.role ?? "volunteer"
        })
      });
      break;
    }

    case "signal": {
      if (!message.to) {
        throw new Error("signal requires a target peer in 'to'");
      }

      const delivered = sendToPeer(message.to, {
        type: "signal",
        requestId,
        from: peer.nodeId,
        signalType: message.signalType,
        payload: message.payload
      });

      if (!delivered) {
        throw new Error(`Target peer is unavailable: ${message.to}`);
      }
      break;
    }

    case "submitTask": {
      const accepted = chunkQueue.submitTask({
        taskId: message.taskId,
        submittedBy: peer.nodeId,
        chunks: message.chunks,
        defaultPriority: message.defaultPriority,
        defaultReplicas: message.defaultReplicas,
        defaultQuorum: message.defaultQuorum
      });

      await persistQueue();

      send(peer.socket, {
        type: "taskAccepted",
        requestId,
        ...accepted,
        peers: registry.getAvailablePeers({
          limit: message.peerLimit ?? 8,
          excludeNodeIds: [peer.nodeId]
        })
      });
      break;
    }

    case "assignChunk": {
      const targetPeerId = message.peerId ?? peer.nodeId;
      const assignment = message.chunkId
        ? chunkQueue.assignChunk({ chunkId: message.chunkId, peerId: targetPeerId })
        : chunkQueue.assignNext({ taskId: message.taskId, peerId: targetPeerId });

      if (!assignment) {
        send(peer.socket, {
          type: "noChunkAvailable",
          requestId,
          taskId: message.taskId,
          peerId: targetPeerId
        });
        break;
      }

      registry.assignChunk(targetPeerId, assignment.chunkId);
      await persistQueue();

      const controlMessage = {
        type: "chunkAssigned",
        requestId,
        assignedBy: peer.nodeId,
        ...assignment
      };

      send(peer.socket, {
        type: "chunkAssignment",
        requestId,
        ...assignment
      });

      if (targetPeerId !== peer.nodeId) {
        sendToPeer(targetPeerId, controlMessage);
      }
      break;
    }

    case "chunkResult": {
      validateChunkResultHash(message.resultHash);

      const result = chunkQueue.markResult({
        chunkId: message.chunkId,
        peerId: message.peerId ?? peer.nodeId,
        resultHash: message.resultHash
      });

      registry.unassignChunk(message.peerId ?? peer.nodeId, message.chunkId);

      for (const cancelledPeer of result.cancelledPeers) {
        registry.unassignChunk(cancelledPeer, message.chunkId);
        sendToPeer(cancelledPeer, {
          type: "cancelChunk",
          taskId: result.chunk.taskId,
          chunkId: message.chunkId,
          reason: "quorum_reached"
        });
      }

      await persistQueue();

      send(peer.socket, {
        type: "chunkResultAck",
        requestId,
        ...result
      });

      const owner = result.chunk.submittedBy;

      if (owner && owner !== peer.nodeId) {
        sendToPeer(owner, {
          type: "chunkDone",
          taskId: result.chunk.taskId,
          chunkId: result.chunk.chunkId,
          resultHash: result.chunk.resultHash,
          status: result.chunk.status
        });
      }

      publishTaskCompletion(result.chunk.taskId);
      break;
    }

    case "taskStatus": {
      send(peer.socket, {
        requestId,
        ...taskStatusMessage(message.taskId)
      });
      break;
    }

    default:
      throw new Error(`Unsupported message type: ${message.type}`);
  }
}

async function serveStatic(request, response) {
  const requestUrl = new URL(request.url, `http://${request.headers.host}`);
  const requestedPath = requestUrl.pathname === "/" ? "/index.html" : requestUrl.pathname;
  const filePath = path.resolve(publicDirectory, `.${requestedPath}`);

  response.setHeader("Cross-Origin-Opener-Policy", "same-origin");
  response.setHeader("Cross-Origin-Embedder-Policy", "require-corp");

  if (!filePath.startsWith(publicDirectory)) {
    response.writeHead(403);
    response.end("Forbidden");
    return;
  }

  try {
    const body = await fs.readFile(filePath);
    response.writeHead(200, {
      "Content-Type": contentTypes.get(path.extname(filePath)) ?? "application/octet-stream"
    });
    response.end(body);
  } catch (error) {
    if (error.code === "ENOENT") {
      response.writeHead(404);
      response.end("Not found");
      return;
    }

    response.writeHead(500);
    response.end("Internal server error");
  }
}

const server = http.createServer((request, response) => {
  if (request.method !== "GET") {
    response.writeHead(405);
    response.end("Method not allowed");
    return;
  }

  void serveStatic(request, response);
});

const webSocketServer = new WebSocketServer({ server });

webSocketServer.on("connection", (socket, request) => {
  const requestUrl = new URL(request.url, `http://${request.headers.host}`);
  const requestedNodeId = requestUrl.searchParams.get("nodeId");
  const peer = registry.connect(socket, { nodeId: requestedNodeId });

  send(socket, {
    type: "welcome",
    nodeId: peer.nodeId,
    heartbeatIntervalMs: config.heartbeatIntervalMs,
    stalePeerMs: config.stalePeerMs,
    chunkTimeoutMs: config.chunkTimeoutMs
  });

  socket.on("message", (raw) => {
    handleMessage(peer, raw).catch((error) => {
      send(socket, errorMessage(undefined, error));
    });
  });

  socket.on("close", () => {
    if (registry.get(peer.nodeId)?.socket !== socket) {
      return;
    }

    registry.remove(peer.nodeId);
    const requeued = chunkQueue.requeuePeer(peer.nodeId);

    for (const chunk of requeued) {
      sendToPeer(chunk.submittedBy, {
        type: "peerEvicted",
        nodeId: peer.nodeId,
        taskId: chunk.taskId,
        chunkId: chunk.chunkId
      });
    }

    void persistQueue();
  });
});

setInterval(() => {
  const evictedPeers = registry.evictStale();

  for (const evictedPeer of evictedPeers) {
    const requeued = chunkQueue.requeuePeer(evictedPeer.nodeId);

    for (const chunk of requeued) {
      sendToPeer(chunk.submittedBy, {
        type: "peerEvicted",
        nodeId: evictedPeer.nodeId,
        taskId: chunk.taskId,
        chunkId: chunk.chunkId
      });
    }
  }

  if (evictedPeers.length > 0) {
    void persistQueue();
  }
}, Math.max(1000, Math.floor(config.stalePeerMs / 3))).unref();

setInterval(() => {
  const expiredAssignments = chunkQueue.expireAssignments();

  for (const expiredAssignment of expiredAssignments) {
    registry.unassignChunk(expiredAssignment.peerId, expiredAssignment.chunk.chunkId);
    sendToPeer(expiredAssignment.chunk.submittedBy, {
      type: "chunkExpired",
      peerId: expiredAssignment.peerId,
      taskId: expiredAssignment.chunk.taskId,
      chunkId: expiredAssignment.chunk.chunkId
    });
  }

  if (expiredAssignments.length > 0) {
    void persistQueue();
  }
}, 1000).unref();

server.listen(config.port, () => {
  console.log(`Signaling server listening on http://localhost:${config.port}`);
});

async function shutdown() {
  await persistQueue();
  await stateStore.close();
  server.close();
}

process.on("SIGINT", () => {
  shutdown().finally(() => process.exit(0));
});

process.on("SIGTERM", () => {
  shutdown().finally(() => process.exit(0));
});
