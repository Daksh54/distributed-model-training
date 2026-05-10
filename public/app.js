import {
  bytesToTrainingPayload,
  encodeTrainingPayload
} from "/browserTrainingKernel.js";

const elements = {
  identity: document.querySelector("#identity"),
  socketStatus: document.querySelector("#socketStatus"),
  peerCount: document.querySelector("#peerCount"),
  chunkCount: document.querySelector("#chunkCount"),
  capacity: document.querySelector("#capacity"),
  registerVolunteer: document.querySelector("#registerVolunteer"),
  findPeers: document.querySelector("#findPeers"),
  peerList: document.querySelector("#peerList"),
  taskInput: document.querySelector("#taskInput"),
  chunkSize: document.querySelector("#chunkSize"),
  replicas: document.querySelector("#replicas"),
  quorum: document.querySelector("#quorum"),
  runTask: document.querySelector("#runTask"),
  taskSummary: document.querySelector("#taskSummary"),
  log: document.querySelector("#log")
};

const state = {
  nodeId: localStorage.getItem("dbc.nodeId") || crypto.randomUUID(),
  ws: null,
  heartbeatTimer: null,
  peers: [],
  peerConnections: new Map(),
  channels: new Map(),
  pendingBinaryMeta: new Map(),
  chunks: new Map(),
  activeWorkers: new Map(),
  cancelledChunks: new Set(),
  activeTaskId: null,
  doneChunks: 0,
  totalChunks: 0,
  dispatchedPeerCount: 0,
  retryEvents: 0,
  lastRetryEvent: null
};

localStorage.setItem("dbc.nodeId", state.nodeId);

function log(message, details) {
  const timestamp = new Date().toLocaleTimeString();
  const suffix = details ? ` ${JSON.stringify(details)}` : "";
  elements.log.textContent = `[${timestamp}] ${message}${suffix}\n${elements.log.textContent}`;
}

function send(message) {
  if (state.ws?.readyState === WebSocket.OPEN) {
    state.ws.send(JSON.stringify({
      requestId: crypto.randomUUID(),
      ...message
    }));
  }
}

function updateIdentity() {
  elements.identity.textContent = `Node ${state.nodeId}`;
}

function updateTaskProgress() {
  elements.chunkCount.textContent = `${state.doneChunks}/${state.totalChunks}`;
}

function updateTaskSummary(statusText) {
  const parts = [statusText];

  if (state.retryEvents > 0) {
    parts.push(`${state.retryEvents} retry event(s)`);
  }

  if (state.lastRetryEvent) {
    parts.push(`latest: ${state.lastRetryEvent}`);
  }

  elements.taskSummary.textContent = parts.join(". ");
}

function noteRetryEvent(message) {
  state.retryEvents += 1;
  state.lastRetryEvent = message;
  updateTaskSummary(`Task ${state.activeTaskId} is running on ${state.dispatchedPeerCount} peer(s)`);
}

function capacityScore() {
  return Number(elements.capacity.value);
}

function connectSocket() {
  const protocol = location.protocol === "https:" ? "wss:" : "ws:";
  state.ws = new WebSocket(`${protocol}//${location.host}?nodeId=${encodeURIComponent(state.nodeId)}`);

  state.ws.addEventListener("open", () => {
    elements.socketStatus.textContent = "online";
    send({
      type: "register",
      role: "volunteer",
      capacityScore: capacityScore()
    });
  });

  state.ws.addEventListener("close", () => {
    elements.socketStatus.textContent = "offline";
    clearInterval(state.heartbeatTimer);
    setTimeout(connectSocket, 1000);
  });

  state.ws.addEventListener("message", (event) => {
    handleServerMessage(JSON.parse(event.data)).catch((error) => log(error.message));
  });
}

async function handleServerMessage(message) {
  switch (message.type) {
    case "welcome":
      state.nodeId = message.nodeId;
      localStorage.setItem("dbc.nodeId", state.nodeId);
      updateIdentity();
      clearInterval(state.heartbeatTimer);
      state.heartbeatTimer = setInterval(() => {
        send({
          type: "heartbeat",
          capacityScore: capacityScore()
        });
      }, message.heartbeatIntervalMs);
      log("Connected to signaling server");
      break;

    case "registered":
      log("Registered as volunteer", {
        capacityScore: message.capacityScore
      });
      break;

    case "peerList":
      state.peers = message.peers;
      renderPeers();
      log("Peer list updated", {
        count: message.peers.length
      });
      break;

    case "signal":
      await handleSignal(message);
      break;

    case "taskAccepted":
      state.activeTaskId = message.taskId;
      state.totalChunks = message.totalChunks;
      state.doneChunks = 0;
      state.dispatchedPeerCount = 0;
      state.retryEvents = 0;
      state.lastRetryEvent = null;
      updateTaskProgress();
      state.peers = message.peers;
      renderPeers();
      log("Task accepted", {
        taskId: message.taskId,
        chunks: message.totalChunks
      });
      await dispatchChunks(message.peers);
      break;

    case "chunkAssignment":
      sendChunkToPeer(message);
      break;

    case "chunkAssigned":
      log("Chunk assigned", {
        chunkId: message.chunkId,
        assignedBy: message.assignedBy
      });
      break;

    case "chunkDone":
      log("Server marked chunk done", {
        chunkId: message.chunkId
      });
      break;

    case "chunkResultAck":
      if (message.done) {
        state.doneChunks += 1;
        updateTaskProgress();
      }
      break;

    case "taskComplete":
      updateTaskSummary(`Task complete. Merkle root: ${message.merkleRoot}`);
      log("Task complete", {
        merkleRoot: message.merkleRoot
      });
      break;

    case "peerEvicted":
      noteRetryEvent(`peer ${message.nodeId} dropped chunk ${message.chunkId}`);
      log(message.type, message);
      break;

    case "chunkExpired":
      noteRetryEvent(`chunk ${message.chunkId} expired on peer ${message.peerId}`);
      log(message.type, message);
      break;

    case "cancelChunk":
      cancelActiveChunk(message.chunkId, message.reason);
      break;

    case "error":
      log(`Server error: ${message.message}`);
      break;

    default:
      break;
  }
}

function renderPeers() {
  elements.peerCount.textContent = String(state.peers.length);
  elements.peerList.replaceChildren();

  for (const peer of state.peers) {
    const item = document.createElement("li");
    item.textContent = `${peer.nodeId} (${peer.capacityScore})`;
    elements.peerList.append(item);
  }
}

function createPeerConnection(peerId, initiator) {
  if (state.peerConnections.has(peerId)) {
    const existingConnection = state.peerConnections.get(peerId);
    const existingChannel = state.channels.get(peerId);

    if (initiator && (!existingChannel || ["closing", "closed"].includes(existingChannel.readyState))) {
      setupChannel(peerId, existingConnection.createDataChannel("chunks"));
    }

    return existingConnection;
  }

  const connection = new RTCPeerConnection({
    iceServers: [{ urls: "stun:stun.l.google.com:19302" }]
  });

  connection.addEventListener("icecandidate", (event) => {
    if (event.candidate) {
      send({
        type: "signal",
        to: peerId,
        signalType: "ice",
        payload: event.candidate
      });
    }
  });

  connection.addEventListener("datachannel", (event) => {
    setupChannel(peerId, event.channel);
  });

  state.peerConnections.set(peerId, connection);

  if (initiator) {
    const channel = connection.createDataChannel("chunks");
    setupChannel(peerId, channel);
  }

  return connection;
}

function setupChannel(peerId, channel) {
  channel.binaryType = "arraybuffer";
  state.channels.set(peerId, channel);

  channel.addEventListener("open", () => {
    log("DataChannel open", {
      peerId
    });
  });

  channel.addEventListener("message", (event) => {
    handleDataChannelMessage(peerId, event.data).catch((error) => log(error.message));
  });
}

async function ensureChannel(peerId) {
  const existing = state.channels.get(peerId);

  if (existing?.readyState === "open") {
    return existing;
  }

  const connection = createPeerConnection(peerId, true);
  const offer = await connection.createOffer();
  await connection.setLocalDescription(offer);
  send({
    type: "signal",
    to: peerId,
    signalType: "offer",
    payload: offer
  });

  return new Promise((resolve, reject) => {
    const startedAt = performance.now();
    const timer = setInterval(() => {
      const channel = state.channels.get(peerId);

      if (channel?.readyState === "open") {
        clearInterval(timer);
        resolve(channel);
      } else if (performance.now() - startedAt > 10000) {
        clearInterval(timer);
        reject(new Error(`Timed out opening DataChannel to ${peerId}`));
      }
    }, 100);
  });
}

async function handleSignal(message) {
  const peerId = message.from;
  const connection = createPeerConnection(peerId, message.signalType !== "offer");

  if (message.signalType === "offer") {
    await connection.setRemoteDescription(message.payload);
    const answer = await connection.createAnswer();
    await connection.setLocalDescription(answer);
    send({
      type: "signal",
      to: peerId,
      signalType: "answer",
      payload: answer
    });
  }

  if (message.signalType === "answer") {
    await connection.setRemoteDescription(message.payload);
  }

  if (message.signalType === "ice") {
    await connection.addIceCandidate(message.payload);
  }
}

async function digestHex(buffer) {
  const hash = await crypto.subtle.digest("SHA-256", buffer);
  return [...new Uint8Array(hash)]
    .map((byte) => byte.toString(16).padStart(2, "0"))
    .join("");
}

function splitBytes(bytes, chunkSize) {
  const chunks = [];

  for (let offset = 0; offset < bytes.byteLength; offset += chunkSize) {
    chunks.push(bytes.slice(offset, offset + chunkSize));
  }

  return chunks;
}

async function buildTaskChunks() {
  const encoder = new TextEncoder();
  const bytes = encoder.encode(elements.taskInput.value);
  const chunkSize = Number(elements.chunkSize.value);
  const byteChunks = splitBytes(bytes, chunkSize);
  const taskId = crypto.randomUUID();
  const chunks = [];

  state.chunks.clear();

  for (let index = 0; index < byteChunks.length; index += 1) {
    const chunkId = `${taskId}:${index}`;
    const trainingPayload = bytesToTrainingPayload(byteChunks[index], {
      chunkId,
      order: index * chunkSize,
      sourceLength: bytes.byteLength
    });
    const payload = encodeTrainingPayload(trainingPayload);
    const checksum = await digestHex(payload);

    chunks.push({
      chunkId,
      order: index,
      checksum,
      byteLength: payload.byteLength,
      replicas: Number(elements.replicas.value),
      quorum: Number(elements.quorum.value)
    });

    state.chunks.set(chunkId, payload);
  }

  return {
    taskId,
    chunks
  };
}

async function dispatchChunks(peers) {
  if (peers.length === 0) {
    updateTaskSummary("No volunteer peers are currently available");
    return;
  }

  const channelAttempts = await Promise.allSettled(peers.map(async (peer) => ({
    peer,
    channel: await ensureChannel(peer.nodeId)
  })));

  const readyPeers = [];

  for (const attempt of channelAttempts) {
    if (attempt.status === "rejected") {
      log("Unable to open DataChannel", {
        error: attempt.reason instanceof Error ? attempt.reason.message : String(attempt.reason)
      });
      continue;
    }

    const { peer, channel } = attempt.value;

    if (channel.readyState === "open") {
      readyPeers.push(peer);
    } else {
      log("DataChannel was not ready for dispatch", {
        peerId: peer.nodeId,
        readyState: channel.readyState
      });
    }
  }

  if (readyPeers.length === 0) {
    updateTaskSummary("No peer DataChannels opened before dispatch");
    return;
  }

  const peerIds = readyPeers.map((peer) => peer.nodeId);
  const chunkIds = [...state.chunks.keys()];

  for (let index = 0; index < chunkIds.length; index += 1) {
    const peerId = peerIds[index % peerIds.length];
    send({
      type: "assignChunk",
      taskId: state.activeTaskId,
      chunkId: chunkIds[index],
      peerId
    });
  }

  state.dispatchedPeerCount = peerIds.length;
  updateTaskSummary(`Task ${state.activeTaskId} dispatched to ${peerIds.length} peer(s)`);
}

function sendChunkToPeer(assignment) {
  const channel = state.channels.get(assignment.peerId);
  const payload = state.chunks.get(assignment.chunkId);

  if (!channel || channel.readyState !== "open" || !payload) {
    log("Unable to send chunk payload", assignment);
    return;
  }

  channel.send(JSON.stringify({
    type: "chunkMeta",
    taskId: assignment.taskId,
    chunkId: assignment.chunkId,
    checksum: assignment.checksum,
    byteLength: payload.byteLength
  }));
  channel.send(payload.buffer.slice(payload.byteOffset, payload.byteOffset + payload.byteLength));
  log("Sent chunk over DataChannel", {
    chunkId: assignment.chunkId,
    peerId: assignment.peerId
  });
}

async function handleDataChannelMessage(peerId, data) {
  if (typeof data === "string") {
    const message = JSON.parse(data);

    if (message.type === "chunkMeta") {
      state.pendingBinaryMeta.set(peerId, message);
      return;
    }

    if (message.type === "chunkResult") {
      send({
        type: "chunkResult",
        taskId: message.taskId,
        chunkId: message.chunkId,
        peerId,
        resultHash: message.resultHash
      });
      log("Received chunk result", {
        chunkId: message.chunkId,
        result: message.result
      });
      return;
    }

    if (message.type === "chunkError") {
      send({
        type: "chunkFailed",
        taskId: message.taskId,
        chunkId: message.chunkId,
        peerId,
        error: message.error
      });
      log("Received chunk error", {
        chunkId: message.chunkId,
        error: message.error
      });
      return;
    }
  }

  const meta = state.pendingBinaryMeta.get(peerId);

  if (!meta) {
    throw new Error("Received binary payload without metadata");
  }

  state.pendingBinaryMeta.delete(peerId);
  const payload = data instanceof Blob ? await data.arrayBuffer() : data;
  const checksum = await digestHex(payload);

  if (checksum !== meta.checksum) {
    throw new Error(`Checksum mismatch for ${meta.chunkId}`);
  }

  runWorker(meta, payload, peerId);
}

function runWorker(meta, payload, peerId) {
  if (state.cancelledChunks.has(meta.chunkId)) {
    state.cancelledChunks.delete(meta.chunkId);
    log("Skipped cancelled chunk payload", {
      chunkId: meta.chunkId
    });
    return;
  }

  const worker = new Worker("/worker.js", {
    type: "module"
  });

  state.activeWorkers.set(meta.chunkId, worker);

  worker.addEventListener("message", (event) => {
    const channel = state.channels.get(peerId);

    if (event.data.error && channel?.readyState === "open") {
      channel.send(JSON.stringify({
        type: "chunkError",
        taskId: meta.taskId,
        chunkId: meta.chunkId,
        error: event.data.error
      }));
    } else if (!state.cancelledChunks.has(meta.chunkId) && channel?.readyState === "open") {
      channel.send(JSON.stringify({
        type: "chunkResult",
        taskId: meta.taskId,
        chunkId: meta.chunkId,
        result: event.data.result,
        resultHash: event.data.resultHash
      }));
    }

    state.cancelledChunks.delete(meta.chunkId);
    state.activeWorkers.delete(meta.chunkId);
    worker.terminate();
  });

  worker.addEventListener("error", (event) => {
    state.activeWorkers.delete(meta.chunkId);
    worker.terminate();
    log("Worker failed", {
      chunkId: meta.chunkId,
      message: event.message
    });
  });

  worker.postMessage({
    chunkId: meta.chunkId,
    payload
  }, [payload]);
}

function cancelActiveChunk(chunkId, reason) {
  state.cancelledChunks.add(chunkId);
  const worker = state.activeWorkers.get(chunkId);

  if (worker) {
    worker.terminate();
    state.activeWorkers.delete(chunkId);
    log("Cancelled active chunk", {
      chunkId,
      reason
    });
    return;
  }

  log("Marked chunk cancelled", {
    chunkId,
    reason
  });
}

elements.registerVolunteer.addEventListener("click", () => {
  send({
    type: "register",
    role: "volunteer",
    capacityScore: capacityScore()
  });
});

elements.findPeers.addEventListener("click", () => {
  send({
    type: "requestPeers",
    limit: 8
  });
});

elements.runTask.addEventListener("click", async () => {
  const task = await buildTaskChunks();
  send({
    type: "submitTask",
    taskId: task.taskId,
    chunks: task.chunks,
    peerLimit: 8
  });
});

updateIdentity();
updateTaskProgress();
connectSocket();
