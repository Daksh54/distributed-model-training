import { randomUUID } from "node:crypto";

const SOCKET_OPEN = 1;

function toAssignedChunkIds(value) {
  if (value instanceof Set) {
    return value;
  }

  if (Array.isArray(value)) {
    return new Set(value);
  }

  return new Set();
}

export class PeerRegistry {
  constructor({ stalePeerMs = 15000, idFactory = randomUUID } = {}) {
    this.stalePeerMs = stalePeerMs;
    this.idFactory = idFactory;
    this.peers = new Map();
  }

  connect(socket, { nodeId, capacityScore = 0, role = "volunteer", now = Date.now() } = {}) {
    const nextNodeId = nodeId || this.idFactory();
    const existingPeer = this.peers.get(nextNodeId);

    if (existingPeer?.socket && existingPeer.socket !== socket && existingPeer.socket.readyState === SOCKET_OPEN) {
      existingPeer.socket.close(4000, "Duplicate node connection replaced");
    }

    const peer = {
      nodeId: nextNodeId,
      socket,
      capacityScore: Number(capacityScore) || 0,
      role,
      lastHeartbeat: now,
      assignedChunkIds: toAssignedChunkIds(existingPeer?.assignedChunkIds)
    };

    this.peers.set(nextNodeId, peer);
    return peer;
  }

  update(nodeId, { capacityScore, role, now = Date.now() } = {}) {
    const peer = this.requirePeer(nodeId);

    if (capacityScore !== undefined) {
      peer.capacityScore = Math.max(0, Number(capacityScore) || 0);
    }

    if (role) {
      peer.role = role;
    }

    peer.lastHeartbeat = now;
    return peer;
  }

  heartbeat(nodeId, { capacityScore, now = Date.now() } = {}) {
    return this.update(nodeId, { capacityScore, now });
  }

  get(nodeId) {
    return this.peers.get(nodeId);
  }

  has(nodeId) {
    return this.peers.has(nodeId);
  }

  remove(nodeId) {
    const peer = this.peers.get(nodeId);
    this.peers.delete(nodeId);
    return peer ?? null;
  }

  assignChunk(nodeId, chunkId) {
    const peer = this.peers.get(nodeId);

    if (peer) {
      peer.assignedChunkIds.add(chunkId);
    }
  }

  unassignChunk(nodeId, chunkId) {
    const peer = this.peers.get(nodeId);

    if (peer) {
      peer.assignedChunkIds.delete(chunkId);
    }
  }

  evictStale(now = Date.now()) {
    const evicted = [];

    for (const peer of this.peers.values()) {
      if (now - peer.lastHeartbeat > this.stalePeerMs) {
        this.peers.delete(peer.nodeId);
        evicted.push(peer);
      }
    }

    return evicted;
  }

  getAvailablePeers({ limit = 8, excludeNodeIds = [], role = "volunteer", now = Date.now() } = {}) {
    const excluded = new Set(excludeNodeIds);

    return [...this.peers.values()]
      .filter((peer) => peer.role === role)
      .filter((peer) => !excluded.has(peer.nodeId))
      .filter((peer) => now - peer.lastHeartbeat <= this.stalePeerMs)
      .filter((peer) => !peer.socket || peer.socket.readyState === SOCKET_OPEN)
      .sort((left, right) => {
        const capacityDifference = right.capacityScore - left.capacityScore;

        if (capacityDifference !== 0) {
          return capacityDifference;
        }

        return left.assignedChunkIds.size - right.assignedChunkIds.size;
      })
      .slice(0, limit)
      .map((peer) => ({
        nodeId: peer.nodeId,
        capacityScore: peer.capacityScore,
        lastHeartbeat: peer.lastHeartbeat,
        assignedChunkIds: [...peer.assignedChunkIds]
      }));
  }

  snapshot() {
    return {
      peers: [...this.peers.values()].map((peer) => ({
        nodeId: peer.nodeId,
        capacityScore: peer.capacityScore,
        role: peer.role,
        lastHeartbeat: peer.lastHeartbeat,
        assignedChunkIds: [...peer.assignedChunkIds]
      }))
    };
  }

  requirePeer(nodeId) {
    const peer = this.peers.get(nodeId);

    if (!peer) {
      throw new Error(`Unknown peer: ${nodeId}`);
    }

    return peer;
  }
}
