import { randomUUID } from "node:crypto";
import { sha256Hex } from "./hash.js";

const STATUS_PENDING = "pending";
const STATUS_IN_FLIGHT = "in-flight";
const STATUS_DONE = "done";

function asPositiveInteger(value, fallback) {
  const number = Number.parseInt(value ?? fallback, 10);
  return Number.isFinite(number) && number > 0 ? number : fallback;
}

function activeAssignmentIds(chunk) {
  return Object.keys(chunk.assignments);
}

function completedPeerIds(chunk) {
  return Object.values(chunk.resultVotes).flat();
}

export class ChunkQueue {
  constructor({ assignmentTimeoutMs = 30000, idFactory = randomUUID } = {}) {
    this.assignmentTimeoutMs = assignmentTimeoutMs;
    this.idFactory = idFactory;
    this.chunks = new Map();
  }

  submitTask({
    taskId = this.idFactory(),
    submittedBy,
    chunks,
    defaultPriority = 0,
    defaultReplicas = 1,
    defaultQuorum = 1,
    now = Date.now()
  }) {
    if (!Array.isArray(chunks) || chunks.length === 0) {
      throw new Error("submitTask requires at least one chunk");
    }

    const createdChunks = chunks.map((chunk, index) => {
      const chunkId = chunk.chunkId ?? `${taskId}:${index}`;

      if (this.chunks.has(chunkId)) {
        throw new Error(`Duplicate chunk ID: ${chunkId}`);
      }

      const checksum = chunk.checksum ?? (chunk.input !== undefined ? sha256Hex(chunk.input) : undefined);

      if (!checksum) {
        throw new Error(`Chunk ${chunkId} is missing a checksum`);
      }

      const replicas = asPositiveInteger(chunk.replicas, defaultReplicas);
      const quorum = asPositiveInteger(chunk.quorum, defaultQuorum);

      if (quorum > replicas) {
        throw new Error(`Chunk ${chunkId} quorum cannot exceed replicas`);
      }

      const record = {
        taskId,
        chunkId,
        order: Number.isFinite(chunk.order) ? chunk.order : index,
        checksum,
        byteLength: chunk.byteLength ?? null,
        priority: Number(chunk.priority ?? defaultPriority) || 0,
        replicas,
        quorum,
        status: STATUS_PENDING,
        submittedBy,
        assignedTo: null,
        assignments: {},
        resultVotes: {},
        resultHash: null,
        attempts: 0,
        createdAt: now,
        updatedAt: now,
        doneAt: null
      };

      this.chunks.set(chunkId, record);
      return this.publicChunk(record);
    });

    return {
      taskId,
      chunks: createdChunks,
      totalChunks: createdChunks.length
    };
  }

  assignNext({ peerId, taskId, now = Date.now() }) {
    const eligibleChunks = [...this.chunks.values()]
      .filter((chunk) => !taskId || chunk.taskId === taskId)
      .filter((chunk) => this.canAssign(chunk, peerId))
      .sort((left, right) => {
        const priorityDifference = right.priority - left.priority;

        if (priorityDifference !== 0) {
          return priorityDifference;
        }

        return left.createdAt - right.createdAt || left.order - right.order;
      });

    const chunk = eligibleChunks[0];

    if (!chunk) {
      return null;
    }

    return this.assignChunk({ chunkId: chunk.chunkId, peerId, now });
  }

  assignChunk({ chunkId, peerId, now = Date.now() }) {
    const chunk = this.requireChunk(chunkId);

    if (!this.canAssign(chunk, peerId)) {
      return null;
    }

    chunk.assignments[peerId] = {
      assignedAt: now,
      expiresAt: now + this.assignmentTimeoutMs
    };
    chunk.assignedTo = activeAssignmentIds(chunk)[0] ?? null;
    chunk.status = STATUS_IN_FLIGHT;
    chunk.attempts += 1;
    chunk.updatedAt = now;

    return this.publicAssignment(chunk, peerId);
  }

  markResult({ chunkId, peerId, resultHash, now = Date.now() }) {
    const chunk = this.requireChunk(chunkId);

    if (chunk.status === STATUS_DONE) {
      return {
        accepted: false,
        alreadyDone: true,
        chunk: this.publicChunk(chunk),
        cancelledPeers: []
      };
    }

    if (!chunk.assignments[peerId]) {
      throw new Error(`Peer ${peerId} does not hold an active assignment for ${chunkId}`);
    }

    delete chunk.assignments[peerId];
    chunk.resultVotes[resultHash] = chunk.resultVotes[resultHash] ?? [];

    if (!chunk.resultVotes[resultHash].includes(peerId)) {
      chunk.resultVotes[resultHash].push(peerId);
    }

    const votesForHash = chunk.resultVotes[resultHash].length;
    const cancelledPeers = [];

    if (votesForHash >= chunk.quorum) {
      cancelledPeers.push(...activeAssignmentIds(chunk));
      chunk.assignments = {};
      chunk.assignedTo = null;
      chunk.resultHash = resultHash;
      chunk.status = STATUS_DONE;
      chunk.doneAt = now;
    } else {
      this.refreshStatus(chunk);
    }

    chunk.updatedAt = now;

    return {
      accepted: true,
      done: chunk.status === STATUS_DONE,
      chunk: this.publicChunk(chunk),
      cancelledPeers
    };
  }

  requeuePeer(peerId, now = Date.now()) {
    const requeued = [];

    for (const chunk of this.chunks.values()) {
      if (chunk.assignments[peerId]) {
        delete chunk.assignments[peerId];
        this.refreshStatus(chunk);
        if (chunk.status === STATUS_PENDING) {
          chunk.attempts = 0;
        }
        chunk.updatedAt = now;
        requeued.push(this.publicChunk(chunk));
      }
    }

    return requeued;
  }

  expireAssignments(now = Date.now()) {
    const expired = [];

    for (const chunk of this.chunks.values()) {
      for (const [peerId, assignment] of Object.entries(chunk.assignments)) {
        if (assignment.expiresAt <= now) {
          delete chunk.assignments[peerId];
          expired.push({
            peerId,
            chunk: this.publicChunk(chunk)
          });
        }
      }

      this.refreshStatus(chunk);
      chunk.updatedAt = now;
    }

    return expired;
  }

  getTaskStatus(taskId) {
    const chunks = [...this.chunks.values()]
      .filter((chunk) => chunk.taskId === taskId)
      .sort((left, right) => left.order - right.order);

    const counts = {
      pending: chunks.filter((chunk) => chunk.status === STATUS_PENDING).length,
      inFlight: chunks.filter((chunk) => chunk.status === STATUS_IN_FLIGHT).length,
      done: chunks.filter((chunk) => chunk.status === STATUS_DONE).length,
      total: chunks.length
    };

    return {
      taskId,
      counts,
      complete: counts.total > 0 && counts.done === counts.total,
      chunks: chunks.map((chunk) => this.publicChunk(chunk)),
      resultHashes: chunks.map((chunk) => chunk.resultHash)
    };
  }

  snapshot() {
    return {
      version: 1,
      assignmentTimeoutMs: this.assignmentTimeoutMs,
      chunks: [...this.chunks.values()].map((chunk) => structuredClone(chunk))
    };
  }

  restore(snapshot) {
    if (!snapshot?.chunks) {
      return;
    }

    this.chunks.clear();

    for (const chunk of snapshot.chunks) {
      this.chunks.set(chunk.chunkId, {
        ...chunk,
        assignments: chunk.assignments ?? {},
        resultVotes: chunk.resultVotes ?? {}
      });
    }
  }

  canAssign(chunk, peerId) {
    if (chunk.status === STATUS_DONE) {
      return false;
    }

    if (chunk.assignments[peerId]) {
      return false;
    }

    if (completedPeerIds(chunk).includes(peerId)) {
      return false;
    }

    const activeCount = activeAssignmentIds(chunk).length;
    const completedCount = completedPeerIds(chunk).length;

    return activeCount + completedCount < chunk.replicas;
  }

  refreshStatus(chunk) {
    if (chunk.status === STATUS_DONE) {
      return;
    }

    const activeIds = activeAssignmentIds(chunk);
    chunk.assignedTo = activeIds[0] ?? null;
    chunk.status = activeIds.length > 0 ? STATUS_IN_FLIGHT : STATUS_PENDING;
  }

  publicAssignment(chunk, peerId) {
    return {
      taskId: chunk.taskId,
      chunkId: chunk.chunkId,
      peerId,
      checksum: chunk.checksum,
      byteLength: chunk.byteLength,
      assignedAt: chunk.assignments[peerId].assignedAt,
      expiresAt: chunk.assignments[peerId].expiresAt,
      replicas: chunk.replicas,
      quorum: chunk.quorum
    };
  }

  publicChunk(chunk) {
    return {
      taskId: chunk.taskId,
      chunkId: chunk.chunkId,
      order: chunk.order,
      checksum: chunk.checksum,
      byteLength: chunk.byteLength,
      priority: chunk.priority,
      replicas: chunk.replicas,
      quorum: chunk.quorum,
      status: chunk.status,
      assignedTo: chunk.assignedTo,
      assignedPeerIds: activeAssignmentIds(chunk),
      resultHash: chunk.resultHash,
      resultVotes: chunk.resultVotes,
      attempts: chunk.attempts,
      submittedBy: chunk.submittedBy,
      createdAt: chunk.createdAt,
      updatedAt: chunk.updatedAt,
      doneAt: chunk.doneAt
    };
  }

  requireChunk(chunkId) {
    const chunk = this.chunks.get(chunkId);

    if (!chunk) {
      throw new Error(`Unknown chunk: ${chunkId}`);
    }

    return chunk;
  }
}
