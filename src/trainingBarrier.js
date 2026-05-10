import { averageGradients, gradientNorm, hashGradient } from "./gradientAggregator.js";

export class TrainingBarrier {
  constructor({
    taskId,
    stepId,
    expectedPeerIds,
    ownerId,
    timeoutMs = 120000,
    now = Date.now()
  }) {
    if (!taskId) {
      throw new Error("TrainingBarrier requires a taskId");
    }

    if (!stepId) {
      throw new Error("TrainingBarrier requires a stepId");
    }

    if (!Array.isArray(expectedPeerIds) || expectedPeerIds.length === 0) {
      throw new Error("TrainingBarrier requires at least one expected peer");
    }

    this.taskId = taskId;
    this.stepId = stepId;
    this.expectedPeerIds = [...new Set(expectedPeerIds)];
    this.ownerId = ownerId;
    this.timeoutMs = timeoutMs;
    this.createdAt = now;
    this.updatedAt = now;
    this.received = new Map();
    this.completedPeers = new Set();
    this.status = "waiting";
    this.aggregate = null;
    this.aggregateHash = null;
  }

  get key() {
    return TrainingBarrier.key(this.taskId, this.stepId);
  }

  static key(taskId, stepId) {
    return `${taskId}:${stepId}`;
  }

  recordGradient({
    peerId,
    gradients,
    gradientHash,
    loss = null,
    byteLength = null,
    now = Date.now()
  }) {
    if (this.status !== "waiting") {
      throw new Error(`Barrier ${this.key} is already ${this.status}`);
    }

    if (!this.expectedPeerIds.includes(peerId)) {
      throw new Error(`Peer ${peerId} is not expected for barrier ${this.key}`);
    }

    if (!gradients || typeof gradients !== "object" || Array.isArray(gradients)) {
      throw new Error("gradientReady requires an inline gradients object for this barrier");
    }

    const actualHash = hashGradient(gradients);

    if (gradientHash && gradientHash !== actualHash) {
      throw new Error(`Gradient hash mismatch for ${peerId}`);
    }

    this.received.set(peerId, {
      peerId,
      gradients,
      gradientHash: actualHash,
      loss,
      byteLength,
      norm: gradientNorm(gradients),
      receivedAt: now
    });
    this.updatedAt = now;

    if (this.received.size === this.expectedPeerIds.length) {
      const gradientsToAverage = [...this.received.values()].map((entry) => entry.gradients);
      this.aggregate = averageGradients(gradientsToAverage);
      this.aggregateHash = hashGradient(this.aggregate);
      this.status = "aggregated";
    }

    return this.publicStatus();
  }

  markStepComplete({ peerId, now = Date.now() }) {
    if (!this.expectedPeerIds.includes(peerId)) {
      throw new Error(`Peer ${peerId} is not expected for barrier ${this.key}`);
    }

    this.completedPeers.add(peerId);
    this.updatedAt = now;

    if (this.completedPeers.size === this.expectedPeerIds.length) {
      this.status = "complete";
    }

    return this.publicStatus();
  }

  expire(now = Date.now()) {
    if (this.status !== "waiting") {
      return null;
    }

    if (now - this.createdAt <= this.timeoutMs) {
      return null;
    }

    this.status = "rollback";
    this.updatedAt = now;

    return {
      taskId: this.taskId,
      stepId: this.stepId,
      ownerId: this.ownerId,
      reason: "barrier_timeout",
      expectedPeerIds: [...this.expectedPeerIds],
      missingPeerIds: this.expectedPeerIds.filter((peerId) => !this.received.has(peerId))
    };
  }

  publicStatus() {
    return {
      taskId: this.taskId,
      stepId: this.stepId,
      ownerId: this.ownerId,
      status: this.status,
      expectedPeerIds: [...this.expectedPeerIds],
      receivedPeerIds: [...this.received.keys()],
      completedPeerIds: [...this.completedPeers],
      aggregate: this.aggregate,
      aggregateHash: this.aggregateHash,
      losses: [...this.received.values()].map((entry) => ({
        peerId: entry.peerId,
        loss: entry.loss,
        norm: entry.norm
      }))
    };
  }
}

export class TrainingBarrierRegistry {
  constructor({ timeoutMs = 120000 } = {}) {
    this.timeoutMs = timeoutMs;
    this.barriers = new Map();
  }

  startBarrier({ taskId, stepId, expectedPeerIds, ownerId, now = Date.now() }) {
    const barrier = new TrainingBarrier({
      taskId,
      stepId,
      expectedPeerIds,
      ownerId,
      timeoutMs: this.timeoutMs,
      now
    });

    this.barriers.set(barrier.key, barrier);
    return barrier.publicStatus();
  }

  get(taskId, stepId) {
    return this.barriers.get(TrainingBarrier.key(taskId, stepId)) ?? null;
  }

  recordGradient({ taskId, stepId, ...entry }) {
    const barrier = this.get(taskId, stepId);

    if (!barrier) {
      throw new Error(`Unknown training barrier: ${TrainingBarrier.key(taskId, stepId)}`);
    }

    return barrier.recordGradient(entry);
  }

  markStepComplete({ taskId, stepId, ...entry }) {
    const barrier = this.get(taskId, stepId);

    if (!barrier) {
      throw new Error(`Unknown training barrier: ${TrainingBarrier.key(taskId, stepId)}`);
    }

    const status = barrier.markStepComplete(entry);

    if (status.status === "complete") {
      this.barriers.delete(barrier.key);
    }

    return status;
  }

  expire(now = Date.now()) {
    const expired = [];

    for (const [key, barrier] of this.barriers) {
      const rollback = barrier.expire(now);

      if (rollback) {
        this.barriers.delete(key);
        expired.push(rollback);
      }
    }

    return expired;
  }
}
