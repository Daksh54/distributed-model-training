import assert from "node:assert/strict";
import test from "node:test";
import { ChunkQueue } from "../src/chunkQueue.js";
import { sha256Hex } from "../src/hash.js";

test("queue submits and assigns chunks by priority", () => {
  const queue = new ChunkQueue({ assignmentTimeoutMs: 1000 });
  const task = queue.submitTask({
    taskId: "task-1",
    submittedBy: "owner",
    chunks: [
      { chunkId: "low", checksum: sha256Hex("low"), priority: 0 },
      { chunkId: "high", checksum: sha256Hex("high"), priority: 10 }
    ],
    now: 0
  });

  assert.equal(task.totalChunks, 2);

  const assignment = queue.assignNext({
    taskId: "task-1",
    peerId: "peer-a",
    now: 10
  });

  assert.equal(assignment.chunkId, "high");
});

test("queue requeues chunks when a peer disappears", () => {
  const queue = new ChunkQueue();

  queue.submitTask({
    taskId: "task-1",
    submittedBy: "owner",
    chunks: [{ chunkId: "chunk-1", checksum: sha256Hex("payload") }]
  });
  queue.assignChunk({
    chunkId: "chunk-1",
    peerId: "peer-a",
    now: 0
  });

  const requeued = queue.requeuePeer("peer-a", 10);

  assert.equal(requeued[0].status, "pending");
  assert.equal(requeued[0].attempts, 0);
  assert.equal(queue.getTaskStatus("task-1").counts.pending, 1);
});

test("queue expires stalled assignments", () => {
  const queue = new ChunkQueue({ assignmentTimeoutMs: 30 });

  queue.submitTask({
    taskId: "task-1",
    submittedBy: "owner",
    chunks: [{ chunkId: "chunk-1", checksum: sha256Hex("payload") }],
    now: 0
  });
  queue.assignChunk({
    chunkId: "chunk-1",
    peerId: "peer-a",
    now: 0
  });

  const expired = queue.expireAssignments(31);

  assert.equal(expired[0].peerId, "peer-a");
  assert.equal(queue.getTaskStatus("task-1").counts.pending, 1);

  const reassignment = queue.assignNext({
    taskId: "task-1",
    peerId: "peer-b",
    now: 32
  });

  assert.equal(reassignment.chunkId, "chunk-1");
  assert.equal(reassignment.peerId, "peer-b");
});

test("queue can fail a single assignment without dropping other replicas", () => {
  const queue = new ChunkQueue();

  queue.submitTask({
    taskId: "task-1",
    submittedBy: "owner",
    chunks: [{
      chunkId: "chunk-1",
      checksum: sha256Hex("payload"),
      replicas: 2,
      quorum: 1
    }]
  });
  queue.assignChunk({
    chunkId: "chunk-1",
    peerId: "peer-a",
    now: 0
  });
  queue.assignChunk({
    chunkId: "chunk-1",
    peerId: "peer-b",
    now: 0
  });

  const failed = queue.failAssignment({
    chunkId: "chunk-1",
    peerId: "peer-a",
    now: 1
  });

  assert.equal(failed.status, "in-flight");
  assert.deepEqual(failed.assignedPeerIds, ["peer-b"]);
});

test("queue stores result hashes only after quorum", () => {
  const queue = new ChunkQueue();
  const resultHash = sha256Hex("result");

  queue.submitTask({
    taskId: "task-1",
    submittedBy: "owner",
    chunks: [{
      chunkId: "chunk-1",
      checksum: sha256Hex("payload"),
      replicas: 2,
      quorum: 2
    }]
  });
  queue.assignChunk({
    chunkId: "chunk-1",
    peerId: "peer-a",
    now: 0
  });
  queue.assignChunk({
    chunkId: "chunk-1",
    peerId: "peer-b",
    now: 0
  });

  const firstVote = queue.markResult({
    chunkId: "chunk-1",
    peerId: "peer-a",
    resultHash,
    now: 1
  });

  assert.equal(firstVote.done, false);
  assert.equal(queue.getTaskStatus("task-1").counts.done, 0);

  const secondVote = queue.markResult({
    chunkId: "chunk-1",
    peerId: "peer-b",
    resultHash,
    now: 2
  });

  assert.equal(secondVote.done, true);
  assert.equal(queue.getTaskStatus("task-1").resultHashes[0], resultHash);
});

test("queue returns peers to cancel when quorum completes early", () => {
  const queue = new ChunkQueue();
  const resultHash = sha256Hex("result");

  queue.submitTask({
    taskId: "task-1",
    submittedBy: "owner",
    chunks: [{
      chunkId: "chunk-1",
      checksum: sha256Hex("payload"),
      replicas: 3,
      quorum: 2
    }]
  });
  queue.assignChunk({
    chunkId: "chunk-1",
    peerId: "peer-a",
    now: 0
  });
  queue.assignChunk({
    chunkId: "chunk-1",
    peerId: "peer-b",
    now: 0
  });
  queue.assignChunk({
    chunkId: "chunk-1",
    peerId: "peer-c",
    now: 0
  });

  const firstVote = queue.markResult({
    chunkId: "chunk-1",
    peerId: "peer-a",
    resultHash,
    now: 1
  });
  const secondVote = queue.markResult({
    chunkId: "chunk-1",
    peerId: "peer-b",
    resultHash,
    now: 2
  });
  const status = queue.getTaskStatus("task-1");

  assert.deepEqual(firstVote.cancelledPeers, []);
  assert.deepEqual(secondVote.cancelledPeers, ["peer-c"]);
  assert.equal(secondVote.done, true);
  assert.equal(status.counts.done, 1);
  assert.deepEqual(status.chunks[0].assignedPeerIds, []);
});
