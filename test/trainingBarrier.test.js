import assert from "node:assert/strict";
import test from "node:test";
import { hashGradient } from "../src/gradientAggregator.js";
import { TrainingBarrierRegistry } from "../src/trainingBarrier.js";

test("training barrier aggregates gradients once all expected peers report", () => {
  const barriers = new TrainingBarrierRegistry({ timeoutMs: 1000 });
  const taskId = "task-1";
  const stepId = "step-1";

  barriers.startBarrier({
    taskId,
    stepId,
    ownerId: "coordinator",
    expectedPeerIds: ["peer-a", "peer-b"],
    now: 0
  });

  const firstGradient = { weights: [1, 3], bias: [2] };
  const secondGradient = { weights: [3, 5], bias: [4] };

  const waiting = barriers.recordGradient({
    taskId,
    stepId,
    peerId: "peer-a",
    gradients: firstGradient,
    gradientHash: hashGradient(firstGradient),
    loss: 2,
    now: 10
  });

  assert.equal(waiting.status, "waiting");
  assert.deepEqual(waiting.receivedPeerIds, ["peer-a"]);

  const aggregated = barriers.recordGradient({
    taskId,
    stepId,
    peerId: "peer-b",
    gradients: secondGradient,
    gradientHash: hashGradient(secondGradient),
    loss: 4,
    now: 20
  });

  assert.equal(aggregated.status, "aggregated");
  assert.deepEqual(aggregated.aggregate, {
    bias: [3],
    weights: [2, 4]
  });
  assert.equal(aggregated.losses.length, 2);
});

test("training barrier rejects mismatched gradient hashes", () => {
  const barriers = new TrainingBarrierRegistry({ timeoutMs: 1000 });

  barriers.startBarrier({
    taskId: "task-1",
    stepId: "step-1",
    ownerId: "coordinator",
    expectedPeerIds: ["peer-a"],
    now: 0
  });

  assert.throws(() => barriers.recordGradient({
    taskId: "task-1",
    stepId: "step-1",
    peerId: "peer-a",
    gradients: { weights: [1] },
    gradientHash: "0".repeat(64),
    now: 10
  }), /hash mismatch/);
});

test("training barrier expires missing peers into rollback notices", () => {
  const barriers = new TrainingBarrierRegistry({ timeoutMs: 100 });

  barriers.startBarrier({
    taskId: "task-1",
    stepId: "step-1",
    ownerId: "coordinator",
    expectedPeerIds: ["peer-a", "peer-b"],
    now: 0
  });

  const expired = barriers.expire(101);

  assert.equal(expired.length, 1);
  assert.deepEqual(expired[0].missingPeerIds, ["peer-a", "peer-b"]);
  assert.equal(barriers.get("task-1", "step-1"), null);
});
