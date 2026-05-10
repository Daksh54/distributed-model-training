import assert from "node:assert/strict";
import test from "node:test";
import {
  bytesToTrainingPayload,
  decodeTrainingPayload,
  encodeTrainingPayload,
  runTrainingChunk,
  stableStringify
} from "../public/browserTrainingKernel.js";

test("browser training payload encodes text bytes into feature rows", () => {
  const payload = bytesToTrainingPayload(new Uint8Array([65, 200]), {
    chunkId: "task:0",
    sourceLength: 2
  });

  assert.equal(payload.chunkId, "task:0");
  assert.equal(payload.features.length, 2);
  assert.deepEqual(payload.labels, [0, 1]);
});

test("browser training kernel runs forward and backward pass", () => {
  const payload = bytesToTrainingPayload(new Uint8Array([10, 250]), {
    chunkId: "task:0",
    sourceLength: 2
  });
  const result = runTrainingChunk(payload);

  assert.equal(result.kernelType, "single_neuron_mse_v1");
  assert.equal(result.gradients.weights.length, 2);
  assert.equal(result.gradients.bias.length, 1);
  assert.equal(result.sampleCount, 2);
  assert.ok(Number.isFinite(result.loss));
});

test("browser training payload round-trips through stable JSON bytes", () => {
  const payload = bytesToTrainingPayload(new Uint8Array([1, 2, 3]), {
    chunkId: "task:0",
    sourceLength: 3
  });
  const encoded = encodeTrainingPayload(payload);
  const decoded = decodeTrainingPayload(encoded);

  assert.equal(stableStringify(decoded), stableStringify(payload));
});
