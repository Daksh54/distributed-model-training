import assert from "node:assert/strict";
import test from "node:test";
import { runKernel, runLinearRegressionKernel } from "../agent/kernelRunner.js";

const payload = {
  features: [[1, 2], [3, 4]],
  labels: [1, 2],
  weights: [0.1, 0.2],
  bias: 0
};

test("linear regression kernel returns gradients and loss", () => {
  const result = runLinearRegressionKernel(payload);

  assert.equal(result.gradients.weights.length, 2);
  assert.equal(result.gradients.bias.length, 1);
  assert.equal(result.metrics.samples, 2);
  assert.ok(result.loss > 0);
});

test("runKernel dispatches data_parallel to the local JS kernel", async () => {
  const result = await runKernel({
    kernel: { type: "data_parallel" },
    payload
  });

  assert.deepEqual(Object.keys(result.gradients).sort(), ["bias", "weights"]);
});
