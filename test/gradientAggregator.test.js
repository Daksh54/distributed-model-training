import assert from "node:assert/strict";
import test from "node:test";
import { averageGradients, gradientNorm, hashGradient } from "../src/gradientAggregator.js";

test("averageGradients averages nested tensor values", () => {
  const averaged = averageGradients([
    {
      bias: [2, 4],
      weights: [[1, 3], [5, 7]]
    },
    {
      bias: [4, 8],
      weights: [[3, 5], [7, 9]]
    }
  ]);

  assert.deepEqual(averaged, {
    bias: [3, 6],
    weights: [[2, 4], [6, 8]]
  });
});

test("averageGradients rejects mismatched tensor shapes", () => {
  assert.throws(() => averageGradients([
    { weights: [1, 2] },
    { weights: [1, 2, 3] }
  ]), /length mismatch/);
});

test("hashGradient is stable regardless of object key order", () => {
  assert.equal(
    hashGradient({ z: [1], a: [2] }),
    hashGradient({ a: [2], z: [1] })
  );
});

test("gradientNorm computes an L2 norm", () => {
  assert.equal(gradientNorm({ weights: [3, 4] }), 5);
});
