import assert from "node:assert/strict";
import test from "node:test";
import { buildMerkleRoot, sha256Hex } from "../src/hash.js";

test("sha256Hex hashes strings", () => {
  assert.equal(
    sha256Hex("abc"),
    "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
  );
});

test("buildMerkleRoot is stable for odd leaf counts", () => {
  const leaves = ["a", "b", "c"].map((value) => sha256Hex(value));
  const root = buildMerkleRoot(leaves);

  assert.equal(root.length, 64);
  assert.equal(root, buildMerkleRoot(leaves));
});

test("buildMerkleRoot returns null for empty input", () => {
  assert.equal(buildMerkleRoot([]), null);
});
