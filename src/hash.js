import { createHash } from "node:crypto";

export function normalizeInput(input) {
  if (Buffer.isBuffer(input)) {
    return input;
  }

  if (input instanceof ArrayBuffer) {
    return Buffer.from(input);
  }

  if (ArrayBuffer.isView(input)) {
    return Buffer.from(input.buffer, input.byteOffset, input.byteLength);
  }

  if (typeof input === "string") {
    return Buffer.from(input, "utf8");
  }

  return Buffer.from(JSON.stringify(input), "utf8");
}

export function sha256Hex(input) {
  return createHash("sha256").update(normalizeInput(input)).digest("hex");
}

export function buildMerkleRoot(hashes) {
  if (!Array.isArray(hashes) || hashes.length === 0) {
    return null;
  }

  let layer = hashes.map((hash) => {
    if (typeof hash !== "string" || hash.length === 0) {
      throw new Error("Merkle leaves must be non-empty hash strings");
    }

    return hash;
  });

  while (layer.length > 1) {
    const nextLayer = [];

    for (let index = 0; index < layer.length; index += 2) {
      const left = layer[index];
      const right = layer[index + 1] ?? left;
      nextLayer.push(sha256Hex(`${left}${right}`));
    }

    layer = nextLayer;
  }

  return layer[0];
}
