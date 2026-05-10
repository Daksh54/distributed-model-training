import {
  decodeTrainingPayload,
  runTrainingChunk,
  stableStringify
} from "/browserTrainingKernel.js";

let wasmKernelPromise;

async function digestHex(buffer) {
  const hash = await crypto.subtle.digest("SHA-256", buffer);
  return [...new Uint8Array(hash)]
    .map((byte) => byte.toString(16).padStart(2, "0"))
    .join("");
}

async function loadWasmKernel() {
  if (!wasmKernelPromise) {
    wasmKernelPromise = import("/wasm_kernel/distributed_ml_kernel.js")
      .then(async (module) => {
        await module.default();
        return module;
      })
      .catch(() => null);
  }

  return wasmKernelPromise;
}

async function runKernel(payloadBuffer) {
  const payload = decodeTrainingPayload(payloadBuffer);
  const wasmKernel = await loadWasmKernel();
  const result = wasmKernel?.run_training_chunk
    ? JSON.parse(wasmKernel.run_training_chunk(stableStringify(payload)))
    : runTrainingChunk(payload);
  const resultBytes = new TextEncoder().encode(stableStringify(result));

  return {
    result,
    resultHash: await digestHex(resultBytes)
  };
}

self.addEventListener("message", async (event) => {
  try {
    const output = await runKernel(event.data.payload);

    self.postMessage({
      chunkId: event.data.chunkId,
      ...output
    });
  } catch (error) {
    self.postMessage({
      chunkId: event.data.chunkId,
      error: error instanceof Error ? error.message : String(error)
    });
  }
});
