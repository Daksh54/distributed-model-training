async function digestHex(buffer) {
  const hash = await crypto.subtle.digest("SHA-256", buffer);
  return [...new Uint8Array(hash)]
    .map((byte) => byte.toString(16).padStart(2, "0"))
    .join("");
}

async function runKernel(payload) {
  const bytes = new Uint8Array(payload);
  let sum = 0;

  for (const byte of bytes) {
    sum = (sum + byte) >>> 0;
  }

  const resultBuffer = new ArrayBuffer(4);
  new DataView(resultBuffer).setUint32(0, sum);

  return {
    result: sum,
    resultHash: await digestHex(resultBuffer)
  };
}

self.addEventListener("message", async (event) => {
  const output = await runKernel(event.data.payload);

  self.postMessage({
    chunkId: event.data.chunkId,
    ...output
  });
});
