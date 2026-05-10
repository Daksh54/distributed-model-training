#!/usr/bin/env node
import os from "node:os";
import { execFileSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";
import WebSocket from "ws";
import { hashGradient } from "../src/gradientAggregator.js";
import { sha256Hex } from "../src/hash.js";
import { runKernel } from "./kernelRunner.js";
import { WeightCache } from "./weightCache.js";

function parseArgs(argv) {
  const args = new Map();

  for (let index = 2; index < argv.length; index += 1) {
    const value = argv[index];

    if (!value.startsWith("--")) {
      continue;
    }

    const key = value.slice(2);
    const next = argv[index + 1];

    if (!next || next.startsWith("--")) {
      args.set(key, true);
    } else {
      args.set(key, next);
      index += 1;
    }
  }

  return args;
}

function tryCommand(command, args, timeout = 3000) {
  try {
    return execFileSync(command, args, {
      timeout,
      windowsHide: true,
      encoding: "utf8"
    }).trim();
  } catch {
    return null;
  }
}

function parseNvidiaSmi(output) {
  if (!output) {
    return null;
  }

  const [name, memory] = output.split(",").map((value) => value.trim());
  const memoryMiB = Number.parseInt(memory, 10);

  return {
    type: "cuda",
    name,
    vramGB: Number.isFinite(memoryMiB) ? Math.round((memoryMiB / 1024) * 10) / 10 : null
  };
}

export function detectCapabilities() {
  const cpuCores = os.cpus().length;
  const memGB = Math.round((os.totalmem() / 1e9) * 10) / 10;
  const nvidia = parseNvidiaSmi(tryCommand("nvidia-smi", [
    "--query-gpu=name,memory.total",
    "--format=csv,noheader,nounits"
  ]));
  const mpsAvailable = !nvidia && tryCommand("python", [
    "-c",
    "import torch; assert torch.backends.mps.is_available(); print('mps')"
  ], 5000);

  return {
    cpuCores,
    memGB,
    platform: os.platform(),
    arch: os.arch(),
    gpu: nvidia ?? (mpsAvailable ? { type: "mps", name: "Apple Silicon", vramGB: null } : null)
  };
}

function scoreCapabilities(capabilities) {
  const gpuBonus = capabilities.gpu?.type === "cuda" ? 100 : capabilities.gpu?.type === "mps" ? 70 : 0;
  return Math.max(1, Math.round(capabilities.cpuCores * 5 + capabilities.memGB + gpuBonus));
}

function makeSender(socket) {
  return (message) => {
    if (socket.readyState === WebSocket.OPEN) {
      socket.send(JSON.stringify({
        requestId: randomUUID(),
        ...message
      }));
    }
  };
}

async function handleAssignment({ message, send, capabilities, weightCache }) {
  const payload = message.payload ?? {};
  const cachedWeights = payload.weightHash ? await weightCache.get(payload.weightHash) : null;
  const kernelPayload = cachedWeights
    ? { ...payload, weights: cachedWeights.weights ?? cachedWeights }
    : payload;
  const output = await runKernel({
    kernel: message.kernel ?? {
      type: message.kernelType ?? "linear_regression"
    },
    payload: {
      ...kernelPayload,
      device: message.device ?? capabilities.gpu?.type ?? "cpu"
    }
  });
  const resultHash = sha256Hex(output);

  send({
    type: "chunkResult",
    taskId: message.taskId,
    chunkId: message.chunkId,
    peerId: message.peerId,
    resultHash
  });

  if (output.gradients && message.stepId) {
    send({
      type: "gradientReady",
      taskId: message.taskId,
      stepId: message.stepId,
      chunkId: message.chunkId,
      gradientHash: hashGradient(output.gradients),
      byteLength: Buffer.byteLength(JSON.stringify(output.gradients)),
      gradients: output.gradients,
      loss: output.loss
    });
  }

  console.log(`completed ${message.chunkId} loss=${output.loss ?? "n/a"}`);
}

export function startAgent({
  server,
  token,
  nodeId,
  role = "volunteer-native",
  weightCacheDir
}) {
  const capabilities = detectCapabilities();
  const url = new URL(server);

  if (nodeId) {
    url.searchParams.set("nodeId", nodeId);
  }

  const socket = new WebSocket(url);
  const send = makeSender(socket);
  const weightCache = new WeightCache({ directory: weightCacheDir });
  let heartbeatTimer = null;

  socket.on("open", () => {
    send({
      type: "register",
      role,
      token,
      capacityScore: scoreCapabilities(capabilities),
      capabilities
    });
    send({
      type: "registerCapabilities",
      capabilities
    });
  });

  socket.on("message", (raw) => {
    const message = JSON.parse(raw.toString("utf8"));

    if (message.type === "welcome") {
      clearInterval(heartbeatTimer);
      heartbeatTimer = setInterval(() => {
        send({
          type: "heartbeat",
          capacityScore: scoreCapabilities(capabilities)
        });
      }, message.heartbeatIntervalMs);
      console.log(`connected as ${message.nodeId}`);
      return;
    }

    if (message.type === "registered") {
      console.log(`registered ${message.role} capacity=${message.capacityScore}`);
      return;
    }

    if (message.type === "weightSync") {
      weightCache.put({
        weightHash: message.weightHash,
        payload: message.weights
      }).then((filePath) => {
        console.log(`cached weights ${message.weightHash} at ${filePath}`);
      }).catch((error) => {
        console.error(error.message);
      });
      return;
    }

    if (message.type === "chunkAssigned") {
      handleAssignment({
        message,
        send,
        capabilities,
        weightCache
      }).catch((error) => {
        console.error(`assignment failed: ${error.message}`);
        send({
          type: "chunkFailed",
          taskId: message.taskId,
          chunkId: message.chunkId,
          error: error.message
        });
      });
      return;
    }

    if (message.type === "cancelChunk") {
      console.log(`cancelled ${message.chunkId}: ${message.reason}`);
      return;
    }

    if (message.type === "aggregatedGradient") {
      console.log(`received aggregate for ${message.taskId}/${message.stepId}: ${message.gradientHash}`);
      send({
        type: "stepComplete",
        taskId: message.taskId,
        stepId: message.stepId
      });
      return;
    }

    if (message.type === "error") {
      console.error(message.message);
    }
  });

  socket.on("close", () => {
    clearInterval(heartbeatTimer);
    console.log("server connection closed");
  });

  socket.on("error", (error) => {
    console.error(error.message);
  });

  return socket;
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  const args = parseArgs(process.argv);
  const server = args.get("server") ?? process.env.AGENT_SERVER ?? "ws://localhost:3000";

  startAgent({
    server,
    token: args.get("token") ?? process.env.INVITE_TOKEN,
    nodeId: args.get("node-id") ?? process.env.NODE_ID,
    weightCacheDir: args.get("weight-cache-dir") ?? process.env.WEIGHT_CACHE_DIR
  });
}
