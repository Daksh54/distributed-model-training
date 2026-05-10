#!/usr/bin/env node
import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";
import WebSocket from "ws";
import { hashGradient } from "./gradientAggregator.js";
import { sha256Hex } from "./hash.js";

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

function send(socket, message) {
  if (socket.readyState === WebSocket.OPEN) {
    socket.send(JSON.stringify({
      requestId: randomUUID(),
      ...message
    }));
  }
}

function splitRows(features, labels, parts) {
  return Array.from({ length: parts }, (_, index) => ({
    features: features.filter((_, rowIndex) => rowIndex % parts === index),
    labels: labels.filter((_, rowIndex) => rowIndex % parts === index)
  })).filter((batch) => batch.features.length > 0);
}

function buildToyStep(peerIds) {
  const features = [
    [0, 0],
    [0, 1],
    [1, 0],
    [1, 1],
    [2, 1],
    [2, 2],
    [3, 1],
    [3, 2]
  ];
  const labels = features.map(([left, right]) => left * 2 + right * 3 + 1);
  const weights = [0, 0];
  const bias = 0;
  const batches = splitRows(features, labels, peerIds.length);
  const taskId = randomUUID();
  const stepId = "step-1";

  return {
    taskId,
    stepId,
    weights,
    bias,
    assignments: batches.map((batch, index) => {
      const payload = {
        ...batch,
        weights,
        bias,
        lr: 0.01
      };

      return {
        peerId: peerIds[index],
        chunkId: `${taskId}:${index}`,
        payload,
        checksum: sha256Hex(payload)
      };
    })
  };
}

export function startCoordinator({
  server = "ws://localhost:3000",
  token,
  peerLimit = 2,
  minPeers = 1
} = {}) {
  const socket = new WebSocket(server);
  const state = {
    nodeId: null,
    heartbeatTimer: null,
    started: false,
    pendingStep: null
  };

  socket.on("open", () => {
    send(socket, {
      type: "register",
      role: "coordinator",
      token,
      capacityScore: 0
    });
  });

  socket.on("message", (raw) => {
    const message = JSON.parse(raw.toString("utf8"));

    if (message.type === "welcome") {
      state.nodeId = message.nodeId;
      clearInterval(state.heartbeatTimer);
      state.heartbeatTimer = setInterval(() => {
        send(socket, {
          type: "heartbeat",
          capacityScore: 0
        });
      }, message.heartbeatIntervalMs);
      console.log(`coordinator connected as ${message.nodeId}`);
      return;
    }

    if (message.type === "registered" && !state.started) {
      state.started = true;
      send(socket, {
        type: "requestPeers",
        role: "volunteer-native",
        limit: peerLimit
      });
      return;
    }

    if (message.type === "peerList") {
      if (message.peers.length < minPeers) {
        console.log(`waiting for ${minPeers} native agent(s); found ${message.peers.length}`);
        return;
      }

      const peerIds = message.peers.slice(0, peerLimit).map((peer) => peer.nodeId);
      const step = buildToyStep(peerIds);
      state.pendingStep = step;

      send(socket, {
        type: "startTrainingStep",
        taskId: step.taskId,
        stepId: step.stepId,
        expectedPeerIds: peerIds
      });
      send(socket, {
        type: "submitTask",
        taskId: step.taskId,
        chunks: step.assignments.map((assignment) => ({
          chunkId: assignment.chunkId,
          checksum: assignment.checksum,
          replicas: 1,
          quorum: 1
        })),
        peerLimit
      });
      return;
    }

    if (message.type === "taskAccepted" && state.pendingStep?.taskId === message.taskId) {
      for (const assignment of state.pendingStep.assignments) {
        send(socket, {
          type: "assignChunk",
          taskId: state.pendingStep.taskId,
          stepId: state.pendingStep.stepId,
          chunkId: assignment.chunkId,
          peerId: assignment.peerId,
          kernel: {
            type: "data_parallel"
          },
          payload: assignment.payload
        });
      }
      return;
    }

    if (message.type === "barrierReached") {
      const gradientHash = message.gradients ? hashGradient(message.gradients) : message.gradientHash;
      console.log(`barrier reached for ${message.taskId}/${message.stepId} hash=${gradientHash}`);
      console.log(JSON.stringify({
        losses: message.losses,
        gradients: message.gradients
      }, null, 2));
      socket.close();
      return;
    }

    if (message.type === "stepRollback") {
      console.error(`step rollback: ${message.reason}`);
      socket.close();
      return;
    }

    if (message.type === "error") {
      console.error(message.message);
    }
  });

  socket.on("close", () => {
    clearInterval(state.heartbeatTimer);
  });

  return socket;
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  const args = parseArgs(process.argv);

  startCoordinator({
    server: args.get("server") ?? process.env.COORDINATOR_SERVER ?? "ws://localhost:3000",
    token: args.get("token") ?? process.env.INVITE_TOKEN,
    peerLimit: Number.parseInt(args.get("peers") ?? "2", 10),
    minPeers: Number.parseInt(args.get("min-peers") ?? "1", 10)
  });
}
