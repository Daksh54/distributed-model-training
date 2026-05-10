import assert from "node:assert/strict";
import test from "node:test";
import { PeerRegistry } from "../src/peerRegistry.js";

function socket() {
  return {
    readyState: 1,
    OPEN: 1,
    close() {
      this.readyState = 3;
    },
    send() {}
  };
}

test("registry tracks capacity and available volunteers", () => {
  const registry = new PeerRegistry({ stalePeerMs: 1000 });

  registry.connect(socket(), {
    nodeId: "slow",
    capacityScore: 5,
    now: 0
  });
  registry.connect(socket(), {
    nodeId: "fast",
    capacityScore: 50,
    now: 0
  });

  const peers = registry.getAvailablePeers({ now: 500 });

  assert.deepEqual(peers.map((peer) => peer.nodeId), ["fast", "slow"]);
});

test("registry evicts stale peers", () => {
  const registry = new PeerRegistry({ stalePeerMs: 1000 });

  registry.connect(socket(), {
    nodeId: "peer-a",
    now: 0
  });

  assert.equal(registry.evictStale(1000).length, 0);
  assert.deepEqual(registry.evictStale(1001).map((peer) => peer.nodeId), ["peer-a"]);
});

test("registry maintains assigned chunk IDs", () => {
  const registry = new PeerRegistry();

  registry.connect(socket(), {
    nodeId: "peer-a"
  });
  registry.assignChunk("peer-a", "chunk-1");
  registry.unassignChunk("peer-a", "chunk-1");

  assert.deepEqual(registry.snapshot().peers[0].assignedChunkIds, []);
});

test("registry stores native agent capabilities", () => {
  const registry = new PeerRegistry();

  registry.connect(socket(), {
    nodeId: "agent-a",
    role: "volunteer-native"
  });
  registry.update("agent-a", {
    capabilities: {
      cpuCores: 8,
      memGB: 16,
      gpu: { type: "cuda", vramGB: 8 }
    }
  });

  const [agent] = registry.getAvailablePeers({
    role: "volunteer-native"
  });

  assert.equal(agent.capabilities.gpu.type, "cuda");
  assert.equal(registry.snapshot().peers[0].capabilities.cpuCores, 8);
});
