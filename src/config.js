export const config = {
  port: Number.parseInt(process.env.PORT ?? "3000", 10),
  heartbeatIntervalMs: Number.parseInt(process.env.HEARTBEAT_INTERVAL_MS ?? "10000", 10),
  stalePeerMs: Number.parseInt(process.env.STALE_PEER_MS ?? "15000", 10),
  chunkTimeoutMs: Number.parseInt(process.env.CHUNK_TIMEOUT_MS ?? "30000", 10),
  redisUrl: process.env.REDIS_URL,
  redisKeyPrefix: process.env.REDIS_KEY_PREFIX ?? "dbc"
};
