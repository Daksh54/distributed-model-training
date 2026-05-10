function parseInteger(value, fallback) {
  const parsed = Number.parseInt(value ?? String(fallback), 10);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function parseTokens(value) {
  return new Set((value ?? "")
    .split(",")
    .map((token) => token.trim())
    .filter(Boolean));
}

export const config = {
  port: parseInteger(process.env.PORT, 3000),
  heartbeatIntervalMs: parseInteger(process.env.HEARTBEAT_INTERVAL_MS, 10000),
  stalePeerMs: parseInteger(process.env.STALE_PEER_MS, 15000),
  chunkTimeoutMs: parseInteger(process.env.CHUNK_TIMEOUT_MS, 30000),
  redisUrl: process.env.REDIS_URL,
  redisKeyPrefix: process.env.REDIS_KEY_PREFIX ?? "dbc",
  inviteTokens: parseTokens(process.env.INVITE_TOKENS),
  trainingMode: process.env.TRAINING_MODE ?? "data_parallel",
  gradientCompression: process.env.GRADIENT_COMPRESSION ?? "float16",
  checkpointEvery: parseInteger(process.env.CHECKPOINT_EVERY, 100),
  checkpointDir: process.env.CHECKPOINT_DIR ?? "./checkpoints",
  barrierTimeoutMs: parseInteger(process.env.BARRIER_TIMEOUT_MS, 120000),
  minAgentsForStep: parseInteger(process.env.MIN_AGENTS_FOR_STEP, 1),
  weightCacheDir: process.env.WEIGHT_CACHE_DIR ?? "./weight_cache",
  tlsCert: process.env.TLS_CERT,
  tlsKey: process.env.TLS_KEY,
  tlsCa: process.env.TLS_CA
};
