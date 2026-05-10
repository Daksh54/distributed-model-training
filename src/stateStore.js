export class MemoryStateStore {
  constructor() {
    this.queueSnapshot = null;
  }

  async loadQueueSnapshot() {
    return this.queueSnapshot;
  }

  async saveQueueSnapshot(snapshot) {
    this.queueSnapshot = snapshot;
  }

  async close() {}
}

class RedisStateStore {
  constructor(redis, keyPrefix) {
    this.redis = redis;
    this.keyPrefix = keyPrefix;
  }

  async loadQueueSnapshot() {
    const encoded = await this.redis.get(`${this.keyPrefix}:queue`);
    return encoded ? JSON.parse(encoded) : null;
  }

  async saveQueueSnapshot(snapshot) {
    await this.redis.set(`${this.keyPrefix}:queue`, JSON.stringify(snapshot));
  }

  async close() {
    this.redis.disconnect();
  }
}

export async function createStateStore({ redisUrl, redisKeyPrefix = "dbc" } = {}) {
  if (!redisUrl) {
    return new MemoryStateStore();
  }

  const { default: Redis } = await import("ioredis");
  const redis = new Redis(redisUrl, {
    lazyConnect: true,
    maxRetriesPerRequest: 2
  });

  await redis.connect();
  return new RedisStateStore(redis, redisKeyPrefix);
}
