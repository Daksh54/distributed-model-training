import fs from "node:fs/promises";
import path from "node:path";

export class WeightCache {
  constructor({ directory = process.env.WEIGHT_CACHE_DIR ?? ".weight_cache" } = {}) {
    this.directory = directory;
  }

  async put({ weightHash, payload }) {
    if (!weightHash) {
      throw new Error("weightHash is required to cache weights");
    }

    await fs.mkdir(this.directory, { recursive: true });
    const filePath = this.pathFor(weightHash);
    await fs.writeFile(filePath, JSON.stringify(payload));
    return filePath;
  }

  async get(weightHash) {
    try {
      const body = await fs.readFile(this.pathFor(weightHash), "utf8");
      return JSON.parse(body);
    } catch (error) {
      if (error.code === "ENOENT") {
        return null;
      }

      throw error;
    }
  }

  pathFor(weightHash) {
    return path.join(this.directory, `${weightHash}.json`);
  }
}
