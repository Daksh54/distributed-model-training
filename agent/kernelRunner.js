import { spawn } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const DEFAULT_KERNEL_TIMEOUT_MS = 60000;

function dot(left, right) {
  return left.reduce((sum, value, index) => sum + value * right[index], 0);
}

function assertTrainingPayload(payload) {
  if (!Array.isArray(payload?.features) || payload.features.length === 0) {
    throw new Error("Training payload requires non-empty features");
  }

  if (!Array.isArray(payload.labels) || payload.labels.length !== payload.features.length) {
    throw new Error("Training payload labels must match feature rows");
  }

  if (!Array.isArray(payload.weights) || payload.weights.length !== payload.features[0].length) {
    throw new Error("Training payload weights must match feature width");
  }
}

export function runLinearRegressionKernel(payload) {
  assertTrainingPayload(payload);

  const weights = payload.weights;
  const bias = Number(payload.bias ?? 0);
  const gradients = {
    weights: Array.from({ length: weights.length }, () => 0),
    bias: [0]
  };
  let loss = 0;

  for (let rowIndex = 0; rowIndex < payload.features.length; rowIndex += 1) {
    const features = payload.features[rowIndex];
    const label = payload.labels[rowIndex];
    const prediction = dot(features, weights) + bias;
    const error = prediction - label;

    loss += error * error;

    for (let index = 0; index < features.length; index += 1) {
      gradients.weights[index] += (2 * error * features[index]) / payload.features.length;
    }

    gradients.bias[0] += (2 * error) / payload.features.length;
  }

  return {
    gradients,
    loss: loss / payload.features.length,
    metrics: {
      samples: payload.features.length
    }
  };
}

function parsePythonOutput(stdout) {
  const lines = stdout
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);
  const lastLine = lines.at(-1);

  if (!lastLine) {
    throw new Error("Python kernel produced no JSON output");
  }

  return JSON.parse(lastLine);
}

export function runPythonKernel({
  payload,
  scriptPath = path.join(__dirname, "kernels", "train_chunk.py"),
  pythonCommand = process.env.PYTHON ?? "python",
  timeoutMs = DEFAULT_KERNEL_TIMEOUT_MS
}) {
  return new Promise((resolve, reject) => {
    const child = spawn(pythonCommand, [scriptPath, JSON.stringify(payload)], {
      stdio: ["ignore", "pipe", "pipe"]
    });
    let stdout = "";
    let stderr = "";
    let settled = false;
    const timer = setTimeout(() => {
      settled = true;
      child.kill("SIGTERM");
      reject(new Error(`Python kernel timed out after ${timeoutMs}ms`));
    }, timeoutMs);

    child.stdout.on("data", (chunk) => {
      stdout += chunk.toString("utf8");
    });
    child.stderr.on("data", (chunk) => {
      stderr += chunk.toString("utf8");
    });
    child.on("error", (error) => {
      if (!settled) {
        settled = true;
        clearTimeout(timer);
        reject(error);
      }
    });
    child.on("close", (code) => {
      if (settled) {
        return;
      }

      settled = true;
      clearTimeout(timer);

      if (code !== 0) {
        reject(new Error(`Python kernel exited with ${code}: ${stderr.trim()}`));
        return;
      }

      try {
        resolve(parsePythonOutput(stdout));
      } catch (error) {
        reject(error);
      }
    });
  });
}

export async function runKernel({ kernel = {}, payload }) {
  const kernelType = kernel.type ?? kernel.kernelType ?? "linear_regression";

  if (kernelType === "python" || kernelType === "pytorch" || kernel.runner === "python") {
    return runPythonKernel({
      payload,
      scriptPath: kernel.scriptPath,
      pythonCommand: kernel.pythonCommand,
      timeoutMs: kernel.timeoutMs
    });
  }

  if (kernelType === "linear_regression" || kernelType === "data_parallel") {
    return runLinearRegressionKernel(payload);
  }

  throw new Error(`Unsupported kernel type: ${kernelType}`);
}
