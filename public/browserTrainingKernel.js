const DEFAULT_WEIGHTS = [0.05, -0.02];
const DEFAULT_BIAS = 0.1;

function assertFiniteNumber(value, name) {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new Error(`${name} must be a finite number`);
  }
}

function dot(left, right) {
  return left.reduce((sum, value, index) => sum + value * right[index], 0);
}

function stableCopy(value) {
  if (Array.isArray(value)) {
    return value.map((item) => stableCopy(item));
  }

  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.keys(value)
        .sort()
        .map((key) => [key, stableCopy(value[key])])
    );
  }

  return value;
}

export function stableStringify(value) {
  return JSON.stringify(stableCopy(value));
}

export function bytesToTrainingPayload(bytes, {
  chunkId,
  order = 0,
  sourceLength = bytes.length,
  weights = DEFAULT_WEIGHTS,
  bias = DEFAULT_BIAS,
  learningRate = 0.05
} = {}) {
  const features = [];
  const labels = [];
  const denominator = Math.max(1, sourceLength - 1);

  for (let index = 0; index < bytes.length; index += 1) {
    const byte = bytes[index];
    features.push([
      byte / 255,
      (order + index) / denominator
    ]);
    labels.push(byte > 127 ? 1 : 0);
  }

  return {
    kernelType: "single_neuron_mse_v1",
    architecture: {
      type: "single_neuron",
      inputSize: 2,
      loss: "mean_squared_error"
    },
    chunkId,
    weights,
    bias,
    learningRate,
    features,
    labels
  };
}

export function encodeTrainingPayload(payload) {
  return new TextEncoder().encode(stableStringify(payload));
}

export function decodeTrainingPayload(buffer) {
  const text = new TextDecoder().decode(buffer);
  return JSON.parse(text);
}

export function runTrainingChunk(payload) {
  if (payload.kernelType !== "single_neuron_mse_v1") {
    throw new Error(`Unsupported browser kernel: ${payload.kernelType}`);
  }

  if (!Array.isArray(payload.features) || payload.features.length === 0) {
    throw new Error("Training payload requires at least one feature row");
  }

  if (!Array.isArray(payload.labels) || payload.labels.length !== payload.features.length) {
    throw new Error("Training labels must match feature rows");
  }

  if (!Array.isArray(payload.weights) || payload.weights.length !== payload.features[0].length) {
    throw new Error("Training weights must match feature width");
  }

  const gradients = {
    weights: Array.from({ length: payload.weights.length }, () => 0),
    bias: [0]
  };
  let loss = 0;

  for (let rowIndex = 0; rowIndex < payload.features.length; rowIndex += 1) {
    const features = payload.features[rowIndex];
    const label = payload.labels[rowIndex];
    const prediction = dot(features, payload.weights) + payload.bias;
    const error = prediction - label;

    assertFiniteNumber(label, `labels[${rowIndex}]`);
    loss += error * error;

    for (let index = 0; index < features.length; index += 1) {
      assertFiniteNumber(features[index], `features[${rowIndex}][${index}]`);
      gradients.weights[index] += (2 * error * features[index]) / payload.features.length;
    }

    gradients.bias[0] += (2 * error) / payload.features.length;
  }

  return {
    kernelType: payload.kernelType,
    loss: loss / payload.features.length,
    gradients,
    sampleCount: payload.features.length,
    architecture: payload.architecture
  };
}
