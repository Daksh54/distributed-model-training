import { sha256Hex } from "./hash.js";

function assertFiniteNumber(value, path) {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new Error(`Gradient value at ${path} must be a finite number`);
  }
}

function assertSameShape(left, right, path) {
  if (Array.isArray(left) !== Array.isArray(right)) {
    throw new Error(`Gradient shape mismatch at ${path}`);
  }

  if (Array.isArray(left)) {
    if (left.length !== right.length) {
      throw new Error(`Gradient length mismatch at ${path}`);
    }

    for (let index = 0; index < left.length; index += 1) {
      assertSameShape(left[index], right[index], `${path}[${index}]`);
    }
  }
}

function addGradientValues(left, right, path) {
  assertSameShape(left, right, path);

  if (Array.isArray(left)) {
    return left.map((value, index) => addGradientValues(value, right[index], `${path}[${index}]`));
  }

  assertFiniteNumber(left, path);
  assertFiniteNumber(right, path);
  return left + right;
}

function scaleGradientValue(value, divisor, path) {
  if (Array.isArray(value)) {
    return value.map((item, index) => scaleGradientValue(item, divisor, `${path}[${index}]`));
  }

  assertFiniteNumber(value, path);
  return value / divisor;
}

function visitGradientValues(value, visitor, path) {
  if (Array.isArray(value)) {
    for (let index = 0; index < value.length; index += 1) {
      visitGradientValues(value[index], visitor, `${path}[${index}]`);
    }
    return;
  }

  assertFiniteNumber(value, path);
  visitor(value);
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

export function hashGradient(gradient) {
  return sha256Hex(stableStringify(gradient));
}

export function averageGradients(gradientObjects) {
  if (!Array.isArray(gradientObjects) || gradientObjects.length === 0) {
    throw new Error("averageGradients requires at least one gradient object");
  }

  const keys = Object.keys(gradientObjects[0]).sort();

  for (const gradient of gradientObjects) {
    const gradientKeys = Object.keys(gradient).sort();

    if (stableStringify(gradientKeys) !== stableStringify(keys)) {
      throw new Error("All gradient objects must contain the same tensor names");
    }
  }

  const summed = {};

  for (const key of keys) {
    summed[key] = gradientObjects
      .slice(1)
      .reduce(
        (accumulator, gradient) => addGradientValues(accumulator, gradient[key], key),
        stableCopy(gradientObjects[0][key])
      );
  }

  return Object.fromEntries(
    keys.map((key) => [key, scaleGradientValue(summed[key], gradientObjects.length, key)])
  );
}

export function gradientNorm(gradient) {
  let sumSquares = 0;

  for (const [name, tensor] of Object.entries(gradient)) {
    visitGradientValues(tensor, (value) => {
      sumSquares += value * value;
    }, name);
  }

  return Math.sqrt(sumSquares);
}
