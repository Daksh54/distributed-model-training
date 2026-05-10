import json
import sys


def dot(left, right):
    return sum(value * right[index] for index, value in enumerate(left))


def run_linear_regression(payload):
    features = payload["features"]
    labels = payload["labels"]
    weights = payload["weights"]
    bias = float(payload.get("bias", 0))

    if not features:
        raise ValueError("features must not be empty")

    gradients = {
        "weights": [0 for _ in weights],
        "bias": [0],
    }
    loss = 0

    for row_index, row in enumerate(features):
        prediction = dot(row, weights) + bias
        error = prediction - labels[row_index]
        loss += error * error

        for index, value in enumerate(row):
            gradients["weights"][index] += (2 * error * value) / len(features)

        gradients["bias"][0] += (2 * error) / len(features)

    return {
        "gradients": gradients,
        "loss": loss / len(features),
        "metrics": {
            "samples": len(features),
            "device": payload.get("device", "cpu"),
        },
    }


def main():
    payload = json.loads(sys.argv[1])
    print(json.dumps(run_linear_regression(payload), separators=(",", ":")))


if __name__ == "__main__":
    main()
