use ndarray::{Array1, Array2};
use serde::{Deserialize, Serialize};
use wasm_bindgen::prelude::*;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Architecture {
    #[serde(rename = "type")]
    model_type: String,
    input_size: usize,
    loss: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TrainingPayload {
    kernel_type: String,
    architecture: Architecture,
    chunk_id: Option<String>,
    weights: Vec<f64>,
    bias: f64,
    learning_rate: Option<f64>,
    features: Vec<Vec<f64>>,
    labels: Vec<f64>,
}

#[derive(Debug, Serialize)]
struct Gradients {
    weights: Vec<f64>,
    bias: Vec<f64>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct TrainingResult {
    kernel_type: String,
    loss: f64,
    gradients: Gradients,
    sample_count: usize,
    architecture: ArchitectureResult,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ArchitectureResult {
    #[serde(rename = "type")]
    model_type: String,
    input_size: usize,
    loss: String,
}

fn validate_payload(payload: &TrainingPayload) -> Result<(), JsValue> {
    if payload.kernel_type != "single_neuron_mse_v1" {
        return Err(JsValue::from_str("Unsupported browser kernel"));
    }

    if payload.features.is_empty() {
        return Err(JsValue::from_str("Training payload requires feature rows"));
    }

    if payload.labels.len() != payload.features.len() {
        return Err(JsValue::from_str("Training labels must match feature rows"));
    }

    if payload.weights.len() != payload.features[0].len() {
        return Err(JsValue::from_str("Training weights must match feature width"));
    }

    if payload.architecture.input_size != payload.weights.len() {
        return Err(JsValue::from_str("Architecture input size must match weights"));
    }

    Ok(())
}

fn run_single_neuron_mse(payload: TrainingPayload) -> Result<TrainingResult, JsValue> {
    validate_payload(&payload)?;

    let sample_count = payload.features.len();
    let input_size = payload.weights.len();
    let flat_features: Vec<f64> = payload.features.into_iter().flatten().collect();
    let features = Array2::from_shape_vec((sample_count, input_size), flat_features)
        .map_err(|error| JsValue::from_str(&error.to_string()))?;
    let labels = Array1::from_vec(payload.labels);
    let weights = Array1::from_vec(payload.weights);
    let predictions = features.dot(&weights) + payload.bias;
    let errors = &predictions - &labels;
    let loss = errors.mapv(|value| value * value).sum() / sample_count as f64;
    let gradient_weights = features.t().dot(&errors).mapv(|value| 2.0 * value / sample_count as f64);
    let gradient_bias = 2.0 * errors.sum() / sample_count as f64;

    Ok(TrainingResult {
        kernel_type: payload.kernel_type,
        loss,
        gradients: Gradients {
            weights: gradient_weights.to_vec(),
            bias: vec![gradient_bias],
        },
        sample_count,
        architecture: ArchitectureResult {
            model_type: payload.architecture.model_type,
            input_size: payload.architecture.input_size,
            loss: payload.architecture.loss,
        },
    })
}

#[wasm_bindgen]
pub fn run_training_chunk(payload_json: &str) -> Result<String, JsValue> {
    let payload: TrainingPayload = serde_json::from_str(payload_json)
        .map_err(|error| JsValue::from_str(&error.to_string()))?;
    let result = run_single_neuron_mse(payload)?;

    serde_json::to_string(&result).map_err(|error| JsValue::from_str(&error.to_string()))
}
