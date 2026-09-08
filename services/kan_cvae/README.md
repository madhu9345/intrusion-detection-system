# IR-IDS KAN-CVAE Kafka Service

This module implements the KAN-CVAE stage of the IR-IDS pipeline.

## Your responsibility

The service starts from:

    unknown-traffic

and ends at:

    alerts

The Threat Intelligence stage is NOT included here.

## Final pipeline

    selected-features
            |
            v
       Shapley Tree
            |
            | confidence < 60%
            v
      unknown-traffic
            |
            v
        KAN-CVAE
            |
            v
    reconstruction error
            |
            v
       EVT threshold
            |
        +---+---+
        |       |
      <= T     > T
        |       |
      ignore   alerts
                |
                v
        ONLY 16 FEATURES

## Important alert requirement

The `alerts` Kafka topic contains ONLY the 16 DMB-selected features.

It does NOT contain:

- label
- UNKNOWN_ATTACK
- Shapley prediction
- Shapley confidence
- reconstruction error
- EVT threshold
- route
- timestamp added by this service

Example alert message:

{
  "Down/Up Ratio": 1.0,
  "flow_packets_per_sec": 1250.5,
  "flow_duration": 54231,
  "Bwd Pkt Len Max": 1460,
  "Fwd IAT Min": 10,
  "Fwd Pkt Len Max": 1460,
  "Bwd IAT Min": 12,
  "Idle Mean": 0,
  "Init Bwd Win Byts": 65535,
  "Init Fwd Win Byts": 64240,
  "Pkt Size Avg": 512.4,
  "total_forward_packets": 120,
  "RST Flag Cnt": 2,
  "Fwd IAT Std": 15.3,
  "Bwd Header Len": 40,
  "Active Min": 100
}

## Model artifacts

Copy the artifacts from your v3 Kaggle training output into:

    kan_cvae/
    └── models/
        ├── kan_cvae.pt
        ├── scaler.pkl
        ├── preprocessing_config.json
        ├── kan_features.json
        └── threshold.json

The v3 training code stores the model architecture information inside
`kan_cvae.pt`, including input_dim, condition_dim, hidden_dim, latent_dim,
grid_size and spline_order.

## Install

    python -m venv .venv

Windows:

    .venv\Scripts\activate

Linux/macOS:

    source .venv/bin/activate

    pip install -r requirements.txt

## Start Kafka

Make sure Kafka is running at:

    localhost:9092

## Start the KAN-CVAE service

From the `kan_cvae` directory:

    python kan_cvae_service.py

You should see:

    Input topic : unknown-traffic
    Alert topic : alerts
    EVT anomaly threshold: ...

Then the service waits for low-confidence traffic.

## Test

First make sure `unknown-traffic` has messages.

Then run:

    python kan_cvae_service.py

If a record has:

    reconstruction_error > EVT threshold

the service publishes the 16 selected features to:

    alerts

If:

    reconstruction_error <= EVT threshold

nothing is published to `alerts`.

## Kafka UI verification

You should see:

    unknown-traffic   -> input
    alerts            -> output

For an unknown/anomalous record, the `alerts` message must contain
exactly the 16 selected feature fields.

## BENIGN dataset extraction

If you need to recreate the normal dataset from Kafka:

    python extract_benign_from_kafka.py

It reads:

    clean-traffic

filters:

    label == BENIGN

and writes:

    data/normal_traffic.csv

with only the 16 selected features.

## Important compatibility note

The supplied v3 KAN model uses batch-dependent min/max normalization
inside KANLinear and stochastic VAE sampling during inference. This
service keeps those behaviors so that it is compatible with the
trained v3 artifact and threshold.

This is appropriate for the current project integration/testing stage.
For production hardening, the model should be retrained with fixed
feature-wise spline normalization and deterministic inference, followed
by threshold recalibration.
