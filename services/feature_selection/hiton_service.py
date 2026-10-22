"""
hiton_service.py
----------------
Real-time Causal Feature Selection Service using the pre-trained HITON-PC Model.
Replaces the batch DG / Double Markov Blanket pipeline with high-throughput streaming.

Features:
✓ Uses pre-trained HITON-PC model from models/hiton_pc_model.pkl
✓ Zero-latency per-record feature extraction & normalization
✓ Fast Logistic Regression causal inference (Attack vs Benign probability)
✓ Publishes selected features directly to Kafka topic 'selected-features'
✓ Saves selected_features.json
"""

import json
import logging
import math
import signal
import sys
import time
from pathlib import Path

import joblib
import numpy as np
from kafka import KafkaConsumer, KafkaProducer

import config

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("HITON-PC-Service")

# ============================================================
# Column Aliases to map raw/clean names to HITON-PC features
# ============================================================
FEATURE_ALIASES = {
    "Destination Port": ["destination_port", "Dst Port", "dest_port", "Destination Port"],
    "Bwd Packet Length Min": ["Bwd Pkt Len Min", "bwd_packet_length_min", "Bwd Packet Length Min"],
    "Bwd Packet Length Mean": ["Bwd Pkt Len Mean", "bwd_packet_length_mean", "Bwd Packet Length Mean"],
    "Flow Packets/s": ["flow_packets_per_sec", "Flow Pkts/s", "flow_packets_s", "Flow Packets/s"],
    "Flow IAT Mean": ["flow_iat_mean", "Flow IAT Mean", "flow_iat_avg"],
    "Flow IAT Min": ["flow_iat_min", "Flow IAT Min"],
    "Bwd IAT Total": ["backward_iat_total", "Bwd IAT Tot", "bwd_iat_tot", "Bwd IAT Total"],
    "Fwd PSH Flags": ["Fwd PSH Flags", "fwd_psh_flags", "fwd_psh"],
    "Min Packet Length": ["Pkt Len Min", "min_packet_length", "Min Packet Length"],
    "Packet Length Variance": ["Pkt Len Var", "packet_length_variance", "Packet Length Variance"],
    "FIN Flag Count": ["FIN Flag Cnt", "fin_flag_cnt", "FIN Flag Count", "fin"],
    "RST Flag Count": ["RST Flag Cnt", "rst_flag_cnt", "RST Flag Count", "rst"],
    "URG Flag Count": ["URG Flag Cnt", "urg_flag_cnt", "URG Flag Count", "urg"],
    "Init_Win_bytes_backward": ["Init Bwd Win Byts", "init_bwd_win_bytes", "Init_Win_bytes_backward"],
    "min_seg_size_forward": ["Fwd Seg Size Min", "min_seg_size_forward", "fwd_seg_size_min"]
}


class HitonPCService:

    def __init__(self, model_path=None):
        self.model_path = Path(model_path or (config.MODEL_FOLDER / "hiton_pc_model.pkl"))

        if not self.model_path.exists():
            raise FileNotFoundError(f"HITON-PC model not found at: {self.model_path}")

        logger.info("=" * 70)
        logger.info(f"Loading HITON-PC Model from: {self.model_path.name}")
        logger.info("=" * 70)

        # Load Model Dict
        self.model_data = joblib.load(self.model_path)
        self.features = self.model_data["selected_features"]
        self.means = np.array(self.model_data["feature_means"], dtype=np.float32)
        self.stds = np.array(self.model_data["feature_stds"], dtype=np.float32)
        # Avoid division by zero
        self.stds[self.stds == 0] = 1.0

        weights_val = self.model_data["model_state_dict"]["linear.weight"]
        bias_val = self.model_data["model_state_dict"]["linear.bias"]
        if hasattr(weights_val, "cpu"):
            self.weights = weights_val.cpu().numpy().flatten().astype(np.float32)
        else:
            self.weights = np.array(weights_val, dtype=np.float32).flatten()

        if hasattr(bias_val, "cpu"):
            self.bias = float(bias_val.cpu().numpy().item())
        else:
            self.bias = float(np.array(bias_val).item())
        self.threshold = float(self.model_data.get("threshold", 0.5))

        logger.info(f"Loaded {len(self.features)} Causal Features from HITON-PC:")
        for idx, feat in enumerate(self.features, 1):
            logger.info(f"  {idx:2d}. {feat:<25} (weight: {self.weights[idx-1]:+7.4f})")
        logger.info(f"Classification Bias: {self.bias:+.4f} | Threshold: {self.threshold}")
        logger.info("=" * 70)

        # Save selected features JSON for downstream services
        self.save_selected_features_json()

        # Kafka Consumer
        self.consumer = KafkaConsumer(
            config.INPUT_TOPIC,
            bootstrap_servers=config.BOOTSTRAP_SERVER,
            group_id=config.GROUP_ID + "-hiton",
            auto_offset_reset=config.AUTO_OFFSET_RESET,
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v else None
        )

        # Kafka Producer
        self.producer = KafkaProducer(
            bootstrap_servers=config.BOOTSTRAP_SERVER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            compression_type="gzip",
            linger_ms=10,
            acks=1
        )

        self.running = True
        self.processed_count = 0
        self.attack_count = 0
        self.benign_count = 0
        self.start_time = None

    def save_selected_features_json(self):
        output_file = config.SERVICE_ROOT / "selected_features.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(self.features, f, indent=4)
        logger.info(f"Saved selected features list to: {output_file.name}")

    def extract_feature_vector(self, record):
        """Extracts the 15 HITON-PC features from incoming record using alias matching."""
        vector = []
        for feat in self.features:
            aliases = FEATURE_ALIASES.get(feat, [feat])
            val = None
            for alias in aliases:
                if alias in record:
                    val = record[alias]
                    break
            try:
                val = float(val) if val is not None else 0.0
                if math.isnan(val) or math.isinf(val):
                    val = 0.0
            except (ValueError, TypeError):
                val = 0.0
            vector.append(val)
        return np.array(vector, dtype=np.float32)

    def predict_causal_score(self, vector):
        """Standardizes vector and calculates logistic regression attack probability."""
        x_norm = (vector - self.means) / self.stds
        z = np.dot(self.weights, x_norm) + self.bias
        # Sigmoid
        prob = 1.0 / (1.0 + np.exp(-np.clip(z, -50.0, 50.0)))
        return float(prob)

    def process_record(self, record):
        raw_vector = self.extract_feature_vector(record)
        attack_prob = self.predict_causal_score(raw_vector)
        predicted_label = "ATTACK" if attack_prob >= self.threshold else "BENIGN"

        # Build clean output payload preserving all flow features for downstream Classifier & KAN-CVAE
        output_payload = dict(record)
        for feat, val in zip(self.features, raw_vector):
            output_payload[feat] = float(val)

        # Include original label if available
        if "label" in record:
            output_payload["label"] = record["label"]
        elif "Label" in record:
            output_payload["label"] = record["Label"]

        output_payload["hiton_attack_prob"] = round(attack_prob, 4)
        output_payload["hiton_prediction"] = predicted_label

        self.producer.send(config.OUTPUT_TOPIC, value=output_payload)

        self.processed_count += 1
        if predicted_label == "ATTACK":
            self.attack_count += 1
        else:
            self.benign_count += 1

        if self.processed_count % 1000 == 0:
            elapsed = max(time.time() - self.start_time, 0.001)
            rate = self.processed_count / elapsed
            logger.info(
                f"Processed: {self.processed_count:,} records | "
                f"Rate: {rate:.1f} rec/s | "
                f"Attacks: {self.attack_count:,} | "
                f"Benign: {self.benign_count:,}"
            )

    def run(self):
        self.start_time = time.time()
        logger.info(f"HITON-PC Service streaming '{config.INPUT_TOPIC}' -> '{config.OUTPUT_TOPIC}'...")

        try:
            for message in self.consumer:
                if not self.running:
                    break
                if message.value is None:
                    continue
                self.process_record(message.value)
        except KeyboardInterrupt:
            logger.info("Keyboard Interrupt received.")
        finally:
            self.shutdown()

    def shutdown(self):
        logger.info("Flushing and closing Kafka connections...")
        try:
            self.producer.flush()
            self.producer.close()
        except Exception:
            pass
        try:
            self.consumer.close()
        except Exception:
            pass

        elapsed = max(time.time() - self.start_time, 0.001) if self.start_time else 1
        logger.info("=" * 70)
        logger.info("HITON-PC SERVICE COMPLETED")
        logger.info(f"Total Records Streamed : {self.processed_count:,}")
        logger.info(f"Total Attacks Detected : {self.attack_count:,}")
        logger.info(f"Total Benign Flows     : {self.benign_count:,}")
        logger.info(f"Elapsed Time           : {elapsed:.2f} sec")
        logger.info(f"Throughput             : {self.processed_count / elapsed:.2f} records/sec")
        logger.info("=" * 70)


def main():
    service = HitonPCService()

    def handle_sig(sig, frame):
        service.running = False

    signal.signal(signal.SIGINT, handle_sig)
    signal.signal(signal.SIGTERM, handle_sig)

    service.run()


if __name__ == "__main__":
    main()
