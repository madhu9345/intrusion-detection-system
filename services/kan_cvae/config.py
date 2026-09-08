import os


# ============================================================
# KAFKA CONFIGURATION
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)

# Input from Shapley Tree low-confidence routing
KAN_INPUT_TOPIC = os.getenv(
    "KAN_INPUT_TOPIC",
    "unknown-traffic"
)

# KAN-negative / known-normal traffic
KAN_CLASSIFICATION_TOPIC = os.getenv(
    "KAN_CLASSIFICATION_TOPIC",
    "classification-results"
)

# KAN-positive unknown attacks
KAN_ALERT_TOPIC = os.getenv(
    "KAN_ALERT_TOPIC",
    "alerts"
)

# Consumer group
KAN_CONSUMER_GROUP_ID = os.getenv(
    "KAN_CONSUMER_GROUP_ID",
    "kan-cvae-unknown-detector-test-v1"
)

KAFKA_AUTO_OFFSET_RESET = os.getenv(
    "KAFKA_AUTO_OFFSET_RESET",
    "earliest"
)

ENABLE_AUTO_COMMIT = False

POLL_TIMEOUT_MS = 1000

MAX_POLL_RECORDS = 32


# ============================================================
# PROCESSING
# ============================================================

BATCH_SIZE = int(
    os.getenv(
        "KAN_BATCH_SIZE",
        "32"
    )
)

BATCH_WAIT_MS = int(
    os.getenv(
        "KAN_BATCH_WAIT_MS",
        "100"
    )
)


# ============================================================
# MODEL ARTIFACTS
# ============================================================

MODEL_PATH = os.getenv(
    "MODEL_PATH",
    "models/kan_cvae.pt"
)

SCALER_PATH = os.getenv(
    "SCALER_PATH",
    "models/scaler.pkl"
)

PREPROCESSING_CONFIG_PATH = os.getenv(
    "PREPROCESSING_CONFIG_PATH",
    "models/preprocessing_config.json"
)

FEATURES_PATH = os.getenv(
    "FEATURES_PATH",
    "models/kan_features.json"
)

THRESHOLD_PATH = os.getenv(
    "THRESHOLD_PATH",
    "models/threshold.json"
)


# ============================================================
# KAN-CVAE ARCHITECTURE
# ============================================================

HIDDEN_DIM = 128

LATENT_DIM = 32

KAN_GRID_SIZE = 8

KAN_SPLINE_ORDER = 3

CONDITION_DIM = 1

DEVICE = os.getenv(
    "KAN_DEVICE",
    "cpu"
)


# ============================================================
# 16 DMB-SELECTED FEATURES
# ============================================================

SELECTED_FEATURES = [

    "Down/Up Ratio",

    "flow_packets_per_sec",

    "flow_duration",

    "Bwd Pkt Len Max",

    "Fwd IAT Min",

    "Fwd Pkt Len Max",

    "Bwd IAT Min",

    "Idle Mean",

    "Init Bwd Win Byts",

    "Init Fwd Win Byts",

    "Pkt Size Avg",

    "total_forward_packets",

    "RST Flag Cnt",

    "Fwd IAT Std",

    "Bwd Header Len",

    "Active Min",
]


# ============================================================
# VALIDATION
# ============================================================

if len(SELECTED_FEATURES) != 16:

    raise ValueError(
        "SELECTED_FEATURES must contain exactly 16 features."
    )