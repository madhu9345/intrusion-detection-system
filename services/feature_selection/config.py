import os
from pathlib import Path


# ==========================================================
# Paths
# ==========================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

SERVICE_ROOT = Path(__file__).resolve().parent

LOG_FOLDER = SERVICE_ROOT / "logs"

LOG_FILE = LOG_FOLDER / "feature_selection.log"

MODEL_FOLDER = SERVICE_ROOT / "models"

LOG_FOLDER.mkdir(
    parents=True,
    exist_ok=True
)

MODEL_FOLDER.mkdir(
    parents=True,
    exist_ok=True
)


# ==========================================================
# Kafka Consumer
# ==========================================================

BOOTSTRAP_SERVER = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)

INPUT_TOPIC = os.getenv(
    "INPUT_TOPIC",
    "clean-traffic"
)

# New consumer group allows existing clean-traffic
# records to be processed again.

GROUP_ID = os.getenv(
    "GROUP_ID",
    "feature-selection-training-v2"
)

CLIENT_ID = "feature-selection"

# Read existing records when this group has no offset.

AUTO_OFFSET_RESET = "earliest"

# Manual/controlled batch processing.

ENABLE_AUTO_COMMIT = False

AUTO_COMMIT_INTERVAL_MS = 5000

SESSION_TIMEOUT_MS = 30000

HEARTBEAT_INTERVAL_MS = 3000

MAX_POLL_RECORDS = 1000

MAX_POLL_INTERVAL_MS = 300000

FETCH_MAX_BYTES = 52428800

FETCH_MIN_BYTES = 1

FETCH_MAX_WAIT_MS = 500


# ==========================================================
# Kafka Producer
# ==========================================================

OUTPUT_TOPIC = os.getenv(
    "OUTPUT_TOPIC",
    "selected-features"
)

ACKS = 1

MAX_RETRIES = 20

RETRY_BACKOFF_MS = 1000

COMPRESSION_TYPE = "gzip"

KAFKA_BATCH_SIZE = 262144

LINGER_MS = 20

MAX_IN_FLIGHT = 5

# Kept for compatibility.
# Do not pass this to KafkaProducer if your
# installed kafka-python version rejects it.

BUFFER_MEMORY = 268435456

REQUEST_TIMEOUT_MS = 120000

DELIVERY_TIMEOUT_MS = 180000

MAX_BLOCK_MS = 30000


# ==========================================================
# Feature Selection Pipeline
# ==========================================================

# Number of records processed in one batch.

BATCH_RECORDS = 5000

TARGET_COLUMN = "label"


# ==========================================================
# Minimum / Maximum Selected Features
# ==========================================================

# Minimum number of features required in final output.

MIN_FEATURES = 15

# Maximum number of features allowed.

MAX_SELECTED_FEATURES = 25

SAVE_SELECTED_FEATURES = True

SELECTED_FEATURES_FILE = (
    SERVICE_ROOT / "selected_features.json"
)

PRINT_PIPELINE_SUMMARY = True


# ==========================================================
# DMB / Causal Effect Parameters
# ==========================================================

MIN_FEATURE_SCORE = 0.01

# Minimum number of target classes required
# for meaningful DMB causal selection.

MIN_TARGET_CLASSES = 2

NUM_CLASSES = 15

ALPHA_PERCENTILE = 75

MIN_GROUP_SIZE = 5

# Engineering limit to avoid extremely sparse
# high-dimensional conditional grouping.

MAX_CMB_CONDITIONING_COLUMNS = 6

RANK_BY = "causal_effect"

RANDOM_STATE = 42


# ==========================================================
# Models
# ==========================================================

CLASSIFIER_MODEL = (
    MODEL_FOLDER / "classifier.pkl"
)

SCALER_MODEL = (
    MODEL_FOLDER / "scaler.pkl"
)


# ==========================================================
# Logging
# ==========================================================

PRINT_STATS_EVERY = 5000

LOG_LEVEL = "INFO"

LOG_FORMAT = (
    "%(asctime)s | "
    "%(levelname)-8s | "
    "%(message)s"
)

DATE_FORMAT = "%Y-%m-%d %H:%M:%S"