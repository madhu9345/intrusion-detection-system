"""
config.py
---------
Enterprise Configuration for the IR-IDS Preprocessing Service
Compatible with kafka-python 2.2.15
"""

import os
from pathlib import Path

# =============================================================================
# Project Paths
# =============================================================================

SERVICE_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = Path(__file__).resolve().parents[2] if len(Path(__file__).resolve().parents) > 2 else SERVICE_ROOT

LOG_FOLDER = SERVICE_ROOT / "logs"

LOG_FILE = LOG_FOLDER / "preprocessing.log"

# =============================================================================
# Kafka Consumer
# =============================================================================

BOOTSTRAP_SERVER = os.getenv(
    "BOOTSTRAP_SERVER",
    os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
)

INPUT_TOPIC = os.getenv("INPUT_TOPIC", "raw-traffic")

CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "preprocessing-service-v2")

AUTO_OFFSET_RESET = "earliest"

ENABLE_AUTO_COMMIT = False

# Consumer Poll Settings
MAX_POLL_RECORDS = 500

POLL_TIMEOUT_MS = 1000

SESSION_TIMEOUT_MS = 30000

HEARTBEAT_INTERVAL_MS = 3000

# =============================================================================
# Kafka Producer
# =============================================================================

OUTPUT_TOPIC = os.getenv("OUTPUT_TOPIC", "clean-traffic")

CLIENT_ID = "preprocessing-producer"

# Producer Reliability
ACKS = 1

MAX_RETRIES = 20

RETRY_BACKOFF_MS = 1000

# Compression
COMPRESSION_TYPE = "gzip"

# Producer Batching
BATCH_SIZE = 262144          # 256 KB

LINGER_MS = 20

MAX_IN_FLIGHT = 5

BUFFER_MEMORY = 268435456    # 256 MB

# Producer Timeouts
REQUEST_TIMEOUT_MS = 120000

# IMPORTANT:
# Must be greater than REQUEST_TIMEOUT_MS + LINGER_MS
DELIVERY_TIMEOUT_MS = 180000

MAX_BLOCK_MS = 30000

# =============================================================================
# Validation
# =============================================================================

REQUIRED_COLUMNS = [
    "Label"
]

DROP_INVALID_RECORDS = True

# =============================================================================
# Cleaning
# =============================================================================

REPLACE_NULL_WITH = 0

REPLACE_INFINITY_WITH = 0

TRIM_WHITESPACE = True

NORMALIZE_COLUMN_NAMES = True

NORMALIZE_LABELS = True

# =============================================================================
# Schema
# =============================================================================

NUMERIC_COLUMNS = [

    "Dst Port",
    "Protocol",
    "Flow Duration",
    "Tot Fwd Pkts",
    "Tot Bwd Pkts",
    "TotLen Fwd Pkts",
    "TotLen Bwd Pkts",
    "Flow Byts/s",
    "Flow Pkts/s",
    "Flow IAT Mean",
    "Flow IAT Std",
    "Flow IAT Max",
    "Flow IAT Min"

]

# =============================================================================
# Statistics
# =============================================================================

PRINT_STATS_EVERY = 5000

# Flush producer every N records
FLUSH_INTERVAL = 5000

# =============================================================================
# Logging
# =============================================================================

LOG_LEVEL = "INFO"

LOG_FORMAT = (
    "%(asctime)s | "
    "%(levelname)-8s | "
    "%(message)s"
)

DATE_FORMAT = "%Y-%m-%d %H:%M:%S"