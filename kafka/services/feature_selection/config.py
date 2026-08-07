"""
Kafka Configuration
"""

BOOTSTRAP_SERVERS = "localhost:9092"

INPUT_TOPIC = "clean-traffic"

OUTPUT_TOPIC = "selected-features"

GROUP_ID = "feature-selection-group"