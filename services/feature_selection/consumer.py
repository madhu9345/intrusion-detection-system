import json
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from kafka.serializer import Deserializer

from config import (
    BOOTSTRAP_SERVER, INPUT_TOPIC, GROUP_ID, CLIENT_ID,
    AUTO_OFFSET_RESET, ENABLE_AUTO_COMMIT, AUTO_COMMIT_INTERVAL_MS,
    SESSION_TIMEOUT_MS, HEARTBEAT_INTERVAL_MS, MAX_POLL_RECORDS,
    MAX_POLL_INTERVAL_MS, FETCH_MAX_BYTES, FETCH_MIN_BYTES, FETCH_MAX_WAIT_MS
)
from logger import IDSLogger

class JSONDeserializer(Deserializer):
    def deserialize(self, topic, value):
        if value is None:
            return None
        return json.loads(value.decode("utf-8"))

class FeatureSelectionConsumer:
    def __init__(self):
        self.logger = IDSLogger().get_logger()
        self.received_records = 0

        consumer_config = {
            "bootstrap_servers": BOOTSTRAP_SERVER,
            "group_id": GROUP_ID,
            "client_id": CLIENT_ID,
            "auto_offset_reset": AUTO_OFFSET_RESET,
            "enable_auto_commit": ENABLE_AUTO_COMMIT,
            "auto_commit_interval_ms": AUTO_COMMIT_INTERVAL_MS,
            "session_timeout_ms": SESSION_TIMEOUT_MS,
            "heartbeat_interval_ms": HEARTBEAT_INTERVAL_MS,
            "max_poll_records": MAX_POLL_RECORDS,
            "max_poll_interval_ms": MAX_POLL_INTERVAL_MS,
            "fetch_max_bytes": FETCH_MAX_BYTES,
            "fetch_min_bytes": FETCH_MIN_BYTES,
            "fetch_max_wait_ms": FETCH_MAX_WAIT_MS,
            "value_deserializer": JSONDeserializer()
        }

        self.consumer = KafkaConsumer(INPUT_TOPIC, **consumer_config)
        self.logger.info("=" * 70)
        self.logger.info(f"Connected to Kafka Topic : {INPUT_TOPIC}")
        self.logger.info("Kafka Consumer Started Successfully")
        self.logger.info("=" * 70)

    def consume(self):
        try:
            for message in self.consumer:
                if message.value is None:
                    continue
                self.received_records += 1
                if self.received_records % 1000 == 0:
                    self.logger.info(f"Received Records : {self.received_records:,}")
                yield message.value
        except KeyboardInterrupt:
            self.logger.info("Consumer Interrupted")
        except KafkaError as e:
            self.logger.exception(f"Kafka Error : {e}")
        except Exception as e:
            self.logger.exception(f"Consumer Error : {e}")

    def close(self):
        self.consumer.close()
        self.logger.info("=" * 70)
        self.logger.info("CONSUMER SUMMARY")
        self.logger.info("=" * 70)
        self.logger.info(f"Received Records : {self.received_records:,}")
        self.logger.info("=" * 70)