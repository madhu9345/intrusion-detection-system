from kafka import KafkaProducer
import json
from config import *

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def publish(data):
    producer.send(OUTPUT_TOPIC, data)
    producer.flush()