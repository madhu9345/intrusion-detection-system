import json
import time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Publishing test records to 'clean-traffic'...")
for i in range(20):
    sample_flow = {
        "flow_duration": 1000 + i * 10,
        "protocol": 6,
        "flow_bytes_per_sec": 500.0 + i,
        "flow_packets_per_sec": 50.0,
        "label": "BENIGN"
    }
    producer.send("clean-traffic", value=sample_flow)

producer.flush()
print("Sent 20 records successfully!")