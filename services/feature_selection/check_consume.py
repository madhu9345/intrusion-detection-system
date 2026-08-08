import json
from kafka import KafkaConsumer

# Initialize Consumer for 'clean-traffic'
consumer = KafkaConsumer(
    'clean-traffic',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',  # Reads from the beginning of the topic
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Listening for messages on 'clean-traffic'...\n" + "="*50)

try:
    for message in consumer:
        print(f"[Partition {message.partition} | Offset {message.offset}] Data:")
        print(json.dumps(message.value, indent=2))
        print("-" * 50)
except KeyboardInterrupt:
    print("\nStopped consumer.")