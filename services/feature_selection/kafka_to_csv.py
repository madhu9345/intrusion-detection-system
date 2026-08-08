from kafka import KafkaConsumer
import json
import csv
import time

# ============================================================
# CONFIGURATION
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "selected-features"

OUTPUT_FILE = "selected_features.csv"

# How long to wait for new messages before considering
# the current Kafka data finished
IDLE_TIMEOUT_SECONDS = 10


# ============================================================
# SELECTED FEATURES
# ============================================================

SELECTED_FEATURES = [
    "Bwd Pkt Len Max",
    "Bwd Header Len",
    "Active Min",
    "Init Bwd Win Byts",
    "Init Fwd Win Byts",
    "Bwd IAT Min",
    "Fwd Pkt Len Max",
    "Idle Mean",
    "RST Flag Cnt",
    "Fwd IAT Std",
    "flow_duration",
    "total_forward_packets",
    "Pkt Size Avg",
    "Down/Up Ratio",
    "Fwd IAT Min",
    "flow_packets_per_sec"
]

TARGET = "label"

CSV_COLUMNS = SELECTED_FEATURES + [TARGET]


# ============================================================
# KAFKA CONSUMER
# ============================================================

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],

    # VERY IMPORTANT:
    # Start from the earliest available record
    auto_offset_reset="earliest",

    # We don't want Kafka to remember this temporary
    # CSV export consumer position
    enable_auto_commit=False,

    value_deserializer=lambda x: json.loads(
        x.decode("utf-8")
    )
)


print("=" * 60)
print("Kafka → CSV")
print("=" * 60)
print(f"Topic : {TOPIC}")
print(f"File  : {OUTPUT_FILE}")
print("=" * 60)


# ============================================================
# CREATE CSV
# ============================================================

count = 0
last_message_time = time.time()

with open(
    OUTPUT_FILE,
    "w",
    newline="",
    encoding="utf-8"
) as file:

    writer = csv.DictWriter(
        file,
        fieldnames=CSV_COLUMNS
    )

    writer.writeheader()

    try:

        while True:

            # poll() allows us to detect when Kafka has no
            # more currently available records
            messages = consumer.poll(
                timeout_ms=1000
            )

            if not messages:

                # If no message has arrived for the timeout
                # period, assume current data is finished
                if (
                    time.time() - last_message_time
                    >= IDLE_TIMEOUT_SECONDS
                ):
                    print(
                        "\nNo new messages received."
                    )
                    print(
                        "Assuming all current records are consumed."
                    )
                    break

                continue

            # ------------------------------------------------
            # PROCESS MESSAGES
            # ------------------------------------------------

            for topic_partition, records in messages.items():

                for message in records:

                    record = message.value

                    row = {
                        column: record.get(
                            column,
                            0
                        )
                        for column in CSV_COLUMNS
                    }

                    writer.writerow(row)

                    count += 1

                    last_message_time = time.time()

                    if count % 10000 == 0:

                        print(
                            f"Saved {count:,} records..."
                        )

    except KeyboardInterrupt:

        print(
            "\nConsumer stopped manually."
        )

    finally:

        consumer.close()


# ============================================================
# COMPLETED
# ============================================================

print("\n" + "=" * 60)
print("CSV CREATION COMPLETED")
print("=" * 60)
print(
    f"Total records saved : {count:,}"
)
print(
    f"File                : {OUTPUT_FILE}"
)
print("=" * 60)