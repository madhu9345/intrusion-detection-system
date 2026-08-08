import json
import time
import pandas as pd

from kafka import KafkaProducer


# ============================================================
# CONFIGURATION
# ============================================================

KAFKA_SERVER = "localhost:9092"

TOPIC = "selected-features"

CSV_PATH = "selected_features.csv"


# ============================================================
# MODEL FEATURES
# ============================================================

FEATURES = [
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


# ============================================================
# LOAD TEST DATA
# ============================================================

print("=" * 70)
print("LOADING TEST DATA")
print("=" * 70)

df = pd.read_csv(
    CSV_PATH
)

print(
    "Total test records:",
    len(df)
)

print("\nClass distribution:")

print(
    df["label"].value_counts()
)


# ============================================================
# TAKE ONLY 3 RECORDS PER CLASS
# ============================================================

TEST_RECORDS_PER_CLASS = 3

test_df = (
    df
    .groupby(
        "label",
        group_keys=False
    )
    .head(
        TEST_RECORDS_PER_CLASS
    )
    .reset_index(
        drop=True
    )
)


print("\n" + "=" * 70)
print("RECORDS SELECTED FOR KAFKA TEST")
print("=" * 70)

print(
    test_df["label"].value_counts()
)

print(
    "\nTotal records to send:",
    len(test_df)
)


# ============================================================
# CREATE KAFKA PRODUCER
# ============================================================

producer = KafkaProducer(

    bootstrap_servers=[
        KAFKA_SERVER
    ],

    value_serializer=lambda x:
        json.dumps(
            x
        ).encode("utf-8")
)


print("\n✓ Kafka producer connected")


# ============================================================
# SEND RECORDS
# ============================================================

for index, row in test_df.iterrows():

    actual_class = str(
        row["label"]
    )


    # --------------------------------------------------------
    # Create message
    # --------------------------------------------------------

    message = {}

    for feature in FEATURES:

        value = row[feature]


        # Handle missing value
        if pd.isna(value):

            value = 0


        # Convert NumPy type to Python type
        elif hasattr(
            value,
            "item"
        ):

            value = value.item()


        message[feature] = value


    # --------------------------------------------------------
    # LABEL IS ONLY FOR TESTING
    # --------------------------------------------------------

    message["label"] = actual_class


    # --------------------------------------------------------
    # Add test ID
    # --------------------------------------------------------

    message["test_id"] = (
        f"TEST_{index + 1}"
    )


    # --------------------------------------------------------
    # Display
    # --------------------------------------------------------

    print("\n" + "-" * 70)

    print(
        f"Sending {index + 1}/{len(test_df)}"
    )

    print(
        "Test ID     :",
        message["test_id"]
    )

    print(
        "Actual class:",
        actual_class
    )


    # --------------------------------------------------------
    # Send to selected-features
    # --------------------------------------------------------

    producer.send(
        TOPIC,
        value=message
    )

    producer.flush()


    print(
        "✓ Sent to:",
        TOPIC
    )


    # Wait so classifier output is easy to observe
    time.sleep(1)


# ============================================================
# FINISH
# ============================================================

producer.flush()

producer.close()


print("\n" + "=" * 70)
print("TEST COMPLETED")
print("=" * 70)

print(
    "Records sent:",
    len(test_df)
)