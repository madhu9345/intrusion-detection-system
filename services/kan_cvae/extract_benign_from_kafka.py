import json
import csv
import os

from kafka import KafkaConsumer, TopicPartition


# ============================================================
# CONFIGURATION
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

INPUT_TOPIC = "clean-traffic"

CONSUMER_GROUP_ID = "kan-normal-traffic-extractor-v2"

# Save CSV inside:
# D:\IR-IDS\services\kan_cvae\data\normal_traffic.csv
OUTPUT_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "data",
    "normal_traffic.csv"
)


# ============================================================
# EXACT 16 DMB SELECTED FEATURES
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
    "Active Min"
]


# ============================================================
# CREATE OUTPUT DIRECTORY
# ============================================================

output_directory = os.path.dirname(OUTPUT_FILE)

os.makedirs(output_directory, exist_ok=True)


# ============================================================
# PRINT CONFIGURATION
# ============================================================

print("=" * 75)
print("KAN-CVAE NORMAL TRAFFIC EXTRACTION")
print("=" * 75)

print(f"Kafka Server      : {KAFKA_BOOTSTRAP_SERVERS}")
print(f"Input Topic       : {INPUT_TOPIC}")
print(f"Consumer Group    : {CONSUMER_GROUP_ID}")
print(f"Output File       : {OUTPUT_FILE}")
print(f"Selected Features : {len(SELECTED_FEATURES)}")

print()
print("Selected 16 Features:")
print("-" * 75)

for index, feature in enumerate(SELECTED_FEATURES, start=1):
    print(f"{index:2}. {feature}")

print("=" * 75)
print()


# ============================================================
# CREATE KAFKA CONSUMER
# ============================================================

consumer = KafkaConsumer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,

    group_id=CONSUMER_GROUP_ID,

    # We manually control offsets
    enable_auto_commit=False,

    auto_offset_reset="earliest",

    # Convert Kafka JSON bytes into Python dictionary
    value_deserializer=lambda message: json.loads(
        message.decode("utf-8")
    ),

    # Large messages / batches
    fetch_max_bytes=52428800,
    max_partition_fetch_bytes=52428800
)


# ============================================================
# GET PARTITIONS
# ============================================================

partitions = consumer.partitions_for_topic(INPUT_TOPIC)

if not partitions:

    print(
        f"ERROR: Kafka topic '{INPUT_TOPIC}' "
        f"was not found or has no partitions."
    )

    consumer.close()
    raise SystemExit(1)


topic_partitions = [
    TopicPartition(INPUT_TOPIC, partition)
    for partition in sorted(partitions)
]


print(f"Found {len(topic_partitions)} Kafka partition(s).")
print()


# ============================================================
# GET CURRENT END OFFSETS
# ============================================================
#
# This creates a snapshot boundary.
#
# Example:
#
# Partition 0 has offsets 0 ... 100000
#
# end_offset = 100001
#
# We process only the records that already exist when
# extraction starts.
#
# New messages arriving later will not be processed.
# ============================================================

end_offsets = consumer.end_offsets(topic_partitions)


print("Kafka snapshot end offsets:")

for tp in topic_partitions:

    print(
        f"Partition {tp.partition}: "
        f"0 -> {end_offsets[tp]}"
    )

print()


# ============================================================
# ASSIGN PARTITIONS MANUALLY
# ============================================================

consumer.assign(topic_partitions)


# ============================================================
# START FROM BEGINNING
# ============================================================

consumer.seek_to_beginning(*topic_partitions)


# ============================================================
# COUNTERS
# ============================================================

processed_records = 0

benign_records = 0

skipped_records = 0

invalid_records = 0

missing_feature_records = 0


# ============================================================
# STORE BENIGN RECORDS
# ============================================================

normal_records = []


# ============================================================
# PROCESS KAFKA DATA
# ============================================================

print("=" * 75)
print("STARTING KAFKA EXTRACTION")
print("=" * 75)
print()


while True:

    records = consumer.poll(
        timeout_ms=1000
    )


    # --------------------------------------------------------
    # NO RECORDS RECEIVED
    # --------------------------------------------------------

    if not records:

        finished = True


        for tp in topic_partitions:

            current_position = consumer.position(tp)

            if current_position < end_offsets[tp]:

                finished = False

                break


        if finished:

            break


        continue


    # ========================================================
    # PROCESS EACH PARTITION
    # ========================================================

    for tp, messages in records.items():

        for message in messages:

            # ------------------------------------------------
            # Do not process messages outside snapshot
            # ------------------------------------------------

            if message.offset >= end_offsets[tp]:

                continue


            processed_records += 1


            # ------------------------------------------------
            # GET MESSAGE DATA
            # ------------------------------------------------

            data = message.value


            # ------------------------------------------------
            # VALIDATE JSON
            # ------------------------------------------------

            if not isinstance(data, dict):

                invalid_records += 1

                continue


            # ------------------------------------------------
            # GET LABEL
            # ------------------------------------------------

            label = data.get("label")


            if label is None:

                skipped_records += 1

                continue


            # =================================================
            # ONLY BENIGN TRAFFIC
            # =================================================

            if str(label).strip().upper() != "BENIGN":

                skipped_records += 1

                continue


            # =================================================
            # CHECK 16 FEATURES
            # =================================================

            missing_features = [
                feature
                for feature in SELECTED_FEATURES
                if feature not in data
            ]


            if missing_features:

                missing_feature_records += 1
                invalid_records += 1

                print()
                print("WARNING: BENIGN record missing features")
                print(f"Kafka partition : {tp.partition}")
                print(f"Kafka offset    : {message.offset}")

                print("Missing features:")

                for feature in missing_features:

                    print(f"  - {feature}")

                print()

                continue


            # =================================================
            # CREATE RECORD WITH EXACT FEATURE ORDER
            # =================================================

            normal_record = {}


            for feature in SELECTED_FEATURES:

                value = data.get(feature)


                # Convert None to empty CSV value
                if value is None:

                    value = ""


                # Handle NaN / Infinity if Python float
                elif isinstance(value, float):

                    if value != value:

                        value = ""

                    elif value == float("inf"):

                        value = ""

                    elif value == float("-inf"):

                        value = ""


                normal_record[feature] = value


            # =================================================
            # STORE BENIGN RECORD
            # =================================================

            normal_records.append(
                normal_record
            )

            benign_records += 1


            # =================================================
            # PROGRESS
            # =================================================

            if benign_records % 10000 == 0:

                print(
                    f"Processed: {processed_records:,} | "
                    f"BENIGN: {benign_records:,}"
                )


# ============================================================
# CLOSE CONSUMER
# ============================================================

consumer.close()


# ============================================================
# WRITE CSV
# ============================================================

print()
print("=" * 75)
print("WRITING NORMAL TRAFFIC CSV")
print("=" * 75)
print()


with open(
    OUTPUT_FILE,
    mode="w",
    newline="",
    encoding="utf-8"
) as csv_file:

    writer = csv.DictWriter(
        csv_file,
        fieldnames=SELECTED_FEATURES
    )


    # --------------------------------------------------------
    # WRITE HEADER
    # --------------------------------------------------------

    writer.writeheader()


    # --------------------------------------------------------
    # WRITE DATA
    # --------------------------------------------------------

    writer.writerows(
        normal_records
    )


# ============================================================
# FINAL RESULT
# ============================================================

print()
print("=" * 75)
print("EXTRACTION COMPLETED SUCCESSFULLY")
print("=" * 75)

print()
print(f"Total records processed       : {processed_records:,}")

print(
    f"BENIGN records extracted     : "
    f"{benign_records:,}"
)

print(
    f"Non-BENIGN / skipped records : "
    f"{skipped_records:,}"
)

print(
    f"Invalid records              : "
    f"{invalid_records:,}"
)

print(
    f"Missing-feature records      : "
    f"{missing_feature_records:,}"
)

print(
    f"Features in output CSV       : "
    f"{len(SELECTED_FEATURES)}"
)

print()
print(f"Output file:")

print(OUTPUT_FILE)

print()
print("=" * 75)
print("CSV FEATURE ORDER")
print("=" * 75)

for index, feature in enumerate(
    SELECTED_FEATURES,
    start=1
):

    print(
        f"{index:2}. {feature}"
    )

print()
print("=" * 75)