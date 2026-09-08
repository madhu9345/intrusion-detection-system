import json
import joblib
import numpy as np
import pandas as pd
import shap

from kafka import KafkaConsumer, KafkaProducer


# ============================================================
# CONFIGURATION
# ============================================================

KAFKA_SERVER = "localhost:9092"


# ============================================================
# KAFKA TOPICS
# ============================================================

# Input from Feature Selection Service
INPUT_TOPIC = "selected-features"

# High-confidence Shapley results
RESULT_TOPIC = "classification-results"

# Low-confidence records -> KAN-CVAE
KAN_TOPIC = "unknown-traffic"


# ============================================================
# CONFIDENCE CONFIGURATION
# ============================================================

# IMPORTANT:
#
# confidence >= 0.60
#       -> Shapley Tree
#
# confidence < 0.60
#       -> KAN-CVAE
#
CONFIDENCE_THRESHOLD = 0.30


# ============================================================
# CONSUMER CONFIGURATION
# ============================================================

# Use a new group when you want to process existing
# selected-features messages from the beginning.

CONSUMER_GROUP_ID = (
    "shapley-classifier-real-data-v2"
)

AUTO_OFFSET_RESET = "earliest"

ENABLE_AUTO_COMMIT = False


# ============================================================
# MODEL FILES
# ============================================================

MODEL_PATH = "shapley_tree.pkl"

LABEL_ENCODER_PATH = "label_encoder.pkl"

FEATURES_PATH = "shapley_features.json"


# ============================================================
# LOAD SHAPLEY TREE
# ============================================================

print("=" * 70)
print("LOADING SHAPLEY TREE")
print("=" * 70)

try:

    shapley_tree = joblib.load(
        MODEL_PATH
    )

    print(
        "✓ Shapley Tree loaded"
    )

except Exception as e:

    print(
        "✗ Failed to load Shapley Tree"
    )

    print(
        "Error:",
        e
    )

    raise


# ============================================================
# LOAD LABEL ENCODER
# ============================================================

try:

    label_encoder = joblib.load(
        LABEL_ENCODER_PATH
    )

    print(
        "✓ Label encoder loaded"
    )

except Exception as e:

    print(
        "✗ Failed to load label encoder"
    )

    print(
        "Error:",
        e
    )

    raise


# ============================================================
# LOAD FEATURE ORDER
# ============================================================

try:

    with open(
        FEATURES_PATH,
        "r",
        encoding="utf-8"
    ) as f:

        SHAPLEY_FEATURES = json.load(
            f
        )

    print(
        "✓ Shapley feature order loaded"
    )

except Exception as e:

    print(
        "✗ Failed to load shapley_features.json"
    )

    print(
        "Error:",
        e
    )

    raise


# ============================================================
# VALIDATE FEATURE COUNT
# ============================================================

if len(
    SHAPLEY_FEATURES
) != 16:

    raise ValueError(
        "Shapley Tree must use exactly "
        f"16 features, found "
        f"{len(SHAPLEY_FEATURES)}."
    )


# ============================================================
# DISPLAY FEATURES
# ============================================================

print(
    "\nFeatures used by Shapley Tree:"
)

for i, feature in enumerate(
    SHAPLEY_FEATURES,
    start=1
):

    print(
        f"{i:2d}. {feature}"
    )

print(
    "\nTotal model features:",
    len(SHAPLEY_FEATURES)
)


# ============================================================
# CHECK PREDICT_PROBA
# ============================================================

print(
    "\nChecking model confidence support..."
)

if hasattr(
    shapley_tree,
    "predict_proba"
):

    print(
        "✓ Model supports predict_proba()"
    )

else:

    raise RuntimeError(
        "Shapley Tree does not support "
        "predict_proba(). Confidence routing "
        "cannot be performed."
    )


# ============================================================
# CREATE SHAP EXPLAINER
# ============================================================

print(
    "\nCreating SHAP TreeExplainer..."
)

try:

    explainer = shap.TreeExplainer(
        shapley_tree
    )

    print(
        "✓ SHAP TreeExplainer ready"
    )

except Exception as e:

    print(
        "✗ Failed to create SHAP explainer"
    )

    print(
        "Error:",
        e
    )

    raise


# ============================================================
# CREATE KAFKA CONSUMER
# ============================================================

print(
    "\n" + "=" * 70
)

print(
    "CONNECTING KAFKA CONSUMER"
)

print(
    "=" * 70
)

consumer = KafkaConsumer(

    INPUT_TOPIC,

    bootstrap_servers=[
        KAFKA_SERVER
    ],

    group_id=(
        CONSUMER_GROUP_ID
    ),

    auto_offset_reset=(
        AUTO_OFFSET_RESET
    ),

    enable_auto_commit=(
        ENABLE_AUTO_COMMIT
    ),

    value_deserializer=lambda x:
        json.loads(
            x.decode(
                "utf-8"
            )
        )
)

print(
    "✓ Kafka consumer connected"
)

print(
    "✓ Input topic:",
    INPUT_TOPIC
)

print(
    "✓ Consumer group:",
    CONSUMER_GROUP_ID
)

print(
    "✓ Offset mode:",
    AUTO_OFFSET_RESET
)


# ============================================================
# CREATE KAFKA PRODUCER
# ============================================================

print(
    "\n" + "=" * 70
)

print(
    "CONNECTING KAFKA PRODUCER"
)

print(
    "=" * 70
)

result_producer = KafkaProducer(

    bootstrap_servers=[
        KAFKA_SERVER
    ],

    value_serializer=lambda x:
        json.dumps(
            x,
            ensure_ascii=False
        ).encode(
            "utf-8"
        ),

    acks=1,

    retries=3,

    max_in_flight_requests_per_connection=1,

    batch_size=0,

    linger_ms=0
)

print(
    "✓ Kafka producer connected"
)

print(
    "✓ High-confidence topic:",
    RESULT_TOPIC
)

print(
    "✓ Low-confidence topic:",
    KAN_TOPIC
)


# ============================================================
# STATISTICS
# ============================================================

total_received = 0

total_processed = 0

high_confidence_count = 0

low_confidence_count = 0

failed_count = 0


# ============================================================
# CREATE SHAP EXPLANATION
# ============================================================

def create_shap_explanation(
    new_data,
    prediction
):

    shap_explanation = []

    try:

        shap_values = (
            explainer.shap_values(
                new_data
            )
        )

        values = np.asarray(
            shap_values
        )

        # ----------------------------------------------------
        # SHAP older format:
        #
        # list[class] -> [samples, features]
        # ----------------------------------------------------

        if isinstance(
            shap_values,
            list
        ):

            class_index = int(
                prediction[0]
            )

            values = np.asarray(
                shap_values[
                    class_index
                ]
            )

            if values.ndim == 2:

                values = values[0]

        # ----------------------------------------------------
        # SHAP array format
        # ----------------------------------------------------

        else:

            values = np.asarray(
                shap_values
            )

            if values.ndim == 2:

                # [samples, features]

                values = values[0]

            elif values.ndim == 3:

                # Possible:
                #
                # [samples, features, classes]
                #
                # OR
                #
                # [samples, classes, features]

                class_index = int(
                    prediction[0]
                )

                if (
                    values.shape[1]
                    == len(
                        SHAPLEY_FEATURES
                    )
                ):

                    # samples x features x classes

                    values = values[
                        0,
                        :,
                        class_index
                    ]

                elif (
                    values.shape[2]
                    == len(
                        SHAPLEY_FEATURES
                    )
                ):

                    # samples x classes x features

                    values = values[
                        0,
                        class_index,
                        :
                    ]

                else:

                    raise ValueError(
                        "Unexpected SHAP shape: "
                        f"{values.shape}"
                    )

            else:

                raise ValueError(
                    "Unexpected SHAP dimensions: "
                    f"{values.ndim}"
                )

        # ----------------------------------------------------
        # Validate SHAP count
        # ----------------------------------------------------

        if len(values) != len(
            SHAPLEY_FEATURES
        ):

            raise ValueError(
                "SHAP value count does not "
                "match feature count."
            )

        # ----------------------------------------------------
        # Create explanation
        # ----------------------------------------------------

        explanation = pd.DataFrame({

            "feature":
                SHAPLEY_FEATURES,

            "value":
                new_data.iloc[0].values,

            "shap_value":
                values

        })

        explanation[
            "abs_shap"
        ] = explanation[
            "shap_value"
        ].abs()

        explanation = (
            explanation
            .sort_values(
                "abs_shap",
                ascending=False
            )
            .reset_index(
                drop=True
            )
        )

        # ----------------------------------------------------
        # Convert to JSON-safe format
        # ----------------------------------------------------

        for _, row in (
            explanation.iterrows()
        ):

            shap_explanation.append({

                "feature":
                    str(
                        row["feature"]
                    ),

                "value":
                    float(
                        row["value"]
                    ),

                "shap_value":
                    float(
                        row["shap_value"]
                    )

            })

    except Exception as e:

        print(
            "\n⚠ SHAP explanation failed:"
        )

        print(
            type(e).__name__,
            ":",
            str(e)
        )

    return shap_explanation


# ============================================================
# CLASSIFY MESSAGE
# ============================================================

def classify_message(
    kafka_message
):

    global total_processed
    global high_confidence_count
    global low_confidence_count
    global failed_count

    # ========================================================
    # VALIDATE
    # ========================================================

    if not isinstance(
        kafka_message,
        dict
    ):

        print(
            "\n✗ Invalid Kafka message."
        )

        failed_count += 1

        return None


    # ========================================================
    # ACTUAL LABEL
    # ========================================================
    #
    # ONLY for evaluation.
    #
    # Never passed to the model.
    # ========================================================

    actual_label = (
        kafka_message.get(
            "label",
            None
        )
    )


    # ========================================================
    # RECORD ID
    # ========================================================

    record_id = (
        kafka_message.get(
            "test_id",
            None
        )
    )


    # ========================================================
    # COPY ORIGINAL MESSAGE
    # ========================================================

    kan_message = (
        kafka_message.copy()
    )


    # ========================================================
    # CREATE MODEL INPUT
    # ========================================================

    feature_data = (
        kafka_message.copy()
    )


    # --------------------------------------------------------
    # Remove non-feature fields
    # --------------------------------------------------------

    feature_data.pop(
        "label",
        None
    )

    feature_data.pop(
        "test_id",
        None
    )

    feature_data.pop(
        "timestamp",
        None
    )

    feature_data.pop(
        "route",
        None
    )

    feature_data.pop(
        "shapley_prediction",
        None
    )

    feature_data.pop(
        "shapley_confidence",
        None
    )

    feature_data.pop(
        "confidence_threshold",
        None
    )


    # ========================================================
    # CHECK FEATURES
    # ========================================================

    missing_features = [

        feature

        for feature in SHAPLEY_FEATURES

        if feature not in feature_data

    ]

    if missing_features:

        print(
            "\n✗ Missing features:"
        )

        for feature in missing_features:

            print(
                "   -",
                feature
            )

        failed_count += 1

        return None


    # ========================================================
    # DATAFRAME
    # ========================================================

    new_data = pd.DataFrame(
        [feature_data]
    )


    # ========================================================
    # EXACT FEATURE ORDER
    # ========================================================

    new_data = new_data[
        SHAPLEY_FEATURES
    ]


    # ========================================================
    # NUMERIC CONVERSION
    # ========================================================

    new_data = new_data.apply(
        pd.to_numeric,
        errors="coerce"
    )


    # ========================================================
    # INVALID VALUES
    # ========================================================

    if new_data.isnull().any().any():

        print(
            "\n✗ Invalid numeric value."
        )

        print(
            new_data
        )

        failed_count += 1

        return None


    # ========================================================
    # PREDICTION
    # ========================================================

    try:

        prediction = (
            shapley_tree.predict(
                new_data
            )
        )

    except Exception as e:

        print(
            "\n✗ Prediction failed:"
        )

        print(
            e
        )

        failed_count += 1

        return None


    # ========================================================
    # DECODE LABEL
    # ========================================================

    try:

        predicted_class = (
            label_encoder
            .inverse_transform(
                prediction
            )[0]
        )

    except Exception as e:

        print(
            "\n✗ Failed to decode prediction:"
        )

        print(
            e
        )

        failed_count += 1

        return None


    # ========================================================
    # PROBABILITIES
    # ========================================================

    try:

        probabilities = (
            shapley_tree
            .predict_proba(
                new_data
            )[0]
        )

        confidence = float(
            np.max(
                probabilities
            )
        )

    except Exception as e:

        print(
            "\n✗ Failed to calculate confidence:"
        )

        print(
            e
        )

        failed_count += 1

        return None


    # ========================================================
    # ROUTING
    # ========================================================

    if (
        confidence
        >=
        CONFIDENCE_THRESHOLD
    ):

        route = (
            "SHAPLEY_TREE"
        )

        high_confidence_count += 1

    else:

        route = (
            "KAN_UNKNOWN_DETECTION"
        )

        low_confidence_count += 1


    # ========================================================
    # DISPLAY
    # ========================================================

    print()
    print("=" * 70)
    print("SHAPLEY TREE PREDICTION")
    print("=" * 70)

    if record_id is not None:

        print(
            "Record ID       :",
            record_id
        )

    print(
        "Predicted class :",
        predicted_class
    )

    print(
        "Confidence      :",
        f"{confidence * 100:.2f}%"
    )

    print(
        "Threshold       :",
        f"{CONFIDENCE_THRESHOLD * 100:.2f}%"
    )

    print(
        "Route           :",
        route
    )


    # ========================================================
    # HIGH CONFIDENCE
    # ========================================================

    if route == "SHAPLEY_TREE":

        # ----------------------------------------------------
        # SHAP
        # ----------------------------------------------------

        shap_explanation = (
            create_shap_explanation(
                new_data,
                prediction
            )
        )


        # ----------------------------------------------------
        # CORRECTNESS
        # ----------------------------------------------------

        correct = None

        if actual_label is not None:

            correct = (

                str(
                    predicted_class
                )
                ==
                str(
                    actual_label
                )

            )


        # ----------------------------------------------------
        # RESULT
        # ----------------------------------------------------

        classification_result = {

            "record_id":
                (
                    str(record_id)
                    if record_id is not None
                    else None
                ),

            "predicted_class":
                str(
                    predicted_class
                ),

            "confidence":
                float(
                    confidence
                ),

            "confidence_threshold":
                float(
                    CONFIDENCE_THRESHOLD
                ),

            "route":
                "SHAPLEY_TREE",

            "actual_label":
                (
                    str(actual_label)
                    if actual_label is not None
                    else None
                ),

            "correct":
                correct,

            "shap_explanation":
                shap_explanation

        }


        # ----------------------------------------------------
        # SEND
        # ----------------------------------------------------

        try:

            future = (
                result_producer.send(
                    RESULT_TOPIC,
                    value=classification_result
                )
            )

            future.get(
                timeout=30
            )

            result_producer.flush()

            print(
                "\n✓ HIGH CONFIDENCE"
            )

            print(
                "✓ Final result:",
                predicted_class
            )

            print(
                "✓ Sent to:",
                RESULT_TOPIC
            )

        except Exception as e:

            print(
                "\n✗ Failed to send classification result:"
            )

            print(
                e
            )

            failed_count += 1

            return None


        # ----------------------------------------------------
        # TOP SHAP FEATURES
        # ----------------------------------------------------

        print(
            "\nTop SHAP features:"
        )

        for item in (
            shap_explanation[:5]
        ):

            print(
                f"  {item['feature']:<30}"
                f"value={item['value']:<12}"
                f"SHAP={item['shap_value']:+.6f}"
            )


        total_processed += 1

        print(
            "=" * 70
        )

        return classification_result


    # ========================================================
    # LOW CONFIDENCE -> KAN
    # ========================================================

    else:

        # ----------------------------------------------------
        # Preserve ORIGINAL record
        # ----------------------------------------------------

        kan_message[
            "shapley_prediction"
        ] = str(
            predicted_class
        )

        kan_message[
            "shapley_confidence"
        ] = float(
            confidence
        )

        kan_message[
            "confidence_threshold"
        ] = float(
            CONFIDENCE_THRESHOLD
        )

        kan_message[
            "route"
        ] = (
            "KAN_UNKNOWN_DETECTION"
        )


        # ----------------------------------------------------
        # SEND TO KAN
        # ----------------------------------------------------

        try:

            future = (
                result_producer.send(
                    KAN_TOPIC,
                    value=kan_message
                )
            )

            future.get(
                timeout=30
            )

            result_producer.flush()

            print(
                "\n⚠ LOW CONFIDENCE"
            )

            print(
                "Shapley prediction:",
                predicted_class
            )

            print(
                "Confidence:",
                f"{confidence * 100:.2f}%"
            )

            print(
                "Threshold:",
                f"{CONFIDENCE_THRESHOLD * 100:.2f}%"
            )

            print(
                "✓ Route:",
                "KAN_UNKNOWN_DETECTION"
            )

            print(
                "✓ Sent to:",
                KAN_TOPIC
            )

        except Exception as e:

            print(
                "\n✗ Failed to send to KAN:"
            )

            print(
                e
            )

            failed_count += 1

            return None


        total_processed += 1

        print(
            "=" * 70
        )

        return kan_message


# ============================================================
# SERVICE START
# ============================================================

print()
print("=" * 70)
print("SHAPLEY CLASSIFICATION SERVICE STARTED")
print("=" * 70)

print(
    "Input topic          :",
    INPUT_TOPIC
)

print(
    "High-confidence topic:",
    RESULT_TOPIC
)

print(
    "Low-confidence topic :",
    KAN_TOPIC
)

print(
    "Confidence threshold :",
    f"{CONFIDENCE_THRESHOLD * 100:.2f}%"
)

print(
    "Consumer group       :",
    CONSUMER_GROUP_ID
)

print(
    "Offset reset         :",
    AUTO_OFFSET_RESET
)

print(
    "\nWaiting for Kafka messages..."
)


# ============================================================
# KAFKA LOOP
# ============================================================

try:

    for message in consumer:

        total_received += 1

        try:

            kafka_message = (
                message.value
            )

            print()
            print("-" * 70)

            print(
                "Received Kafka message"
            )

            print(
                "Partition:",
                message.partition
            )

            print(
                "Offset:",
                message.offset
            )

            if isinstance(
                kafka_message,
                dict
            ):

                if "test_id" in kafka_message:

                    print(
                        "Test ID:",
                        kafka_message[
                            "test_id"
                        ]
                    )

            # ------------------------------------------------
            # PROCESS
            # ------------------------------------------------

            result = classify_message(
                kafka_message
            )

            # ------------------------------------------------
            # Manual commit
            # ------------------------------------------------

            if result is not None:

                try:

                    consumer.commit()

                except Exception as e:

                    print(
                        "⚠ Offset commit failed:",
                        e
                    )

            # ------------------------------------------------
            # Statistics
            # ------------------------------------------------

            if (
                total_received
                % 100
                == 0
            ):

                print()
                print("=" * 70)
                print("PROCESSING STATISTICS")
                print("=" * 70)

                print(
                    "Total received:",
                    total_received
                )

                print(
                    "Total processed:",
                    total_processed
                )

                print(
                    "High confidence:",
                    high_confidence_count
                )

                print(
                    "Low confidence -> KAN:",
                    low_confidence_count
                )

                print(
                    "Failed:",
                    failed_count
                )

                print("=" * 70)


        except Exception as e:

            failed_count += 1

            print()
            print("=" * 70)
            print("MESSAGE PROCESSING ERROR")
            print("=" * 70)

            print(
                "Error type:",
                type(e).__name__
            )

            print(
                "Error:",
                str(e)
            )

            print("=" * 70)


except KeyboardInterrupt:

    print(
        "\nShapley service stopped."
    )


finally:

    try:

        result_producer.flush(
            timeout=10
        )

    except Exception:
        pass

    try:

        result_producer.close(
            timeout=10
        )

    except Exception:
        pass

    try:

        consumer.close()

    except Exception:
        pass

    print()
    print("=" * 70)
    print("SHAPLEY CLASSIFICATION SERVICE STOPPED")
    print("=" * 70)

    print(
        "Total received:",
        total_received
    )

    print(
        "Total processed:",
        total_processed
    )

    print(
        "High confidence:",
        high_confidence_count
    )

    print(
        "Low confidence -> KAN:",
        low_confidence_count
    )

    print(
        "Failed:",
        failed_count
    )

    print("=" * 70)