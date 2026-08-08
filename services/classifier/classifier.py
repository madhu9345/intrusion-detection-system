import json
import joblib
import pandas as pd
import numpy as np
import shap

from kafka import KafkaConsumer, KafkaProducer


# ============================================================
# CONFIGURATION
# ============================================================

KAFKA_SERVER = "localhost:9092"

# Input topic
INPUT_TOPIC = "selected-features"

# Output topic
RESULT_TOPIC = "classification-results"

# Trained model files
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

    print("✓ Shapley Tree loaded")

except Exception as e:

    print("✗ Failed to load Shapley Tree")
    print("Error:", e)
    raise


# ============================================================
# LOAD LABEL ENCODER
# ============================================================

try:

    label_encoder = joblib.load(
        LABEL_ENCODER_PATH
    )

    print("✓ Label encoder loaded")

except Exception as e:

    print("✗ Failed to load label encoder")
    print("Error:", e)
    raise


# ============================================================
# LOAD SHAPLEY FEATURE ORDER
# ============================================================

try:

    with open(
        FEATURES_PATH,
        "r"
    ) as f:

        SHAPLEY_FEATURES = json.load(f)

    print("✓ Shapley feature order loaded")

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
# DISPLAY FEATURES
# ============================================================

print("\nFeatures used by Shapley Tree:")

for i, feature in enumerate(
    SHAPLEY_FEATURES,
    start=1
):

    print(
        f"{i:2d}. {feature}"
    )


# ============================================================
# CREATE SHAP EXPLAINER
# ============================================================

print("\nCreating SHAP TreeExplainer...")

try:

    explainer = shap.TreeExplainer(
        shapley_tree
    )

    print("✓ SHAP TreeExplainer ready")

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

print("\n" + "=" * 70)
print("CONNECTING KAFKA CONSUMER")
print("=" * 70)

try:

    
   consumer = KafkaConsumer(

    INPUT_TOPIC,

    bootstrap_servers=[
        KAFKA_SERVER
    ],

    # IMPORTANT:
    # Start from NEW messages
    auto_offset_reset="latest",

    # Use a NEW group for this controlled test
    group_id="shapley-classifier-test-001",

    enable_auto_commit=True,

    value_deserializer=lambda x:
        json.loads(
            x.decode("utf-8")
        )
)

except Exception as e:

    print(
        "✗ Failed to connect Kafka consumer"
    )

    print(
        "Error:",
        e
    )

    raise


# ============================================================
# CREATE KAFKA RESULT PRODUCER
# ============================================================

print("\n" + "=" * 70)
print("CONNECTING KAFKA RESULT PRODUCER")
print("=" * 70)

try:

    result_producer = KafkaProducer(

        bootstrap_servers=[
            KAFKA_SERVER
        ],

        # Python dictionary → JSON
        value_serializer=lambda x:
            json.dumps(
                x
            ).encode("utf-8")
    )

    print(
        "✓ Kafka result producer connected"
    )

    print(
        "✓ Output topic:",
        RESULT_TOPIC
    )

except Exception as e:

    print(
        "✗ Failed to create Kafka result producer"
    )

    print(
        "Error:",
        e
    )

    raise


# ============================================================
# START SERVICE
# ============================================================

print("\n" + "=" * 70)
print("SHAPLEY CLASSIFICATION SERVICE STARTED")
print("=" * 70)

print(
    "Input topic  :",
    INPUT_TOPIC
)

print(
    "Output topic :",
    RESULT_TOPIC
)

print(
    "\nWaiting for Kafka messages..."
)


# ============================================================
# CLASSIFICATION FUNCTION
# ============================================================

def classify_message(kafka_message):

    # ========================================================
    # GET ACTUAL LABEL
    # ========================================================
    #
    # Your current testing messages contain:
    #
    # "label": "BENIGN"
    #
    # We keep it ONLY for testing.
    #
    # It is NEVER passed to the model.
    #

    actual_label = kafka_message.get(
        "label",
        None
    )


    # ========================================================
    # COPY MESSAGE
    # ========================================================

    feature_data = kafka_message.copy()


    # ========================================================
    # REMOVE LABEL
    # ========================================================

    feature_data.pop(
        "label",
        None
    )


    # ========================================================
    # CHECK REQUIRED FEATURES
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

        return None


    # ========================================================
    # CREATE DATAFRAME
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
    # CONVERT TO NUMERIC
    # ========================================================

    new_data = new_data.apply(
        pd.to_numeric,
        errors="coerce"
    )


    # ========================================================
    # CHECK INVALID VALUES
    # ========================================================

    if new_data.isnull().any().any():

        print(
            "\n✗ Invalid numeric value found"
        )

        print(
            new_data
        )

        return None


    # ========================================================
    # MODEL PREDICTION
    # ========================================================

    prediction = shapley_tree.predict(
        new_data
    )


    # ========================================================
    # CONVERT NUMERIC CLASS → NAME
    # ========================================================

    predicted_class = (
        label_encoder
        .inverse_transform(
            prediction
        )[0]
    )


    # ========================================================
    # CONFIDENCE
    # ========================================================

    confidence = None

    if hasattr(
        shapley_tree,
        "predict_proba"
    ):

        probabilities = (
            shapley_tree
            .predict_proba(
                new_data
            )[0]
        )

        confidence = float(
            probabilities.max()
        )


    # ========================================================
    # LOCAL SHAP EXPLANATION
    # ========================================================

    shap_explanation = []

    try:

        shap_values = explainer.shap_values(
            new_data
        )


        # ----------------------------------------------------
        # Determine SHAP values
        # for predicted class
        # ----------------------------------------------------

        if isinstance(
            shap_values,
            list
        ):

            # Older SHAP format:
            #
            # list[class] →
            # samples × features

            predicted_class_index = (
                int(prediction[0])
            )

            values = np.asarray(
                shap_values[
                    predicted_class_index
                ]
            )[0]


        else:

            values = np.asarray(
                shap_values
            )


            # ------------------------------------------------
            # samples × features
            # ------------------------------------------------

            if values.ndim == 2:

                values = values[0]


            # ------------------------------------------------
            # 3D SHAP output
            # ------------------------------------------------

            elif values.ndim == 3:

                if (
                    values.shape[1]
                    == len(SHAPLEY_FEATURES)
                ):

                    # samples × features × classes

                    values = values[
                        0,
                        :,
                        int(prediction[0])
                    ]

                elif (
                    values.shape[2]
                    == len(SHAPLEY_FEATURES)
                ):

                    # samples × classes × features

                    values = values[
                        0,
                        int(prediction[0]),
                        :
                    ]

                else:

                    raise ValueError(
                        f"Unexpected SHAP shape: "
                        f"{values.shape}"
                    )

            else:

                raise ValueError(
                    f"Unexpected SHAP dimensions: "
                    f"{values.ndim}"
                )


        # ----------------------------------------------------
        # CREATE SHAP TABLE
        # ----------------------------------------------------

        explanation = pd.DataFrame({

            "feature":
                SHAPLEY_FEATURES,

            "value":
                new_data.iloc[0].values,

            "shap_value":
                values
        })


        # ----------------------------------------------------
        # ABSOLUTE SHAP
        # ----------------------------------------------------

        explanation[
            "abs_shap"
        ] = (
            explanation[
                "shap_value"
            ].abs()
        )


        # ----------------------------------------------------
        # SORT BY IMPORTANCE
        # ----------------------------------------------------

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
        # CREATE JSON-SAFE SHAP RESULT
        # ----------------------------------------------------

        for _, row in explanation.iterrows():

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


    # ========================================================
    # CHECK CORRECTNESS
    # ========================================================

    correct = None

    if actual_label is not None:

        correct = (
            predicted_class
            == actual_label
        )


    # ========================================================
    # CREATE CLASSIFICATION RESULT
    # ========================================================

    classification_result = {

        "predicted_class":
            str(
                predicted_class
            ),

        "confidence":
            confidence,

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


    # ========================================================
    # PRINT CLASSIFICATION RESULT
    # ========================================================

    print("\n")
    print("=" * 70)
    print("CLASSIFICATION RESULT")
    print("=" * 70)

    print(
        "Predicted class :",
        predicted_class
    )

    if confidence is not None:

        print(
            "Confidence      :",
            f"{confidence * 100:.2f}%"
        )

    if actual_label is not None:

        print(
            "Actual label    :",
            actual_label
        )

        if correct:

            print(
                "Result          : CORRECT"
            )

        else:

            print(
                "Result          : INCORRECT"
            )


    # ========================================================
    # PRINT TOP SHAP FEATURES
    # ========================================================

    print("\nTop SHAP features:")

    for item in shap_explanation[:5]:

        print(
            f"  {item['feature']:<30}"
            f"value={item['value']:<12}"
            f"SHAP={item['shap_value']:+.6f}"
        )


    # ========================================================
    # PUSH RESULT TO KAFKA
    # ========================================================

    try:

        result_producer.send(
            RESULT_TOPIC,
            value=classification_result
        )

        result_producer.flush()

        print(
            "\n✓ Classification result pushed to:",
            RESULT_TOPIC
        )

    except Exception as e:

        print(
            "\n✗ Failed to push classification result"
        )

        print(
            "Error:",
            e
        )


    print("=" * 70)


    return classification_result


# ============================================================
# CONTINUOUS KAFKA CONSUMPTION
# ============================================================

for message in consumer:

    try:

        # ----------------------------------------------------
        # Get Kafka message
        # ----------------------------------------------------

        kafka_message = message.value


        # ----------------------------------------------------
        # Print received message
        # ----------------------------------------------------

        print("\n")
        print("-" * 70)

        print(
            "Received Kafka message:"
        )

        print(
            kafka_message
        )


        # ----------------------------------------------------
        # Classify
        # ----------------------------------------------------

        classify_message(
            kafka_message
        )


    except Exception as e:

        print("\n")
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

        # Continue with next Kafka message
        continue