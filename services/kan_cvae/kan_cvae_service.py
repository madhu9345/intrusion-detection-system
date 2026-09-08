import json
import logging
import signal
import sys

import numpy as np
import torch

from kafka import KafkaConsumer, KafkaProducer

import config
from preprocessing import load_preprocessor
from kan_model import build_model


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(
    "IR-IDS-KAN-CVAE"
)


# ============================================================
# GLOBAL SERVICE FLAG
# ============================================================

RUNNING = True


def stop_service(
    signum,
    frame
):

    global RUNNING

    logger.info(
        "Shutdown signal received."
    )

    RUNNING = False


signal.signal(
    signal.SIGINT,
    stop_service
)

signal.signal(
    signal.SIGTERM,
    stop_service
)


# ============================================================
# FEATURES
# ============================================================

SELECTED_FEATURES = (
    config.SELECTED_FEATURES
)

FEATURE_COUNT = len(
    SELECTED_FEATURES
)


# ============================================================
# DEVICE
# ============================================================

DEVICE = torch.device(
    config.DEVICE
)


# ============================================================
# JSON SERIALIZATION
# ============================================================

def json_serializer(
    value
):

    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False
    ).encode(
        "utf-8"
    )


def json_deserializer(
    value
):

    return json.loads(
        value.decode(
            "utf-8"
        )
    )


# ============================================================
# CREATE KAFKA CONSUMER
# ============================================================

def create_consumer():

    logger.info(
        "Connecting Kafka consumer..."
    )

    consumer = KafkaConsumer(

        config.KAN_INPUT_TOPIC,

        bootstrap_servers=(
            config.KAFKA_BOOTSTRAP_SERVERS
        ),

        group_id=(
            config.KAN_CONSUMER_GROUP_ID
        ),

        auto_offset_reset=(
            config.KAFKA_AUTO_OFFSET_RESET
        ),

        enable_auto_commit=False,

        value_deserializer=(
            json_deserializer
        ),

        max_poll_records=(
            config.MAX_POLL_RECORDS
        )
    )

    logger.info(
        "Kafka consumer connected."
    )

    return consumer


# ============================================================
# CREATE KAFKA PRODUCER
# ============================================================

def create_producer():

    logger.info(
        "Creating Kafka producer..."
    )

    producer = KafkaProducer(

        bootstrap_servers=(
            config.KAFKA_BOOTSTRAP_SERVERS
        ),

        value_serializer=(
            json_serializer
        ),

        # Simple local configuration.
        # Avoids the ProducerBatch heap error
        # encountered previously.

        acks=1,

        retries=3,

        max_in_flight_requests_per_connection=1,

        batch_size=0,

        linger_ms=0
    )

    logger.info(
        "Kafka producer connected."
    )

    return producer


# ============================================================
# LOAD EVT THRESHOLD
# ============================================================

def load_threshold():

    logger.info(
        "Loading EVT threshold..."
    )

    with open(
        config.THRESHOLD_PATH,
        "r",
        encoding="utf-8"
    ) as file:

        threshold_config = json.load(
            file
        )

    if "threshold" not in threshold_config:

        raise KeyError(
            "threshold not found in "
            "threshold.json"
        )

    threshold = float(
        threshold_config[
            "threshold"
        ]
    )

    logger.info(
        "EVT anomaly threshold: %.10f",
        threshold
    )

    return threshold


# ============================================================
# LOAD KAN-CVAE MODEL
# ============================================================

def load_kan_cvae():

    logger.info(
        "Loading KAN-CVAE model..."
    )

    # --------------------------------------------------------
    # Build exact v3 architecture
    # --------------------------------------------------------

    model = build_model(

        input_dim=FEATURE_COUNT,

        hidden_dim=config.HIDDEN_DIM,

        latent_dim=config.LATENT_DIM,

        grid_size=config.KAN_GRID_SIZE,

        spline_order=config.KAN_SPLINE_ORDER,

        condition_dim=config.CONDITION_DIM
    )

    # --------------------------------------------------------
    # Load checkpoint
    # --------------------------------------------------------

    checkpoint = torch.load(
        config.MODEL_PATH,
        map_location=DEVICE
    )

    # --------------------------------------------------------
    # Support different checkpoint formats
    # --------------------------------------------------------

    if (
        isinstance(
            checkpoint,
            dict
        )
        and
        "model_state_dict"
        in checkpoint
    ):

        state_dict = (
            checkpoint[
                "model_state_dict"
            ]
        )

    elif (
        isinstance(
            checkpoint,
            dict
        )
        and
        "state_dict"
        in checkpoint
    ):

        state_dict = (
            checkpoint[
                "state_dict"
            ]
        )

    else:

        state_dict = checkpoint

    # --------------------------------------------------------
    # Load weights
    # --------------------------------------------------------

    model.load_state_dict(
        state_dict
    )

    model.to(
        DEVICE
    )

    model.eval()

    logger.info(
        "KAN-CVAE loaded successfully."
    )

    logger.info(
        "Input dimension : %d",
        FEATURE_COUNT
    )

    logger.info(
        "Hidden dimension: %d",
        config.HIDDEN_DIM
    )

    logger.info(
        "Latent dimension: %d",
        config.LATENT_DIM
    )

    logger.info(
        "Grid size       : %d",
        config.KAN_GRID_SIZE
    )

    logger.info(
        "Spline order    : %d",
        config.KAN_SPLINE_ORDER
    )

    logger.info(
        "Condition dim   : %d",
        config.CONDITION_DIM
    )

    logger.info(
        "Device          : %s",
        DEVICE
    )

    return model


# ============================================================
# UNWRAP KAFKA RECORD
# ============================================================

def unwrap_record(
    message
):
    """
    Supports messages such as:

        {
            "record": {...}
        }

    or:

        {
            "original_record": {...}
        }

    or:

        {
            "data": {...}
        }

    or a direct feature dictionary.
    """

    if not isinstance(
        message,
        dict
    ):

        raise ValueError(
            "Kafka message must be a JSON object."
        )

    possible_keys = [

        "record",

        "original_record",

        "data",

        "features",

        "traffic"
    ]

    for key in possible_keys:

        if key not in message:

            continue

        value = message[
            key
        ]

        if isinstance(
            value,
            dict
        ):

            return value

    # --------------------------------------------------------
    # Direct record
    # --------------------------------------------------------

    return message


# ============================================================
# EXTRACT 16 FEATURES
# ============================================================

def extract_features(
    record
):
    """
    Extract exactly the 16 DMB-selected features.

    Returns:

        shape = [1, 16]
    """

    values = []

    for feature in SELECTED_FEATURES:

        if feature not in record:

            raise KeyError(
                f"Missing selected feature: "
                f"{feature}"
            )

        value = record[
            feature
        ]

        if value is None:

            raise ValueError(
                f"Feature '{feature}' is None."
            )

        try:

            value = float(
                value
            )

        except (
            ValueError,
            TypeError
        ):

            raise ValueError(
                f"Invalid value for "
                f"'{feature}': {value}"
            )

        values.append(
            value
        )

    X = np.asarray(
        values,
        dtype=np.float32
    )

    if not np.isfinite(
        X
    ).all():

        raise ValueError(
            "Feature vector contains "
            "NaN or infinity."
        )

    return X.reshape(
        1,
        FEATURE_COUNT
    )


# ============================================================
# RECONSTRUCTION ERROR
# ============================================================

@torch.no_grad()
def reconstruction_error(
    model,
    X_processed
):
    """
    Calculate reconstruction MSE.
    """

    X_tensor = torch.tensor(
        X_processed,
        dtype=torch.float32,
        device=DEVICE
    )

    # --------------------------------------------------------
    # Condition
    #
    # v3 training uses zero condition for normal traffic.
    # --------------------------------------------------------

    condition = torch.zeros(
        (
            X_tensor.shape[0],
            config.CONDITION_DIM
        ),
        dtype=torch.float32,
        device=DEVICE
    )

    # --------------------------------------------------------
    # Model reconstruction
    # --------------------------------------------------------

    reconstruction, _, _ = (
        model(
            X_tensor,
            condition
        )
    )

    # --------------------------------------------------------
    # MSE
    # --------------------------------------------------------

    error = torch.mean(
        (
            X_tensor
            - reconstruction
        ) ** 2
    )

    return float(
        error.item()
    )


# ============================================================
# CREATE ALERT
# ============================================================

def create_alert(
    X
):
    """
    IMPORTANT:

    Kafka 'alerts' topic contains ONLY
    the 16 selected feature values.

    No:
        label
        Shapley prediction
        confidence
        reconstruction error
        threshold
        route
        timestamp
    """

    alert = {}

    for index, feature in enumerate(
        SELECTED_FEATURES
    ):

        alert[
            feature
        ] = float(
            X[0][index]
        )

    return alert


# ============================================================
# CREATE CLASSIFICATION RESULT
# ============================================================

def create_classification_result(
    message,
    X,
    error,
    threshold
):
    """
    KAN-negative result.

    The original Shapley prediction and confidence
    are preserved where available.

    This message is sent to:

        classification-results
    """

    result = {}

    # --------------------------------------------------------
    # Preserve useful Shapley metadata
    # --------------------------------------------------------

    if isinstance(
        message,
        dict
    ):

        metadata_keys = [

            "shapley_prediction",

            "shapley_confidence",

            "confidence_threshold",

            "actual_label",

            "correct",

            "shap",

            "prediction",

            "confidence",

            "route"
        ]

        for key in metadata_keys:

            if key in message:

                result[
                    key
                ] = message[
                    key
                ]

    # --------------------------------------------------------
    # KAN result
    # --------------------------------------------------------

    result[
        "kan_result"
    ] = "KNOWN_OR_NORMAL"

    result[
        "kan_anomaly"
    ] = False

    result[
        "reconstruction_error"
    ] = float(
        error
    )

    result[
        "evt_threshold"
    ] = float(
        threshold
    )

    result[
        "kan_route"
    ] = "CLASSIFICATION_RESULTS"

    # --------------------------------------------------------
    # Add the 16 features
    # --------------------------------------------------------

    for index, feature in enumerate(
        SELECTED_FEATURES
    ):

        result[
            feature
        ] = float(
            X[0][index]
        )

    return result


# ============================================================
# SEND MESSAGE
# ============================================================

def send_message(
    producer,
    topic,
    payload
):

    try:

        future = producer.send(
            topic,
            value=payload
        )

        future.get(
            timeout=10
        )

        return True

    except Exception as error:

        logger.error(
            "Kafka send failed | "
            "topic=%s | error=%s",
            topic,
            error
        )

        return False


# ============================================================
# PROCESS ONE MESSAGE
# ============================================================

def process_message(
    message,
    model,
    preprocessor,
    threshold,
    producer
):

    # --------------------------------------------------------
    # Get actual traffic record
    # --------------------------------------------------------

    record = unwrap_record(
        message
    )

    # --------------------------------------------------------
    # Extract raw 16 features
    # --------------------------------------------------------

    X = extract_features(
        record
    )

    # --------------------------------------------------------
    # IMPORTANT:
    #
    # preprocessing.py expects a dictionary,
    # not a NumPy array.
    #
    # Therefore use transform_one(record).
    # --------------------------------------------------------

    X_processed = (
        preprocessor.transform_one(
            record
        )
    )

    # --------------------------------------------------------
    # Calculate reconstruction error
    # --------------------------------------------------------

    error = reconstruction_error(
        model,
        X_processed
    )

    logger.info(
        "Reconstruction error = %.10f | "
        "EVT threshold = %.10f",
        error,
        threshold
    )

    # ========================================================
    # UNKNOWN ATTACK
    # ========================================================

    if error > threshold:

        alert = create_alert(
            X
        )

        success = send_message(
            producer,
            config.KAN_ALERT_TOPIC,
            alert
        )

        if success:

            logger.warning(
                "UNKNOWN ATTACK | "
                "error %.10f > threshold %.10f | "
                "sent to alerts",
                error,
                threshold
            )

            return "alert"

        return "failed"

    # ========================================================
    # KAN NEGATIVE
    # ========================================================
    #
    # Reconstruction error <= EVT threshold
    #
    # Send to classification-results.
    # ========================================================

    result = create_classification_result(

        message,

        X,

        error,

        threshold
    )

    success = send_message(
        producer,
        config.KAN_CLASSIFICATION_TOPIC,
        result
    )

    if success:

        logger.info(
            "KAN NEGATIVE | "
            "error %.10f <= threshold %.10f | "
            "sent to classification-results",
            error,
            threshold
        )

        return "classification"

    return "failed"


# ============================================================
# MAIN SERVICE
# ============================================================

def main():

    print()
    print("=" * 70)
    print("IR-IDS KAN-CVAE UNKNOWN ATTACK DETECTOR")
    print("=" * 70)

    print(
        "Input topic          :",
        config.KAN_INPUT_TOPIC
    )

    print(
        "Classification topic :",
        config.KAN_CLASSIFICATION_TOPIC
    )

    print(
        "Alert topic          :",
        config.KAN_ALERT_TOPIC
    )

    print(
        "Kafka                :",
        config.KAFKA_BOOTSTRAP_SERVERS
    )

    print(
        "Device               :",
        DEVICE
    )

    print(
        "Selected features    :",
        FEATURE_COUNT
    )

    print("=" * 70)
    print()

    # ========================================================
    # CHECK FEATURE COUNT
    # ========================================================

    if FEATURE_COUNT != 16:

        logger.error(
            "Expected 16 selected features, "
            "found %d.",
            FEATURE_COUNT
        )

        sys.exit(1)

    # ========================================================
    # LOAD PREPROCESSOR
    # ========================================================

    try:

        logger.info(
            "Loading preprocessing artifacts..."
        )

        preprocessor = (
            load_preprocessor(
                scaler_path=(
                    config.SCALER_PATH
                ),
                config_path=(
                    config.PREPROCESSING_CONFIG_PATH
                ),
                selected_features=(
                    SELECTED_FEATURES
                )
            )
        )

        logger.info(
            "Preprocessing artifacts loaded."
        )

    except Exception as error:

        logger.exception(
            "Preprocessor loading failed: %s",
            error
        )

        sys.exit(1)

    # ========================================================
    # LOAD EVT THRESHOLD
    # ========================================================

    try:

        threshold = load_threshold()

    except Exception as error:

        logger.exception(
            "Threshold loading failed: %s",
            error
        )

        sys.exit(1)

    # ========================================================
    # LOAD MODEL
    # ========================================================

    try:

        model = load_kan_cvae()

    except Exception as error:

        logger.exception(
            "KAN-CVAE loading failed: %s",
            error
        )

        sys.exit(1)

    # ========================================================
    # CREATE CONSUMER
    # ========================================================

    try:

        consumer = create_consumer()

    except Exception as error:

        logger.exception(
            "Kafka consumer creation failed: %s",
            error
        )

        sys.exit(1)

    # ========================================================
    # CREATE PRODUCER
    # ========================================================

    try:

        producer = create_producer()

    except Exception as error:

        logger.exception(
            "Kafka producer creation failed: %s",
            error
        )

        consumer.close()

        sys.exit(1)

    # ========================================================
    # STATISTICS
    # ========================================================

    total = 0

    alerts = 0

    classifications = 0

    failed = 0

    # ========================================================
    # START CONSUMING
    # ========================================================

    logger.info(
        "Waiting for messages from '%s'...",
        config.KAN_INPUT_TOPIC
    )

    try:

        while RUNNING:

            records = consumer.poll(

                timeout_ms=(
                    config.POLL_TIMEOUT_MS
                ),

                max_records=(
                    config.MAX_POLL_RECORDS
                )
            )

            # ------------------------------------------------
            # No messages
            # ------------------------------------------------

            if not records:

                continue

            # ------------------------------------------------
            # Process EXACT messages returned by this poll
            # ------------------------------------------------

            for (
                partition,
                messages
            ) in records.items():

                for kafka_message in messages:

                    if not RUNNING:

                        break

                    message = (
                        kafka_message.value
                    )

                    try:

                        result = (
                            process_message(

                                message,

                                model,

                                preprocessor,

                                threshold,

                                producer
                            )
                        )

                        total += 1

                        if result == "alert":

                            alerts += 1

                        elif (
                            result
                            == "classification"
                        ):

                            classifications += 1

                        else:

                            failed += 1

                    except Exception as error:

                        total += 1

                        failed += 1

                        logger.exception(
                            "Message failed: %s",
                            error
                        )

            # ------------------------------------------------
            # Flush producer
            # ------------------------------------------------

            try:

                producer.flush(
                    timeout=10
                )

            except Exception as error:

                logger.error(
                    "Producer flush failed: %s",
                    error
                )

            # ------------------------------------------------
            # Commit offsets only after processing
            # ------------------------------------------------

            try:

                consumer.commit()

            except Exception as error:

                logger.error(
                    "Consumer commit failed: %s",
                    error
                )

            # ------------------------------------------------
            # Statistics
            # ------------------------------------------------

            logger.info(
                "Processed=%d | "
                "alerts=%d | "
                "classification-results=%d | "
                "failed=%d",

                total,

                alerts,

                classifications,

                failed
            )

    except KeyboardInterrupt:

        logger.info(
            "Keyboard interrupt received."
        )

    finally:

        print()
        print("=" * 70)
        print("KAN-CVAE SERVICE STOPPED")
        print("=" * 70)

        print(
            "Total processed        :",
            total
        )

        print(
            "Unknown alerts         :",
            alerts
        )

        print(
            "Classification results :",
            classifications
        )

        print(
            "Failed                 :",
            failed
        )

        print("=" * 70)

        # ----------------------------------------------------
        # Flush producer
        # ----------------------------------------------------

        try:

            producer.flush(
                timeout=10
            )

        except Exception:
            pass

        # ----------------------------------------------------
        # Close producer
        # ----------------------------------------------------

        try:

            producer.close(
                timeout=10
            )

        except Exception:
            pass

        # ----------------------------------------------------
        # Close consumer
        # ----------------------------------------------------

        try:

            consumer.close()

        except Exception:
            pass


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    main()