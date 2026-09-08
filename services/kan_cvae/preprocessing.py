import json
import pickle

import numpy as np

from config import (
    SCALER_PATH,
    PREPROCESSING_CONFIG_PATH,
    SELECTED_FEATURES,
)


# ============================================================
# SIGNED LOG1P
# ============================================================

def signed_log1p(x):
    """
    Exact v3 heavy-tail transformation:

        sign(x) * log1p(abs(x))
    """

    x = np.asarray(
        x,
        dtype=np.float64
    )

    return (
        np.sign(x)
        * np.log1p(np.abs(x))
    )


# ============================================================
# TRAFFIC PREPROCESSOR
# ============================================================

class TrafficPreprocessor:

    def __init__(
        self,
        scaler_path=SCALER_PATH,
        config_path=PREPROCESSING_CONFIG_PATH,
    ):

        # ----------------------------------------------------
        # Load saved RobustScaler
        # ----------------------------------------------------

        with open(
            scaler_path,
            "rb"
        ) as f:

            self.scaler = pickle.load(
                f
            )

        # ----------------------------------------------------
        # Load preprocessing configuration
        # ----------------------------------------------------

        with open(
            config_path,
            "r",
            encoding="utf-8"
        ) as f:

            self.config = json.load(
                f
            )

        # ----------------------------------------------------
        # Get feature order
        # ----------------------------------------------------

        self.features = self.config.get(
            "features",
            SELECTED_FEATURES
        )

        # ----------------------------------------------------
        # Verify exact feature order
        # ----------------------------------------------------

        if self.features != SELECTED_FEATURES:

            raise ValueError(
                "\nFeature order mismatch!\n\n"
                "preprocessing_config.json:\n"
                f"{self.features}\n\n"
                "config.py:\n"
                f"{SELECTED_FEATURES}"
            )

        # ----------------------------------------------------
        # Load training-derived clipping bounds
        # ----------------------------------------------------

        if "lower_bounds" not in self.config:

            raise KeyError(
                "lower_bounds missing from "
                "preprocessing_config.json"
            )

        if "upper_bounds" not in self.config:

            raise KeyError(
                "upper_bounds missing from "
                "preprocessing_config.json"
            )

        self.lower_bounds = np.asarray(
            self.config["lower_bounds"],
            dtype=np.float64
        )

        self.upper_bounds = np.asarray(
            self.config["upper_bounds"],
            dtype=np.float64
        )

        # ----------------------------------------------------
        # Validate dimensions
        # ----------------------------------------------------

        if len(self.features) != 16:

            raise ValueError(
                "Expected exactly 16 features."
            )

        if len(self.lower_bounds) != 16:

            raise ValueError(
                "lower_bounds must contain "
                "exactly 16 values."
            )

        if len(self.upper_bounds) != 16:

            raise ValueError(
                "upper_bounds must contain "
                "exactly 16 values."
            )

        # ----------------------------------------------------
        # Validate scaler
        # ----------------------------------------------------

        if not hasattr(
            self.scaler,
            "transform"
        ):

            raise TypeError(
                "Loaded scaler does not have "
                "a transform() method."
            )

        if hasattr(
            self.scaler,
            "n_features_in_"
        ):

            if self.scaler.n_features_in_ != 16:

                raise ValueError(
                    "Scaler expects "
                    f"{self.scaler.n_features_in_} features, "
                    "but this model uses 16."
                )

    # ========================================================
    # TRANSFORM MULTIPLE RECORDS
    # ========================================================

    def transform(
        self,
        records
    ):
        """
        Transform a list of Kafka traffic records.

        Input:

            [
                {
                    "Down/Up Ratio": ...,
                    "flow_packets_per_sec": ...,
                    ...
                },
                {
                    ...
                }
            ]

        Output:

            numpy array of shape:

                [batch_size, 16]

        Pipeline:

            raw
              ↓
            signed_log1p
              ↓
            RobustScaler
              ↓
            training-derived clipping
              ↓
            float32
        """

        if records is None:

            raise ValueError(
                "records cannot be None."
            )

        if not isinstance(
            records,
            (list, tuple)
        ):

            raise TypeError(
                "records must be a list or "
                "tuple of dictionaries."
            )

        if len(records) == 0:

            return np.empty(
                (
                    0,
                    16
                ),
                dtype=np.float32
            )

        rows = []

        # ----------------------------------------------------
        # Extract features in exact order
        # ----------------------------------------------------

        for record_index, record in enumerate(
            records
        ):

            if not isinstance(
                record,
                dict
            ):

                raise TypeError(
                    f"Record {record_index} "
                    "must be a dictionary."
                )

            row = []

            for feature in self.features:

                if feature not in record:

                    raise KeyError(
                        f"Record {record_index}: "
                        f"missing feature "
                        f"'{feature}'."
                    )

                value = record[
                    feature
                ]

                if value is None:

                    raise ValueError(
                        f"Record {record_index}: "
                        f"'{feature}' is None."
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
                        f"Record {record_index}: "
                        f"invalid value for "
                        f"'{feature}': {value}"
                    )

                row.append(
                    value
                )

            rows.append(
                row
            )

        # ----------------------------------------------------
        # Convert to NumPy
        # ----------------------------------------------------

        X = np.asarray(
            rows,
            dtype=np.float64
        )

        # ----------------------------------------------------
        # Validate raw data
        # ----------------------------------------------------

        if X.shape != (
            len(records),
            16
        ):

            raise ValueError(
                f"Unexpected input shape: "
                f"{X.shape}"
            )

        if not np.isfinite(
            X
        ).all():

            raise ValueError(
                "Input contains NaN or infinity."
            )

        # ====================================================
        # STEP 1
        # SIGNED LOG1P
        # ====================================================

        X_log = signed_log1p(
            X
        )

        if not np.isfinite(
            X_log
        ).all():

            raise ValueError(
                "signed_log1p produced "
                "NaN or infinity."
            )

        # ====================================================
        # STEP 2
        # ROBUST SCALER
        # ====================================================

        X_scaled = self.scaler.transform(
            X_log
        )

        X_scaled = np.asarray(
            X_scaled,
            dtype=np.float64
        )

        if not np.isfinite(
            X_scaled
        ).all():

            raise ValueError(
                "RobustScaler produced "
                "NaN or infinity."
            )

        # ====================================================
        # STEP 3
        # TRAINING-DERIVED CLIPPING
        # ====================================================

        X_clipped = np.clip(
            X_scaled,
            self.lower_bounds,
            self.upper_bounds
        )

        # ====================================================
        # STEP 4
        # FLOAT32
        # ====================================================

        X_clipped = X_clipped.astype(
            np.float32
        )

        return X_clipped

    # ========================================================
    # TRANSFORM ONE RECORD
    # ========================================================

    def transform_one(
        self,
        record
    ):
        """
        Transform one Kafka record.

        Returns:

            shape = [1, 16]
        """

        return self.transform(
            [record]
        )


# ============================================================
# LOAD PREPROCESSOR
# ============================================================

def load_preprocessor(
    scaler_path=SCALER_PATH,
    config_path=PREPROCESSING_CONFIG_PATH,
    selected_features=SELECTED_FEATURES,
):
    """
    Load the v3 preprocessing artifacts.
    """

    if selected_features != SELECTED_FEATURES:

        raise ValueError(
            "selected_features does not match "
            "config.SELECTED_FEATURES."
        )

    return TrafficPreprocessor(
        scaler_path=scaler_path,
        config_path=config_path,
    )