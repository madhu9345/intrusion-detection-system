import numpy as np
import pandas as pd

from sklearn.cluster import KMeans

from logger import IDSLogger
from config import (
    NUM_CLASSES,
    RANDOM_STATE,
    TARGET_COLUMN
)


class Preprocessor:

    def __init__(
        self,
        num_classes=NUM_CLASSES
    ):

        self.logger = IDSLogger().get_logger()

        self.num_classes = int(
            num_classes
        )

    # ==========================================================
    # Process One Feature
    # ==========================================================

    def process_feature(
        self,
        series: pd.Series
    ) -> np.ndarray:

        # ------------------------------------------------------
        # Convert to numeric
        # ------------------------------------------------------

        values = pd.to_numeric(
            series,
            errors="coerce"
        )

        values = values.replace(
            [np.inf, -np.inf],
            np.nan
        )

        values = values.fillna(0)

        values = values.astype(
            np.float64
        ).values

        # ------------------------------------------------------
        # Empty feature
        # ------------------------------------------------------

        if len(values) == 0:

            return np.array(
                [],
                dtype=np.int32
            )

        # ------------------------------------------------------
        # Constant feature
        #
        # K-means cannot meaningfully split a
        # feature containing only one value.
        # ------------------------------------------------------

        if np.all(
            values == values[0]
        ):

            return np.zeros(
                len(values),
                dtype=np.int32
            )

        # ------------------------------------------------------
        # Remove numerical problems
        # ------------------------------------------------------

        if not np.all(
            np.isfinite(values)
        ):

            values = np.nan_to_num(
                values,
                nan=0.0,
                posinf=0.0,
                neginf=0.0
            )

        # ------------------------------------------------------
        # K-means
        #
        # Two clusters are used:
        #
        # Cluster 0
        # Cluster 1
        # ------------------------------------------------------

        unique_values = np.unique(
            values
        )

        n_clusters = min(
            2,
            len(unique_values)
        )

        if n_clusters < 2:

            return np.zeros(
                len(values),
                dtype=np.int32
            )

        reshaped = values.reshape(
            -1,
            1
        )

        try:

            model = KMeans(
                n_clusters=n_clusters,
                random_state=RANDOM_STATE,
                n_init=10
            )

            clusters = model.fit_predict(
                reshaped
            )

        except Exception as error:

            self.logger.warning(
                f"K-means failed for "
                f"feature '{series.name}': "
                f"{error}"
            )

            return np.zeros(
                len(values),
                dtype=np.int32
            )

        centers = (
            model.cluster_centers_
            .flatten()
        )

        # ------------------------------------------------------
        # Identify normal cluster
        #
        # Cluster whose center is closest
        # to the overall mean.
        # ------------------------------------------------------

        overall_mean = float(
            np.mean(values)
        )

        normal_cluster = int(
            np.argmin(
                np.abs(
                    centers -
                    overall_mean
                )
            )
        )

        # ------------------------------------------------------
        # Output representation
        # ------------------------------------------------------

        processed = np.zeros(
            len(values),
            dtype=np.int32
        )

        abnormal_mask = (
            clusters != normal_cluster
        )

        # ------------------------------------------------------
        # No abnormal observations
        # ------------------------------------------------------

        if not np.any(
            abnormal_mask
        ):

            return processed

        normal_center = float(
            centers[normal_cluster]
        )

        # ------------------------------------------------------
        # Distance from normal cluster
        # ------------------------------------------------------

        scores = np.abs(
            values[abnormal_mask]
            - normal_center
        )

        if len(scores) == 0:

            return processed

        max_score = float(
            np.max(scores)
        )

        # ------------------------------------------------------
        # Safety for zero distance
        # ------------------------------------------------------

        if max_score <= 0:

            processed[
                abnormal_mask
            ] = 1

            return processed

        # ------------------------------------------------------
        # Convert distance into levels
        #
        # Example with NUM_CLASSES = 15:
        #
        # low abnormality  -> 1
        # high abnormality -> 15
        # ------------------------------------------------------

        levels = np.ceil(
            (
                scores /
                max_score
            )
            * self.num_classes
        ).astype(
            np.int32
        )

        levels = np.clip(
            levels,
            1,
            self.num_classes
        )

        processed[
            abnormal_mask
        ] = levels

        return processed

    # ==========================================================
    # Process Entire DataFrame
    # ==========================================================

    def process(
        self,
        df: pd.DataFrame
    ) -> pd.DataFrame:

        self.logger.info("=" * 70)

        self.logger.info(
            "Preprocessing Features"
        )

        self.logger.info("=" * 70)

        if df is None:

            raise ValueError(
                "Input DataFrame is None."
            )

        if df.empty:

            self.logger.warning(
                "Input DataFrame is empty."
            )

            return df.copy()

        # ------------------------------------------------------
        # Copy input
        # ------------------------------------------------------

        processed_df = df.copy()

        # ------------------------------------------------------
        # Clean column names
        # ------------------------------------------------------

        processed_df.columns = [
            str(column).strip()
            for column
            in processed_df.columns
        ]

        # ------------------------------------------------------
        # Normalize target column name
        # ------------------------------------------------------

        target_column_found = None

        for column in processed_df.columns:

            if (
                str(column).lower()
                ==
                TARGET_COLUMN.lower()
            ):

                target_column_found = column
                break

        if (
            target_column_found is not None
            and
            target_column_found
            != TARGET_COLUMN
        ):

            processed_df.rename(
                columns={
                    target_column_found:
                    TARGET_COLUMN
                },
                inplace=True
            )

        # ------------------------------------------------------
        # Verify target
        # ------------------------------------------------------

        if TARGET_COLUMN not in processed_df.columns:

            raise ValueError(
                f"Target column "
                f"'{TARGET_COLUMN}' "
                f"not found in DataFrame."
            )

        # ------------------------------------------------------
        # Target statistics
        #
        # IMPORTANT:
        # Target is NOT K-means processed.
        # ------------------------------------------------------

        target_classes = (
            processed_df[
                TARGET_COLUMN
            ]
            .dropna()
            .nunique()
        )

        self.logger.info(
            f"Target Classes : "
            f"{target_classes}"
        )

        # ------------------------------------------------------
        # Process candidate features
        # ------------------------------------------------------

        feature_columns = [
            column
            for column
            in processed_df.columns
            if column != TARGET_COLUMN
        ]

        processed_count = 0

        constant_count = 0

        # ------------------------------------------------------
        # K-means preprocessing
        # ------------------------------------------------------

        for column in feature_columns:

            original = processed_df[
                column
            ]

            # Check whether feature is constant
            numeric_values = pd.to_numeric(
                original,
                errors="coerce"
            )

            numeric_values = (
                numeric_values
                .replace(
                    [np.inf, -np.inf],
                    np.nan
                )
                .fillna(0)
            )

            if (
                numeric_values.nunique()
                <= 1
            ):

                constant_count += 1

            processed_df[
                column
            ] = self.process_feature(
                original
            )

            processed_count += 1

        # ------------------------------------------------------
        # Final information
        # ------------------------------------------------------

        self.logger.info(
            f"Processed "
            f"{processed_count} features."
        )

        self.logger.info(
            f"Constant Features : "
            f"{constant_count}"
        )

        self.logger.info(
            f"Processed DataFrame Shape : "
            f"{processed_df.shape}"
        )

        # ------------------------------------------------------
        # Confirm label remains untouched
        # ------------------------------------------------------

        self.logger.info(
            f"Target Column '{TARGET_COLUMN}' "
            "was excluded from K-means preprocessing."
        )

        return processed_df