import json
import numpy as np
import pandas as pd

from sklearn.feature_selection import mutual_info_classif

from config import (
    MIN_FEATURES,
    MAX_SELECTED_FEATURES,
    TARGET_COLUMN,
    RANDOM_STATE
)

from logger import IDSLogger


class FeatureRanker:

    def __init__(self):

        self.logger = IDSLogger().get_logger()

    # ==========================================================
    # Rank DMB Features
    # ==========================================================

    def rank(
        self,
        markov_blanket,
        effects
    ):

        ranked = []

        for feature in markov_blanket:

            if feature == TARGET_COLUMN:
                continue

            score = float(
                effects.get(
                    feature,
                    0.0
                )
            )

            ranked.append(
                (
                    feature,
                    score
                )
            )

        ranked.sort(
            key=lambda x: x[1],
            reverse=True
        )

        return ranked

    # ==========================================================
    # Secondary Feature Selection
    #
    # Mutual Information + Variance
    #
    # This is used ONLY when DMB gives fewer
    # than MIN_FEATURES.
    # ==========================================================

    def secondary_rank(
        self,
        dataframe,
        feature_columns,
        target,
        excluded_features=None
    ):

        if excluded_features is None:
            excluded_features = set()
        else:
            excluded_features = set(
                excluded_features
            )

        candidates = [
            feature
            for feature in feature_columns
            if feature != target
            and feature not in excluded_features
            and feature in dataframe.columns
        ]

        if not candidates:
            return []

        # ------------------------------------------------------
        # Remove constant features
        # ------------------------------------------------------

        valid_features = []

        for feature in candidates:

            unique_count = (
                dataframe[feature]
                .nunique(
                    dropna=False
                )
            )

            if unique_count <= 1:

                self.logger.info(
                    f"Secondary | DROP constant "
                    f"{feature}"
                )

                continue

            valid_features.append(
                feature
            )

        candidates = valid_features

        if not candidates:
            return []

        # ------------------------------------------------------
        # Numeric matrix
        # ------------------------------------------------------

        X = (
            dataframe[candidates]
            .apply(
                pd.to_numeric,
                errors="coerce"
            )
            .fillna(0)
        )

        # ------------------------------------------------------
        # Variance
        # ------------------------------------------------------

        variance_scores = (
            X.var(
                axis=0,
                ddof=0
            )
            .replace(
                [np.inf, -np.inf],
                0
            )
            .fillna(0)
        )

        # ------------------------------------------------------
        # Target
        # ------------------------------------------------------

        y = dataframe[target]

        # ------------------------------------------------------
        # Mutual Information
        #
        # Only possible when target contains
        # at least 2 classes.
        # ------------------------------------------------------

        target_classes = (
            y.nunique(
                dropna=True
            )
        )

        mi_scores = {
            feature: 0.0
            for feature in candidates
        }

        if target_classes >= 2:

            try:

                y_encoded = (
                    y.astype("category")
                    .cat.codes
                )

                mi_values = (
                    mutual_info_classif(
                        X,
                        y_encoded,
                        random_state=RANDOM_STATE
                    )
                )

                for feature, score in zip(
                    candidates,
                    mi_values
                ):

                    mi_scores[feature] = (
                        float(score)
                    )

            except Exception as error:

                self.logger.warning(
                    f"Mutual Information failed: "
                    f"{error}"
                )

        # ------------------------------------------------------
        # Normalize variance
        # ------------------------------------------------------

        max_variance = float(
            variance_scores.max()
        )

        if max_variance > 0:

            normalized_variance = (
                variance_scores
                / max_variance
            )

        else:

            normalized_variance = (
                variance_scores
                * 0.0
            )

        # ------------------------------------------------------
        # Normalize MI
        # ------------------------------------------------------

        max_mi = max(
            mi_scores.values(),
            default=0.0
        )

        if max_mi > 0:

            normalized_mi = {
                feature:
                score / max_mi
                for feature, score
                in mi_scores.items()
            }

        else:

            normalized_mi = {
                feature: 0.0
                for feature in candidates
            }

        # ------------------------------------------------------
        # Combined secondary score
        #
        # MI gets higher importance.
        # Variance prevents flat features.
        # ------------------------------------------------------

        ranked = []

        for feature in candidates:

            mi = normalized_mi.get(
                feature,
                0.0
            )

            variance = float(
                normalized_variance.get(
                    feature,
                    0.0
                )
            )

            score = (
                0.70 * mi
                +
                0.30 * variance
            )

            ranked.append(
                (
                    feature,
                    float(score),
                    float(mi),
                    float(variance)
                )
            )

        ranked.sort(
            key=lambda x: x[1],
            reverse=True
        )

        self.logger.info(
            "=" * 70
        )

        self.logger.info(
            "SECONDARY FEATURE RANKING"
        )

        self.logger.info(
            "=" * 70
        )

        for feature, score, mi, variance in ranked:

            self.logger.info(
                f"Secondary | "
                f"{feature:<35} "
                f"Score={score:.6f} "
                f"MI={mi:.6f} "
                f"Var={variance:.6f}"
            )

        self.logger.info(
            "=" * 70
        )

        return ranked

    # ==========================================================
    # Select DMB Features
    # ==========================================================

    def selected_features(
        self,
        ranked_features
    ):

        selected = []

        for feature, score in ranked_features:

            if feature == TARGET_COLUMN:
                continue

            if feature in selected:
                continue

            selected.append(
                feature
            )

            if len(selected) >= MAX_SELECTED_FEATURES:
                break

        return selected

    # ==========================================================
    # Hybrid Selection
    #
    # DMB first.
    # Secondary method fills only until minimum.
    # ==========================================================

    def hybrid_selection(
        self,
        dataframe,
        feature_columns,
        dmb_features,
        dmb_effects,
        target
    ):

        # ------------------------------------------------------
        # Rank DMB
        # ------------------------------------------------------

        dmb_ranked = self.rank(
            dmb_features,
            dmb_effects
        )

        selected = [
            feature
            for feature, score
            in dmb_ranked
        ]

        # ------------------------------------------------------
        # Remove duplicates
        # ------------------------------------------------------

        selected = list(
            dict.fromkeys(
                selected
            )
        )

        # ------------------------------------------------------
        # DMB already satisfies minimum
        # ------------------------------------------------------

        if len(selected) >= MIN_FEATURES:

            self.logger.info(
                f"DMB produced "
                f"{len(selected)} features."
            )

            self.logger.info(
                "Secondary selection not required."
            )

            return selected[
                :MAX_SELECTED_FEATURES
            ]

        # ------------------------------------------------------
        # DMB < minimum
        # ------------------------------------------------------

        self.logger.warning(
            f"DMB produced only "
            f"{len(selected)} features."
        )

        self.logger.warning(
            f"Required minimum = "
            f"{MIN_FEATURES}"
        )

        # ------------------------------------------------------
        # Secondary ranking
        # ------------------------------------------------------

        secondary = (
            self.secondary_rank(
                dataframe,
                feature_columns,
                target,
                excluded_features=selected
            )
        )

        # ------------------------------------------------------
        # Add only enough features to reach MIN_FEATURES
        # ------------------------------------------------------

        required = (
            MIN_FEATURES
            - len(selected)
        )

        self.logger.info(
            f"Secondary selection needs "
            f"{required} additional features."
        )

        added = 0

        for feature, score, mi, variance in secondary:

            if feature in selected:
                continue

            selected.append(
                feature
            )

            added += 1

            self.logger.info(
                f"Secondary | ADD "
                f"{feature:<35} "
                f"Score={score:.6f}"
            )

            # IMPORTANT:
            # Stop at minimum.
            if len(selected) >= MIN_FEATURES:
                break

        # ------------------------------------------------------
        # Maximum safety limit
        # ------------------------------------------------------

        selected = selected[
            :MAX_SELECTED_FEATURES
        ]

        self.logger.info(
            "=" * 70
        )

        self.logger.info(
            "HYBRID FEATURE SELECTION"
        )

        self.logger.info(
            "=" * 70
        )

        self.logger.info(
            f"DMB Features       : "
            f"{len(dmb_ranked)}"
        )

        self.logger.info(
            f"Secondary Added    : "
            f"{added}"
        )

        self.logger.info(
            f"Final Features     : "
            f"{len(selected)}"
        )

        self.logger.info(
            f"Features           : "
            f"{selected}"
        )

        self.logger.info(
            "=" * 70
        )

        return selected

    # ==========================================================
    # Save
    # ==========================================================

    def save(
        self,
        ranked_features,
        path
    ):

        features = []

        for item in ranked_features:

            if isinstance(item, tuple):

                feature = item[0]

            else:

                feature = item

            if feature == TARGET_COLUMN:
                continue

            if feature in features:
                continue

            features.append(
                feature
            )

            if len(features) >= MAX_SELECTED_FEATURES:
                break

        with open(
            path,
            "w",
            encoding="utf-8"
        ) as file:

            json.dump(
                features,
                file,
                indent=4
            )

        self.logger.info(
            f"Saved selected features -> "
            f"{path}"
        )