import numpy as np

from config import (
    ALPHA_PERCENTILE,
    MIN_FEATURE_SCORE
)

from logger import IDSLogger


class CausalEffect:

    def __init__(
        self,
        alpha_percentile=ALPHA_PERCENTILE,
        min_score=MIN_FEATURE_SCORE,
        epsilon=1e-12
    ):

        self.logger = IDSLogger().get_logger()

        self.alpha_percentile = (
            alpha_percentile
        )

        self.min_score = (
            min_score
        )

        self.epsilon = float(
            epsilon
        )

    # ==========================================================
    # KL(P || Q)
    # ==========================================================

    def kl_divergence(
        self,
        p,
        q
    ):

        keys = set(p.keys()).union(
            q.keys()
        )

        divergence = 0.0

        for key in keys:

            p_value = float(
                p.get(
                    key,
                    self.epsilon
                )
            )

            q_value = float(
                q.get(
                    key,
                    self.epsilon
                )
            )

            p_value = max(
                p_value,
                self.epsilon
            )

            q_value = max(
                q_value,
                self.epsilon
            )

            divergence += (
                p_value
                *
                np.log(
                    p_value / q_value
                )
            )

        return float(
            max(divergence, 0.0)
        )

    # ==========================================================
    # Eq. 13
    # ==========================================================

    def max_kl_within_cmb(
        self,
        per_x_distribution
    ):

        x_values = list(
            per_x_distribution.keys()
        )

        if len(x_values) < 2:
            return 0.0

        max_divergence = 0.0

        for i in range(
            len(x_values)
        ):

            for j in range(
                i + 1,
                len(x_values)
            ):

                p = (
                    per_x_distribution[
                        x_values[i]
                    ]
                )

                q = (
                    per_x_distribution[
                        x_values[j]
                    ]
                )

                # KL is directional.
                # Calculate both directions and
                # retain the larger one.
                d1 = self.kl_divergence(
                    p,
                    q
                )

                d2 = self.kl_divergence(
                    q,
                    p
                )

                divergence = max(
                    d1,
                    d2
                )

                if divergence > max_divergence:

                    max_divergence = (
                        divergence
                    )

        return float(
            max_divergence
        )

    # ==========================================================
    # Eq. 14
    # ==========================================================

    def causal_effect(
        self,
        conditional_distributions
    ):

        if not conditional_distributions:
            return 0.0

        max_effect = 0.0

        for (
            cmb_key,
            per_x_distribution
        ) in conditional_distributions.items():

            dkl = self.max_kl_within_cmb(
                per_x_distribution
            )

            max_effect = max(
                max_effect,
                dkl
            )

        return float(
            max_effect
        )

    # ==========================================================
    # Calculate CE for all features
    # ==========================================================

    def calculate(
        self,
        distributions_by_feature
    ):

        effects = {}

        for (
            feature,
            distributions
        ) in distributions_by_feature.items():

            effects[feature] = (
                self.causal_effect(
                    distributions
                )
            )

        self.logger.info(
            f"Causal Effects Computed : "
            f"{len(effects)}"
        )

        return effects

    # ==========================================================
    # Eq. 15
    # ==========================================================

    def adaptive_threshold(
        self,
        effects
    ):

        values = [
            float(value)
            for value in effects.values()
            if np.isfinite(value)
        ]

        if not values:
            return 0.0

        threshold = float(
            np.percentile(
                values,
                self.alpha_percentile
            )
        )

        return threshold

    # ==========================================================
    # Independence
    # ==========================================================

    def independent(
        self,
        ce_value,
        threshold
    ):

        # If every feature has zero effect,
        # they are indistinguishable using
        # the current data.
        if threshold == 0.0:

            return ce_value <= 0.0

        return ce_value < threshold

    # ==========================================================
    # Test
    # ==========================================================

    def test_independence(
        self,
        feature,
        effects
    ):

        threshold = (
            self.adaptive_threshold(
                effects
            )
        )

        ce_value = float(
            effects.get(
                feature,
                0.0
            )
        )

        independent = (
            self.independent(
                ce_value,
                threshold
            )
        )

        return (
            independent,
            ce_value,
            threshold
        )