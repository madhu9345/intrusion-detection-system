from config import (
    TARGET_COLUMN,
    MIN_TARGET_CLASSES,
    MAX_CMB_CONDITIONING_COLUMNS
)

from logger import IDSLogger


class DoubleMarkovBlanket:

    def __init__(
        self,
        propensity_score,
        causal_effect,
        target=TARGET_COLUMN
    ):

        self.propensity_score = (
            propensity_score
        )

        self.causal_effect = (
            causal_effect
        )

        self.target = target

        self.logger = (
            IDSLogger().get_logger()
        )

        self.markov_blanket = []

    # ==========================================================
    # Validate target
    # ==========================================================

    def valid_target(
        self,
        dataframe
    ):

        if self.target not in dataframe.columns:

            self.logger.error(
                f"Target '{self.target}' "
                "not found."
            )

            return False

        class_count = (
            dataframe[self.target]
            .nunique(dropna=True)
        )

        self.logger.info(
            f"Target Classes : {class_count}"
        )

        if class_count < MIN_TARGET_CLASSES:

            self.logger.warning(
                f"Target column '{self.target}' "
                f"has only {class_count} "
                "distinct value(s)."
            )

            self.logger.warning(
                "DMB requires at least "
                "two target classes."
            )

            return False

        return True

    # ==========================================================
    # Calculate CE population
    # ==========================================================

    def _causal_effects_for_population(
        self,
        dataframe,
        population,
        cmb
    ):

        if not population:
            return {}

        # ------------------------------------------------------
        # Remove target from population
        # ------------------------------------------------------

        population = [
            feature
            for feature in population
            if feature != self.target
        ]

        if not population:
            return {}

        # ------------------------------------------------------
        # CMB used for conditioning
        # ------------------------------------------------------

        conditioning_set = [
            feature
            for feature in cmb
            if feature != self.target
        ]

        if (
            len(conditioning_set)
            > MAX_CMB_CONDITIONING_COLUMNS
        ):

            self.logger.warning(
                f"CMB size "
                f"({len(conditioning_set)}) "
                f"exceeds "
                f"MAX_CMB_CONDITIONING_COLUMNS "
                f"({MAX_CMB_CONDITIONING_COLUMNS}); "
                "using the most recent conditioning "
                "columns for empirical estimation."
            )

            conditioning_set = (
                conditioning_set[
                    -MAX_CMB_CONDITIONING_COLUMNS:
                ]
            )

        distributions = (
            self.propensity_score
            .calculate_all_fast(
                dataframe,
                population,
                self.target,
                conditioning_set
            )
        )

        return self.causal_effect.calculate(
            distributions
        )

    # ==========================================================
    # Growth Phase
    #
    # Algorithm 1:
    #
    # CMBG = []
    # for X in S:
    #   if X independent Y | CMBG:
    #       remove X
    #   else:
    #       CMBG.append(X)
    # ==========================================================

    def growth_phase(
        self,
        dataframe,
        feature_columns
    ):

        self.logger.info("=" * 70)
        self.logger.info(
            "DMB Growth Phase"
        )
        self.logger.info("=" * 70)

        cmb_growth = []

        remaining = [
            feature
            for feature in feature_columns
            if feature != self.target
        ]

        # ------------------------------------------------------
        # Sequential Growth
        # ------------------------------------------------------

        for feature in list(remaining):

            if feature not in remaining:
                continue

            # Population = current remaining features.
            effects = (
                self._causal_effects_for_population(
                    dataframe,
                    remaining,
                    cmb_growth
                )
            )

            if feature not in effects:

                self.logger.info(
                    f"Growth | DROP  "
                    f"{feature:<35s} "
                    "No valid conditional distribution"
                )

                remaining.remove(feature)

                continue

            (
                is_independent,
                ce_value,
                threshold
            ) = (
                self.causal_effect
                .test_independence(
                    feature,
                    effects
                )
            )

            if is_independent:

                remaining.remove(feature)

                self.logger.info(
                    f"Growth | DROP  "
                    f"{feature:<35s} "
                    f"CE={ce_value:.6f} "
                    f"< thr={threshold:.6f}"
                )

            else:

                cmb_growth.append(
                    feature
                )

                remaining.remove(
                    feature
                )

                self.logger.info(
                    f"Growth | KEEP  "
                    f"{feature:<35s} "
                    f"CE={ce_value:.6f} "
                    f">= thr={threshold:.6f}"
                )

        self.markov_blanket = (
            cmb_growth
        )

        self.logger.info(
            f"Growth Complete | "
            f"|CMBG| = {len(cmb_growth)}"
        )

        return cmb_growth

    # ==========================================================
    # Shrink Phase
    #
    # Standard interpretation of Algorithm 1:
    #
    # test X against CMBS - {X}
    # ==========================================================

    def shrink_phase(
        self,
        dataframe
    ):

        self.logger.info("=" * 70)
        self.logger.info(
            "DMB Shrink Phase"
        )
        self.logger.info("=" * 70)

        cmb_shrink = list(
            self.markov_blanket
        )

        if not cmb_shrink:

            self.logger.warning(
                "Empty Markov Blanket. "
                "Shrink skipped."
            )

            return []

        changed = True

        while changed:

            changed = False

            for feature in list(
                cmb_shrink
            ):

                if feature not in cmb_shrink:
                    continue

                # --------------------------------------------------
                # X is NOT included in its own conditioning set.
                # --------------------------------------------------

                conditioning_set = [
                    f
                    for f in cmb_shrink
                    if f != feature
                ]

                population = [
                    f
                    for f in cmb_shrink
                    if f != self.target
                ]

                # Include feature in population so
                # its CE can be evaluated.
                if feature not in population:

                    population.append(
                        feature
                    )

                effects = (
                    self._causal_effects_for_population(
                        dataframe,
                        population,
                        conditioning_set
                    )
                )

                if feature not in effects:

                    self.logger.info(
                        f"Shrink | KEEP   "
                        f"{feature:<35s} "
                        "No valid conditional distribution"
                    )

                    continue

                (
                    is_independent,
                    ce_value,
                    threshold
                ) = (
                    self.causal_effect
                    .test_independence(
                        feature,
                        effects
                    )
                )

                if is_independent:

                    cmb_shrink.remove(
                        feature
                    )

                    changed = True

                    self.logger.info(
                        f"Shrink | REMOVE "
                        f"{feature:<35s} "
                        f"CE={ce_value:.6f} "
                        f"< thr={threshold:.6f}"
                    )

                else:

                    self.logger.info(
                        f"Shrink | KEEP   "
                        f"{feature:<35s} "
                        f"CE={ce_value:.6f} "
                        f">= thr={threshold:.6f}"
                    )

        self.markov_blanket = (
            cmb_shrink
        )

        self.logger.info(
            f"Shrink Complete | "
            f"|MBY| = {len(cmb_shrink)}"
        )

        return cmb_shrink

    # ==========================================================
    # Full DMB
    # ==========================================================

    def run(
        self,
        dataframe,
        feature_columns
    ):

        self.logger.info("=" * 70)
        self.logger.info(
            "Starting Double Markov Blanket"
        )

        self.logger.info(
            f"Input Features : "
            f"{len(feature_columns)}"
        )

        self.logger.info(
            f"Target Column : "
            f"{self.target}"
        )

        self.logger.info("=" * 70)

        # ------------------------------------------------------
        # Do not run DMB on one-class data.
        # ------------------------------------------------------

        if not self.valid_target(
            dataframe
        ):

            self.markov_blanket = []

            self.logger.warning(
                "DMB selection skipped "
                "for this dataset."
            )

            return []

        # ------------------------------------------------------
        # Remove target
        # ------------------------------------------------------

        features = [
            feature
            for feature in feature_columns
            if feature != self.target
        ]

        # ------------------------------------------------------
        # Growth
        # ------------------------------------------------------

        self.growth_phase(
            dataframe,
            features
        )

        # ------------------------------------------------------
        # Shrink
        # ------------------------------------------------------

        self.shrink_phase(
            dataframe
        )

        # ------------------------------------------------------
        # Final result
        # ------------------------------------------------------

        self.markov_blanket = [
            feature
            for feature in self.markov_blanket
            if feature != self.target
        ]

        self.logger.info("=" * 70)

        self.logger.info(
            f"Final Markov Blanket : "
            f"{self.markov_blanket}"
        )

        self.logger.info(
            f"Final Feature Count : "
            f"{len(self.markov_blanket)}"
        )

        self.logger.info("=" * 70)

        return self.markov_blanket