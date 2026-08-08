from config import TARGET_COLUMN
from logger import IDSLogger

class DoubleMarkovBlanket:
    def __init__(self, propensity_score, causal_effect, target=TARGET_COLUMN):
        self.propensity_score = propensity_score
        self.causal_effect = causal_effect
        self.target = target
        self.logger = IDSLogger().get_logger()
        self.markov_blanket = []

    def _causal_effects_for_population(self, dataframe, population, cmb):
        distributions = self.propensity_score.calculate_all_fast(
            dataframe, population, self.target, cmb
        )
        return self.causal_effect.calculate(distributions)

    def growth_phase(self, dataframe, feature_columns):
        self.logger.info("=" * 70)
        self.logger.info("DMB Growth Phase (Algorithm 1, lines 1-6)")
        self.logger.info("=" * 70)

        cmb_growth = []
        remaining = list(feature_columns)

        effects = self._causal_effects_for_population(dataframe, remaining, cmb_growth)

        for feature in list(remaining):
            if feature not in remaining:
                continue

            is_independent, ce_value, threshold = self.causal_effect.test_independence(feature, effects)

            if is_independent:
                remaining.remove(feature)
                self.logger.info(f"Growth | DROP  {feature:<35s} CE={ce_value:.5f} < thr={threshold:.5f}")
            else:
                cmb_growth.append(feature)
                remaining.remove(feature)
                self.logger.info(f"Growth | KEEP  {feature:<35s} CE={ce_value:.5f} >= thr={threshold:.5f}")

                if remaining:
                    effects = self._causal_effects_for_population(dataframe, remaining, cmb_growth)

        self.markov_blanket = cmb_growth
        self.logger.info(f"Growth Phase Complete | |CMBG| = {len(cmb_growth)}")
        return cmb_growth

    def shrink_phase(self, dataframe):
        self.logger.info("=" * 70)
        self.logger.info("DMB Shrink Phase (Algorithm 1, lines 7-13)")
        self.logger.info("=" * 70)

        cmb_shrink = list(self.markov_blanket)
        if not cmb_shrink:
            self.logger.warning("Markov Blanket empty before shrink phase. Skipping shrink.")
            return []

        flag = True

        while flag:
            k = len(cmb_shrink)
            for feature in list(cmb_shrink):
                if feature not in cmb_shrink:
                    continue

                conditioning_set = [f for f in cmb_shrink if f != feature]
                effects = self._causal_effects_for_population(dataframe, cmb_shrink, conditioning_set)
                is_independent, ce_value, threshold = self.causal_effect.test_independence(feature, effects)

                if is_independent:
                    cmb_shrink.remove(feature)
                    self.logger.info(f"Shrink | REMOVE {feature:<35s} CE={ce_value:.5f} < thr={threshold:.5f}")

            if k == len(cmb_shrink):
                flag = False

        self.markov_blanket = cmb_shrink
        self.logger.info(f"Shrink Phase Complete | |MBY| = {len(cmb_shrink)}")
        return cmb_shrink

    def run(self, dataframe, feature_columns):
        self.growth_phase(dataframe, feature_columns)
        self.shrink_phase(dataframe)
        self.logger.info("=" * 70)
        self.logger.info(f"Final Markov Blanket (MBY) : {self.markov_blanket}")
        self.logger.info("=" * 70)
        return self.markov_blanket