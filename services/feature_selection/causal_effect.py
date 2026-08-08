import numpy as np
from config import ALPHA_PERCENTILE, MIN_FEATURE_SCORE
from logger import IDSLogger

class CausalEffect:
    def __init__(self, alpha_percentile=ALPHA_PERCENTILE, min_score=MIN_FEATURE_SCORE, epsilon=1e-12):
        self.logger = IDSLogger().get_logger()
        self.alpha_percentile = alpha_percentile
        self.min_score = min_score
        self.epsilon = float(epsilon)

    def max_kl_within_cmb(self, per_x_distribution: dict) -> float:
        if len(per_x_distribution) < 2:
            return 0.0

        p_matrices = np.array(list(per_x_distribution.values()), dtype=np.float64)
        n_x = p_matrices.shape[0]

        if n_x < 2:
            return 0.0

        p = p_matrices[:, np.newaxis, :]
        q = p_matrices[np.newaxis, :, :]

        kl_matrix = np.sum((p + self.epsilon) * np.log((p + self.epsilon) / (q + self.epsilon)), axis=-1)
        np.fill_diagonal(kl_matrix, 0.0)

        return float(np.max(kl_matrix))

    def causal_effect(self, conditional_distributions: dict) -> float:
        if not conditional_distributions:
            return 0.0

        max_effect = 0.0
        for cmb_key, per_x_distribution in conditional_distributions.items():
            dkl_cmb_i = self.max_kl_within_cmb(per_x_distribution)
            if dkl_cmb_i > max_effect:
                max_effect = dkl_cmb_i

        return max_effect

    def calculate(self, distributions_by_feature: dict) -> dict:
        effects = {}
        for feature, conditional_distributions in distributions_by_feature.items():
            effects[feature] = self.causal_effect(conditional_distributions)
        return effects

    def adaptive_threshold(self, effects: dict) -> float:
        values = list(effects.values())
        if not values:
            return 0.0

        calculated_thr = float(np.percentile(values, self.alpha_percentile))
        if calculated_thr == 0.0 and max(values, default=0.0) > 0.0:
            return self.min_score
        return calculated_thr

    def independent(self, ce_value: float, threshold: float) -> bool:
        if threshold == 0.0:
            return ce_value <= 0.0
        return ce_value < threshold

    def test_independence(self, feature: str, effects: dict):
        threshold = self.adaptive_threshold(effects)
        ce_value = effects.get(feature, 0.0)
        return self.independent(ce_value, threshold), ce_value, threshold