import pandas as pd
import numpy as np
from config import MAX_CMB_CONDITIONING_COLUMNS
from logger import IDSLogger

class PropensityScore:
    def __init__(self, max_cmb=MAX_CMB_CONDITIONING_COLUMNS, epsilon=1e-12):
        self.max_cmb = max_cmb
        self.epsilon = epsilon
        self.logger = IDSLogger().get_logger()

    def calculate_all_fast(self, df: pd.DataFrame, features: list, target: str, cmb: list) -> dict:
        if not features:
            return {}

        distributions = {}
        cmb_cols = list(cmb) if cmb else []

        if len(cmb_cols) > self.max_cmb:
            cmb_cols = cmb_cols[-self.max_cmb:]

        for feature in features:
            group_cols = cmb_cols + [feature]
            counts = df.groupby(group_cols + [target], observed=True).size().unstack(fill_value=0)
            
            if counts.empty:
                distributions[feature] = {}
                continue

            sums = counts.sum(axis=1).values[:, None]
            probs = (counts.values + self.epsilon) / (sums + self.epsilon * counts.shape[1])
            
            prob_dict = {}
            for idx, row_prob in zip(counts.index, probs):
                if cmb_cols:
                    cmb_key = tuple(idx[:-1]) if isinstance(idx, tuple) else (idx,)
                    x_val = idx[-1] if isinstance(idx, tuple) else idx
                else:
                    cmb_key = ()
                    x_val = idx

                if cmb_key not in prob_dict:
                    prob_dict[cmb_key] = {}
                prob_dict[cmb_key][x_val] = row_prob

            distributions[feature] = prob_dict

        return distributions