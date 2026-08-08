import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from logger import IDSLogger
from config import NUM_CLASSES, RANDOM_STATE, TARGET_COLUMN

class Preprocessor:
    def __init__(self, num_classes=NUM_CLASSES):
        self.logger = IDSLogger().get_logger()
        self.num_classes = num_classes

    def process_feature(self, series: pd.Series) -> np.ndarray:
        values = series.fillna(0).astype(np.float64).values
        if len(values) == 0 or np.all(values == values[0]):
            return np.zeros(len(values), dtype=np.int32)

        reshaped = values.reshape(-1, 1)
        model = KMeans(n_clusters=2, random_state=RANDOM_STATE, n_init="auto")
        cluster = model.fit_predict(reshaped)
        centers = model.cluster_centers_.flatten()
        
        overall_mean = np.mean(values)
        normal_cluster = int(np.argmin(np.abs(centers - overall_mean)))
        
        processed = np.zeros(len(values), dtype=np.int32)
        abnormal_mask = (cluster != normal_cluster)
        
        if not np.any(abnormal_mask):
            return processed

        normal_center = centers[normal_cluster]
        scores = np.abs(values[abnormal_mask] - normal_center)
        max_score = scores.max()

        if max_score == 0:
            processed[abnormal_mask] = 1
            return processed

        levels = np.ceil((scores / max_score) * self.num_classes).astype(np.int32)
        processed[abnormal_mask] = np.clip(levels, 1, self.num_classes)
        return processed

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        self.logger.info("=" * 70)
        self.logger.info("Preprocessing Features (Optimized)")
        self.logger.info("=" * 70)

        processed_df = df.copy()

        # Clean column names and standardize target column naming
        processed_df.columns = [str(col).strip() for col in processed_df.columns]
        for col in processed_df.columns:
            if col.lower() == TARGET_COLUMN.lower() and col != TARGET_COLUMN:
                processed_df.rename(columns={col: TARGET_COLUMN}, inplace=True)

        for column in processed_df.columns:
            if column.lower() == TARGET_COLUMN.lower():
                continue
            processed_df[column] = self.process_feature(processed_df[column])

        feature_count = len([c for c in processed_df.columns if c.lower() != TARGET_COLUMN.lower()])
        self.logger.info(f"Processed {feature_count} features.")
        return processed_df