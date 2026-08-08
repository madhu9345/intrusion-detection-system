import time
import pandas as pd

from consumer import FeatureSelectionConsumer
from producer import FeatureSelectionProducer
from preprocessor import Preprocessor
from propensity_score import PropensityScore
from causal_effect import CausalEffect
from dmb import DoubleMarkovBlanket
from feature_ranker import FeatureRanker
from logger import IDSLogger
from config import TARGET_COLUMN, BATCH_RECORDS

class FeatureSelectionPipeline:
    def __init__(self):
        self.logger = IDSLogger().get_logger()
        self.consumer = FeatureSelectionConsumer()
        self.producer = FeatureSelectionProducer()
        self.preprocessor = Preprocessor()
        self.propensity = PropensityScore()
        self.causal = CausalEffect()
        self.dmb = DoubleMarkovBlanket(self.propensity, self.causal, TARGET_COLUMN)
        self.ranker = FeatureRanker()

    def process_batch(self, batch):
        if not batch:
            return

        start = time.time()
        df = pd.DataFrame(batch)
        self.logger.info(f"Received Batch : {len(df)} Records")

        processed_df = self.preprocessor.process(df)
        feature_columns = [col for col in processed_df.columns if col != TARGET_COLUMN]

        markov_blanket = self.dmb.run(processed_df, feature_columns)

        distributions = self.propensity.calculate_all_fast(
            processed_df, markov_blanket, TARGET_COLUMN, []
        )
        effects = self.causal.calculate(distributions)

        ranked = self.ranker.rank(markov_blanket, effects)
        selected_features = self.ranker.selected_features(ranked)

        if TARGET_COLUMN not in selected_features:
            selected_features.append(TARGET_COLUMN)

        self.logger.info(f"Selected Features : {selected_features}")

        filtered_df = processed_df[selected_features]

        for record in filtered_df.to_dict(orient="records"):
            self.producer.send(record)

        self.producer.flush()
        end = time.time()

        self.logger.info("=" * 70)
        self.logger.info("FEATURE SELECTION COMPLETE")
        self.logger.info("=" * 70)
        self.logger.info(f"Execution Time : {end - start:.2f} sec")
        self.logger.info("=" * 70)

    def run(self):
        batch = []
        self.logger.info("=" * 70)
        self.logger.info("Feature Selection Service Started")
        self.logger.info("=" * 70)

        try:
            for record in self.consumer.consume():
                batch.append(record)
                if len(batch) >= BATCH_RECORDS:
                    self.process_batch(batch)
                    batch.clear()

        except KeyboardInterrupt:
            self.logger.info("Stopping Feature Selection Service...")
        finally:
            if batch:
                self.process_batch(batch)
                batch.clear()

            self.consumer.close()
            self.producer.close()

if __name__ == "__main__":
    pipeline = FeatureSelectionPipeline()
    pipeline.run()