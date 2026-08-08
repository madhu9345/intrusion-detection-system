import time
import json
import pandas as pd

from consumer import FeatureSelectionConsumer
from producer import FeatureSelectionProducer
from preprocessor import Preprocessor
from propensity_score import PropensityScore
from causal_effect import CausalEffect
from dmb import DoubleMarkovBlanket
from feature_ranker import FeatureRanker
from logger import IDSLogger

from config import (
    TARGET_COLUMN,
    BATCH_RECORDS,
    MIN_FEATURES,
    MAX_SELECTED_FEATURES,
    SAVE_SELECTED_FEATURES,
    SELECTED_FEATURES_FILE
)


class FeatureSelectionPipeline:

    def __init__(self):

        self.logger = IDSLogger().get_logger()

        # ======================================================
        # Components
        # ======================================================

        self.consumer = FeatureSelectionConsumer()

        self.producer = FeatureSelectionProducer()

        self.preprocessor = Preprocessor()

        self.propensity = PropensityScore()

        self.causal = CausalEffect()

        self.dmb = DoubleMarkovBlanket(
            self.propensity,
            self.causal,
            TARGET_COLUMN
        )

        self.ranker = FeatureRanker()

        # ======================================================
        # CUMULATIVE DMB FEATURES
        # ======================================================

        self.cumulative_dmb_features = []

        # ======================================================
        # CUMULATIVE EFFECTS
        #
        # Keep the strongest observed causal effect
        # for each feature across all batches.
        # ======================================================

        self.cumulative_effects = {}

        # ======================================================
        # Processed data
        #
        # Used later for secondary feature selection.
        # ======================================================

        self.processed_batches = []

        # ======================================================
        # Statistics
        # ======================================================

        self.total_input_records = 0

        self.total_batches = 0

        self.start_time = None

    # ==========================================================
    # Add DMB result cumulatively
    # ==========================================================

    def update_cumulative_dmb(
        self,
        markov_blanket
    ):

        if not markov_blanket:
            return

        for feature in markov_blanket:

            # Never allow target into feature selection.

            if feature == TARGET_COLUMN:
                continue

            # Do NOT remove old features.

            if feature not in self.cumulative_dmb_features:

                self.cumulative_dmb_features.append(
                    feature
                )

                self.logger.info(
                    f"CUMULATIVE DMB ADD | "
                    f"{feature:<35}"
                )

        self.logger.info(
            f"Cumulative DMB Size : "
            f"{len(self.cumulative_dmb_features)}"
        )

    # ==========================================================
    # Update cumulative causal effects
    # ==========================================================

    def update_cumulative_effects(
        self,
        effects
    ):

        for feature, score in effects.items():

            if feature == TARGET_COLUMN:
                continue

            score = float(score)

            previous = self.cumulative_effects.get(
                feature,
                0.0
            )

            # Keep strongest observed score.

            if score > previous:

                self.cumulative_effects[
                    feature
                ] = score

    # ==========================================================
    # Process one batch
    # ==========================================================

    def process_batch(
        self,
        batch
    ):

        if not batch:
            return

        batch_start = time.time()

        df = pd.DataFrame(batch)

        self.total_input_records += len(df)

        self.total_batches += 1

        self.logger.info("=" * 70)

        self.logger.info(
            f"Received Batch : "
            f"{len(df)} Records"
        )

        self.logger.info(
            f"Batch Number : "
            f"{self.total_batches}"
        )

        self.logger.info("=" * 70)

        # ======================================================
        # Target information
        # ======================================================

        if TARGET_COLUMN in df.columns:

            target_classes = (
                df[TARGET_COLUMN]
                .nunique(
                    dropna=True
                )
            )

            self.logger.info(
                f"Target Classes : "
                f"{target_classes}"
            )

        # ======================================================
        # K-means preprocessing
        # ======================================================

        processed_df = (
            self.preprocessor.process(
                df
            )
        )

        # ======================================================
        # Candidate features
        #
        # IMPORTANT:
        # target is NEVER a candidate feature.
        # ======================================================

        feature_columns = [
            column
            for column in processed_df.columns
            if column != TARGET_COLUMN
        ]

        self.logger.info(
            f"Candidate Features : "
            f"{len(feature_columns)}"
        )

        # ======================================================
        # Keep processed batch for final secondary selection
        # ======================================================

        self.processed_batches.append(
            processed_df
        )

        # ======================================================
        # DMB
        # ======================================================

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
            f"{TARGET_COLUMN}"
        )

        self.logger.info("=" * 70)

        # ------------------------------------------------------
        # DMB requires target variation.
        # ------------------------------------------------------

        target_classes = (
            processed_df[TARGET_COLUMN]
            .nunique(
                dropna=True
            )
            if TARGET_COLUMN in processed_df.columns
            else 0
        )

        if target_classes < 2:

            self.logger.warning(
                f"Target has only "
                f"{target_classes} class(es)."
            )

            self.logger.warning(
                "DMB skipped for this batch."
            )

            markov_blanket = []

            effects = {}

        else:

            # --------------------------------------------------
            # Run DMB
            # --------------------------------------------------

            markov_blanket = self.dmb.run(
                processed_df,
                feature_columns
            )

            # --------------------------------------------------
            # Calculate causal effects
            # --------------------------------------------------

            self.logger.info(
                "Calculating causal effects "
                "for candidate features..."
            )

            distributions = (
                self.propensity.calculate_all_fast(
                    processed_df,
                    feature_columns,
                    TARGET_COLUMN,
                    []
                )
            )

            effects = (
                self.causal.calculate(
                    distributions
                )
            )

        # ======================================================
        # IMPORTANT:
        #
        # Add current DMB result to cumulative result.
        #
        # Existing features are NEVER removed.
        # ======================================================

        self.update_cumulative_dmb(
            markov_blanket
        )

        # ======================================================
        # Update cumulative causal scores
        # ======================================================

        self.update_cumulative_effects(
            effects
        )

        # ======================================================
        # Batch summary
        # ======================================================

        self.logger.info("=" * 70)

        self.logger.info(
            "BATCH DMB RESULT"
        )

        self.logger.info("=" * 70)

        self.logger.info(
            f"Current Batch DMB : "
            f"{len(markov_blanket)}"
        )

        self.logger.info(
            f"Cumulative DMB    : "
            f"{len(self.cumulative_dmb_features)}"
        )

        if markov_blanket:

            self.logger.info(
                f"Current DMB : "
                f"{markov_blanket}"
            )

        self.logger.info("=" * 70)

        elapsed = time.time() - batch_start

        self.logger.info(
            f"Batch Execution Time : "
            f"{elapsed:.2f} sec"
        )

    # ==========================================================
    # Final Feature Selection
    # ==========================================================

    def final_feature_selection(self):

        self.logger.info("=" * 70)

        self.logger.info(
            "FINAL CUMULATIVE FEATURE SELECTION"
        )

        self.logger.info("=" * 70)

        # ======================================================
        # Step 1
        # Cumulative DMB result
        # ======================================================

        dmb_features = list(
            self.cumulative_dmb_features
        )

        # Remove duplicates

        dmb_features = list(
            dict.fromkeys(
                dmb_features
            )
        )

        # Never allow target

        dmb_features = [
            feature
            for feature in dmb_features
            if feature != TARGET_COLUMN
        ]

        self.logger.info(
            f"Cumulative DMB Features : "
            f"{len(dmb_features)}"
        )

        self.logger.info(
            f"DMB Features : "
            f"{dmb_features}"
        )

        # ======================================================
        # Step 2
        # Rank cumulative DMB features
        # ======================================================

        dmb_ranked = []

        for feature in dmb_features:

            score = self.cumulative_effects.get(
                feature,
                0.0
            )

            dmb_ranked.append(
                (
                    feature,
                    score
                )
            )

        dmb_ranked.sort(
            key=lambda item: item[1],
            reverse=True
        )

        # ======================================================
        # Step 3
        # If cumulative DMB >= minimum
        #
        # DO NOT use secondary selection.
        # ======================================================

        if len(dmb_ranked) >= MIN_FEATURES:

            final_features = [
                feature
                for feature, score
                in dmb_ranked[
                    :MAX_SELECTED_FEATURES
                ]
            ]

            self.logger.info(
                "Cumulative DMB already "
                "satisfies minimum feature requirement."
            )

        else:

            # ==================================================
            # DMB < minimum
            #
            # Keep ALL cumulative DMB features.
            # Then add only the missing number.
            # ==================================================

            final_features = [
                feature
                for feature, score
                in dmb_ranked
            ]

            required = (
                MIN_FEATURES
                - len(final_features)
            )

            self.logger.warning(
                f"Cumulative DMB selected only "
                f"{len(final_features)} features."
            )

            self.logger.warning(
                f"Minimum required : "
                f"{MIN_FEATURES}"
            )

            self.logger.info(
                f"Need {required} additional features."
            )

            # ==================================================
            # Combine all processed batches
            # ==================================================

            if self.processed_batches:

                combined_df = pd.concat(
                    self.processed_batches,
                    ignore_index=True
                )

                all_features = [
                    column
                    for column in combined_df.columns
                    if column != TARGET_COLUMN
                ]

                # ==================================================
                # Secondary ranking
                # ==================================================

                secondary_ranked = (
                    self.ranker.secondary_rank(
                        combined_df,
                        all_features,
                        TARGET_COLUMN,
                        excluded_features=set(
                            final_features
                        )
                    )
                )

                # ==================================================
                # Add ONLY missing features
                # ==================================================

                for item in secondary_ranked:

                    feature = item[0]

                    if feature in final_features:
                        continue

                    final_features.append(
                        feature
                    )

                    self.logger.info(
                        f"SECONDARY ADD | "
                        f"{feature:<35}"
                    )

                    # STOP exactly at minimum.

                    if len(final_features) >= MIN_FEATURES:
                        break

            # ==================================================
            # Maximum safety limit
            # ==================================================

            final_features = (
                final_features[
                    :MAX_SELECTED_FEATURES
                ]
            )

        # ======================================================
        # Remove duplicates
        # ======================================================

        final_features = list(
            dict.fromkeys(
                final_features
            )
        )

        # ======================================================
        # Never include label
        # ======================================================

        final_features = [
            feature
            for feature in final_features
            if feature != TARGET_COLUMN
        ]

        # ======================================================
        # Final output
        # ======================================================

        self.logger.info("=" * 70)

        self.logger.info(
            "FINAL SELECTED FEATURES"
        )

        self.logger.info("=" * 70)

        self.logger.info(
            f"Final Feature Count : "
            f"{len(final_features)}"
        )

        for index, feature in enumerate(
            final_features,
            start=1
        ):

            score = (
                self.cumulative_effects.get(
                    feature,
                    0.0
                )
            )

            self.logger.info(
                f"{index:02d}. "
                f"{feature:<35} "
                f"Score={score:.8f}"
            )

        self.logger.info("=" * 70)

        # ======================================================
        # Save feature list
        # ======================================================

        if SAVE_SELECTED_FEATURES:

            with open(
                SELECTED_FEATURES_FILE,
                "w",
                encoding="utf-8"
            ) as file:

                json.dump(
                    final_features,
                    file,
                    indent=4
                )

            self.logger.info(
                f"Saved feature list -> "
                f"{SELECTED_FEATURES_FILE}"
            )

        return final_features

    # ==========================================================
    # Publish final selected features
    # ==========================================================

    def publish_final_features(
        self,
        final_features
    ):

        if not final_features:

            self.logger.warning(
                "No final features available."
            )

            return

        # ======================================================
        # We publish records only AFTER the complete
        # cumulative DMB process has finished.
        # ======================================================

        self.logger.info("=" * 70)

        self.logger.info(
            "Publishing Final Selected Features"
        )

        self.logger.info("=" * 70)

        output_records = 0

        # ------------------------------------------------------
        # Process all processed batches again
        # ------------------------------------------------------

        for processed_df in self.processed_batches:

            valid_features = [
                feature
                for feature in final_features
                if feature in processed_df.columns
            ]

            if not valid_features:
                continue

            output_df = (
                processed_df[
                    valid_features
                    + [TARGET_COLUMN]
                ]
            )

            for record in output_df.to_dict(
                orient="records"
            ):

                self.producer.send(
                    record
                )

                output_records += 1

        self.producer.flush()

        # ======================================================
        # Output summary
        # ======================================================

        self.logger.info("=" * 70)

        self.logger.info(
            "SELECTED FEATURES OUTPUT"
        )

        self.logger.info("=" * 70)

        self.logger.info(
            f"Output Records : "
            f"{output_records:,}"
        )

        self.logger.info(
            f"Output Features : "
            f"{len(final_features)}"
        )

        self.logger.info(
            f"Features : "
            f"{final_features}"
        )

        self.logger.info("=" * 70)

    # ==========================================================
    # Run
    # ==========================================================

    def run(self):

        self.start_time = time.time()

        self.logger.info(
            "Starting Feature Selection Pipeline..."
        )

        batch = []

        try:

            for record in self.consumer.consume():

                batch.append(
                    record
                )

                if len(batch) >= BATCH_RECORDS:

                    self.process_batch(
                        batch
                    )

                    batch.clear()

            # ==================================================
            # Process final partial batch
            # ==================================================

            if batch:

                self.process_batch(
                    batch
                )

                batch.clear()

            # ==================================================
            # IMPORTANT:
            #
            # We wait until ALL batches have been processed.
            #
            # Only now do we determine final features.
            # ==================================================

            final_features = (
                self.final_feature_selection()
            )

            # ==================================================
            # Publish using final cumulative feature list
            # ==================================================

            self.publish_final_features(
                final_features
            )

        except KeyboardInterrupt:

            self.logger.info(
                "Stopping Feature Selection Pipeline..."
            )

        except Exception as error:

            self.logger.exception(
                f"Pipeline Error : {error}"
            )

        finally:

            try:
                self.consumer.close()
            except Exception:
                pass

            try:
                self.producer.close()
            except Exception:
                pass

            total_time = (
                time.time()
                - self.start_time
                if self.start_time
                else 0
            )

            self.logger.info("=" * 70)

            self.logger.info(
                "PIPELINE SUMMARY"
            )

            self.logger.info("=" * 70)

            self.logger.info(
                f"Total Input Records : "
                f"{self.total_input_records:,}"
            )

            self.logger.info(
                f"Total Batches       : "
                f"{self.total_batches}"
            )

            self.logger.info(
                f"Cumulative DMB      : "
                f"{len(self.cumulative_dmb_features)}"
            )

            self.logger.info(
                f"Execution Time      : "
                f"{total_time:.2f} sec"
            )

            self.logger.info("=" * 70)


# ==============================================================
# MAIN
# ==============================================================

if __name__ == "__main__":

    pipeline = FeatureSelectionPipeline()

    pipeline.run()