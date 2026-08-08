import json
from logger import IDSLogger

class FeatureRanker:
    def __init__(self):
        self.logger = IDSLogger().get_logger()

    def rank(self, markov_blanket, effects):
        ranked = [(feature, effects.get(feature, 0.0)) for feature in markov_blanket]
        ranked.sort(key=lambda x: x[1], reverse=True)
        return ranked

    def top_k(self, ranked_features, k=None):
        return ranked_features if k is None else ranked_features[:k]

    def selected_features(self, ranked_features):
        return [feature for feature, _ in ranked_features]

    def save(self, ranked_features, path="selected_features.json"):
        features = self.selected_features(ranked_features)
        with open(path, "w") as file:
            json.dump(features, file, indent=4)
        self.logger.info(f"Saved selected features -> {path}")

    def summary(self, original_count, ranked_features):
        selected = len(ranked_features)
        removed = original_count - selected
        self.logger.info("=" * 70)
        self.logger.info("FEATURE RANKING SUMMARY")
        self.logger.info("=" * 70)
        self.logger.info(f"Original Features : {original_count}")
        self.logger.info(f"Selected Features : {selected}")
        self.logger.info(f"Removed Features  : {removed}")
        self.logger.info("=" * 70)