from feature_ranker import FeatureRanker

effects = {

    "A":0.91,

    "B":0.42,

    "C":0.75,

    "D":0.28

}

mb = [

    "A",

    "B",

    "C",

    "D"

]

ranker = FeatureRanker()

ranked = ranker.rank(

    mb,

    effects

)

print(ranked)

print()

print(

    ranker.selected_features(

        ranked

    )

)

ranker.summary(

    4,

    ranked

)