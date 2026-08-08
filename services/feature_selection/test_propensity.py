import pandas as pd

from propensity_score import PropensityScore

df = pd.DataFrame({

    "A":[0,0,0,1,1,1],

    "B":[0,1,0,1,0,1],

    "label":[
        "BENIGN",
        "BENIGN",
        "DOS",
        "DOS",
        "DOS",
        "BENIGN"
    ]
})

ps = PropensityScore()

scores = ps.calculate(df)

print(scores)