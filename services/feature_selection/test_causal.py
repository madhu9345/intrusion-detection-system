import pandas as pd

from causal_effect import CausalEffect

df = pd.DataFrame({

    "A":[0,0,0,1,1,1],

    "B":[1,1,0,0,0,1],

    "C":[3,3,4,4,5,5],

    "label":[
        "BENIGN",
        "BENIGN",
        "DOS",
        "DOS",
        "DOS",
        "BENIGN"
    ]

})

ce = CausalEffect()

effects = ce.calculate(df)

print(effects)

print()

print(
    ce.selected_features(effects)
)