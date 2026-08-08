import pandas as pd

from preprocessor import Preprocessor

data = {

    "Flow Duration": [
        100,
        120,
        110,
        5000,
        6000,
        7000
    ],

    "Flow Bytes/s": [
        20,
        22,
        21,
        500,
        600,
        800
    ],

    "Label": [
        "BENIGN",
        "BENIGN",
        "BENIGN",
        "DoS",
        "DoS",
        "DoS"
    ]
}

df = pd.DataFrame(data)

processor = Preprocessor()

result = processor.process(df)

print(result)