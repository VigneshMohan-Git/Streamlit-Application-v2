
def json_normalize(df):
    import pandas as pd
    import json
    from pandas.io.json import json_normalize

    json_ = []
    for k in df:
        for key, value in k.items():
            json_.append(value)
    data = json_normalize(json_)
    return data

def execute(df):
    df = json_normalize(df)
    return df