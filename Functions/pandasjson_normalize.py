
def pandas_normalize(df):
    import pandas as pd
    dataframe=pd.json_normalize(df, max_level=1)
    return dataframe

def execute(df):
    df = pandas_normalize(df)
    return df