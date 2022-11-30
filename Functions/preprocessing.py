# -- Data Preprocessing


def isnull_(df, value):
    df = df.isnull(value)
    print("Replaced Isnull value:", value)
    return df

# -- Dropna
def dropna(df, how):
    global new_data
    if how=="column":
        new_data = df.dropna(axis=0, how='all')
    elif how=="row":
        new_data = df.dropna(axis=1, how='all')
    # comparing sizes of data frames
    print("Old data frame length:", len(df), "New data frame length:",len(new_data), "Number of rows with at least 1 NA value: ", (len(df) - len(new_data)))
    return new_data

