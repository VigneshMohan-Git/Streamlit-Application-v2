import concurrent.futures
import time
import pandas as pd
import requests
import os
import json


def ReadLDJson(df):
    import pandas as pd
    import json
    from pandas.io.json import json_normalize
    TempPath = "/usr/local/airflow/DeXFiles/Temp_folder/"
    """
    with open(filepath, 'r') as f:
        df = json.loads(f.read())
    """
    multiple_level_data = json_normalize(df, record_path=['Leads'], meta=['Attribute', 'Value', 'Fields'],
                                            errors='ignore')

    json_ = []
    for values in multiple_level_data.LeadPropertyList:
        for v in values:
            json_.append(v)

    with open(TempPath + 'app.json', 'w', encoding='utf-8') as f:
        json.dump(json_, f, ensure_ascii=False, indent=4)

    df = pd.read_json(TempPath + "app.json")
    # columns = df['Attribute'].unique()
    df = df.sort_values("Attribute").reset_index(drop=True)
    df['id'] = df.groupby((df["Attribute"] != df["Attribute"].shift(1)).cumsum()).cumcount() + 1
    df1 = pd.pivot_table(df, index='id', columns="Attribute", values="Value", fill_value='', aggfunc='first')
    return df1


def execute(df):
    df = ReadLDJson(df)
    return df