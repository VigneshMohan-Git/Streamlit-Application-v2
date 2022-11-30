

class JSON_:

    def pandas_json_A(self, filepath):
        import pandas as pd
        import json
        from pandas import json_normalize
        with open(filepath) as f:
            data = json.load(f)
            df = json_normalize(data, record_path=['results'], meta=['fieldMask'], errors='ignore')
        return df


    def pandas_json_B(self, filepath):
        import pandas as pd
        df = pd.read_json(filepath, orient='index')
        return df


    def pandas_json_C(self, filepath):
        import pandas as pd
        df = pd.read_json(filepath)
        return df


    def pandas_json_D(self, filepath):
        import pandas as pd
        import json
        from pandas import json_normalize
        data = []
        with open(filepath, errors='ignore') as f:
            for line in f:
                data.append(json.loads(line))
                df = json_normalize(data)
        return df


    def ReadLDJson(self, filepath):
        import pandas as pd
        import json
        TempPath = "/usr/local/airflow/sql_files/Input/json/"
        with open(filepath, 'r') as f:
            df = json.loads(f.read())

        multiple_level_data = pd.json_normalize(df, record_path=['Leads'], meta=['Attribute', 'Value', 'Fields'],
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