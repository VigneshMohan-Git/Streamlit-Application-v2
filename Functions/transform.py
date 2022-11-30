
# -- Logical Function
class logical:

    def __init__(self):
        self.df = None

    # -- Rename Columns
    def rename(self,df, metadata):
        df1 = df.rename(columns=metadata, inplace=False)
        return df1

    # -- drop a field
    def drop(df, dropcolumn):
        df = df.drop([dropcolumn],axis=1, inplace=False)
        return df

    # -- Tranform with function
    def funtransform(self):
        pass


# -- DateTime
class datetime:

    def __init__(self):
        self.df = None

    #Strip date
    def stripdate(df, column):
        from datetime import datetime
        import pandas as pd
        df['Date'] = pd.to_datetime(df[column]).apply(lambda x: x.date())
        return df


    #strip time
    def striptime(df, column):
        from datetime import datetime

        df['Time'] = df[column].apply(lambda x: x.date().strftime('%H:%M:%S'))

        return df

    #Date time convert
    def datetimeconvert(df, column):
        from datetime import datetime
        # df['datetime'] = df[column].astype(int)
        df['datetime'] = df[column].apply(lambda x: x.datetime.utcfromtimestamp().strftime('%Y-%m-%d %H:%M:%S'))
        return df

    def applydateformat(df,column,format):
       import pandas as pd
       df[column] = df[column].dt.strftime(format)
       return df


    # Date Difference
    def diff(t_a, t_b):
        from dateutil.relativedelta import relativedelta
        t_diff = relativedelta(t_b, t_a)  # later/end time comes first!
        return '{h}h {m}m {s}s'.format(h=t_diff.hours, m=t_diff.minutes, s=t_diff.seconds)


# -- Generic Functions
class generic:

    def __init__(self):
        self.df = None

    # -- Lookup
    def lookup(df, column, value):
        newdf = df[df[column].isin([value])]
        return newdf

    # -- GroupBy
    def groupby(df, column):
        newdf = df.groupby(by=[column], axis=0, level=None, as_index=True, sort=True, dropna=True)
        return newdf

    # -- Replace String with another string
    def replace(df, column, oldstring, replacestring):
        df[column] = df[column].apply(lambda x: x.replace(oldstring, replacestring))
        return df

    # -- To_Upper
    def upper(df, column):
        df = df[column].apply(lambda x: x.upper(), inplace=False)
        return df

    # -- To_Lower
    def lower(df, column):
        df = df[column].apply(lambda x: x.lower(), inplace=False)
        return df

    # -- Trim String
    def trimstring(df, column, params):
        df = df[column].apply(lambda x: x.strip(params), inplace = False)
        return df

def pandas_join(df1,df2,left,right,joinType):
    import pandas as pd
    df_join =pd.merge(df1,df2,how=joinType,left_on=left,right_on=right)
    return df_join

def pandas_select(df,columns):
    print(columns)
    return df[columns]

def addColumn(df,column,value):
     df[column]=value
     return df


def concatColumns(df, column, values):
    concat=""
    for value in values:
        concat =concat+ df[value].map(str)
    df[column]=concat
    return df

def applyAggregartion(df,data):
    return df.aggregate(data)

def limitedRecords(df,data):
    return df[:data]

def GroupBy(df, columns,selectColumns,aggregration,newColumn):
    import pandas as pd
    if aggregration.lower()=="count":
       output=df.groupby([columns])[selectColumns].count().rename(newColumn).reset_index()
       print("Applied GroupBy: Count")
    elif aggregration.lower()=="mean":
        output=df.groupby([columns])[selectColumns].mean().rename(newColumn).reset_index()
        print("Applied GroupBy: Mean")
    elif aggregration.lower()=="sum":
        output=df.groupby([columns])[selectColumns].sum().rename(newColumn).reset_index()
        print("Applied GroupBy: Sum")
    elif aggregration.lower() == "first":
        output=df.groupby([columns]).first().reset_index()
        print("Applied GroupBy: First")
    elif aggregration.lower() == "nth":
        output =df.groupby([columns]).nth(0).reset_index()
        print("Applied GroupBy: Nth")
    elif aggregration.lower() == "bysum":
        output =df.groupby([columns]).sum().reset_index()
        print("Applied GroupBy: bysum")
    return  pd.merge(output,df, how='left')

def operatorFiltering(df,data):
    return df.query(data)

def isInFiltering(df,data):
    return df.isin(data)

def startWith(df,data):
    for key,value in data.items():
        df = df[df[key].str.startwith(value)]
    return df


def contains(df,data):
    for key,value in data.items():
        df = df[df[key].str.contains(value)]
    return df

def columnname(df, value):
    df = df.loc[:, df.columns.isin(value)]
    return df

def dropna_(df, types):
    if types == "row":
        df2=df.dropna(axis = 0, how = 'all')
        print("Removed Row: NaNvalues")
    elif types == "columns":
        df2=df.dropna(axis = 1, how = 'all')
        print("Removed Column: NaNvalues")
    else:
        print("Error ...")
    return df2

def rm_duplicates(df, keep_):
    if keep_ == "True":
        df = df.drop_duplicates(keep='last', inplace=False)
        print("Removed Duplication")
    else:
        print("No Duplication Applied")
    return df


def ColCompare(df, key1, key2, ops):
    if ops == "eq":
        return df[df[key1] == df[key2]]
    elif ops == "gt":
        return df[df[key1] >= df[key2]]
    elif ops == "le":
        return df[df[key1] <= df[key2]]
    else:
        print("Error", ops)


# Math Operations
# Pandas
import pandas as pd

def add_(df, new_column_name, val1, val2):
    df[new_column_name] = df[val1] + df[val2]
    return df


def sub_(df, new_column_name, val1, val2):
    df[new_column_name] = df[val1] - df[val2]
    return df


def mul_(df, new_column_name, val1, val2):
    df[new_column_name] = df[val1] * df[val2]
    return df

def div_(df, new_column_name, val1, val2):
    df[new_column_name] = df[val1].astype(float) / df[val2].astype(float)
    return df

def gt_(df, new_column_name, val1, val2):
    df[new_column_name] = df[val1] > df[val2]
    return df

def le_(df, new_column_name, val1, val2):
    df[new_column_name] = df[val1] < df[val2]
    return df

def eq_(df, new_column_name, val1, val2):
    df[new_column_name] = df[val1] == df[val2]
    return df

def mathops_sub(df, new_column_name, val1, val2, ops):
    if ops.lower() == "add":
        return add_(df ,new_column_name, val1, val2)
    elif ops.lower() == "mul":
        return mul_(df ,new_column_name, val1, val2)
    elif ops.lower() == "sub":
        return sub_(df ,new_column_name, val1, val2)
    elif ops.lower() == "div":
        return div_(df ,new_column_name, val1, val2)
    elif ops.lower() == "gt":
        return gt_(df, new_column_name, val1, val2)
    elif ops.lower() == "le":
        return le_(df, new_column_name, val1, val2)
    elif ops.lower() == "eq":
        return eq_(df, new_column_name, val1, val2)


def assignColValue(df, newcolumn, col1, col2, assign1_, assign2_, ops):
    if ops == "gt":
        df = gt_(df, "BooleanValue", col1, col2)
        df[newcolumn] = df['BooleanValue'].apply(lambda x: assign1_ if x == True else assign2_)
        return df.drop(["BooleanValue"],axis=1,inplace=False)



def stripspaces(df, colname_, option_):
    if option_== "all":
        df[colname_] = df[colname_].str.replace(' ', '')
    else:
        print("Faied to remove spaces")
    return df


def toNumeric(df, value):
    import numpy as np
    colName_ = value['colName']
    for col in colName_:
        df[[col]] = df[[col]].apply(pd.to_numeric, errors='coerce')
        df[col] = df[col].replace(np.nan, 0).astype(np.int64)
        print("Converted:",col)
    return df


def pandas_dtype(df, value):
    df=df.astype(value)
    print("Columns:",value)
    return df


def pandas_removeInf(df, value):
    import numpy as np
    df=df.replace([np.inf, -np.inf], 0, inplace=False)
    print("Replced Inf with '0':", value)
    return df