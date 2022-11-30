from Functions.read import Read
from Functions.preprocessing import dropna
filepath = "C:\\Users\\Poovendran\\PycharmProjects\\LEFT_JOIN_LL.csv"



if __name__ == '__main__':
    df = Read.pandas_csv(filepath)
    df = dropna(df,"column")
    print(df.head(12))

