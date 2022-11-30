#import spark as spark_

# Generic Read
class Read:

    # ------------------------------------ PANDAS ------------------------------------
    # init method or constructor
    # def __init__(self):
    #     self.filepath = None
    #     self.filename = None

    # -- Read With Pandas CSV #header==Boolean #delimwhitespace == Boolean
    def pandas_csv(filepath):
        import pandas as pd
        df = pd.read_csv(filepath, low_memory=False,skip_blank_lines=True, na_values= "NaN")
        return df

    # -- Read With Pandas CSV
    def pandas_excel(self, filepath, sheetname):
        import pandas as pd
        df = pd.read_excel(filepath, sheet_name=sheetname, engine='openpyxl')
        return df

    # -- Read with pandas JSON
    def readjson(self,filename):
        import json
        with open(filename, 'r') as f:
            df = json.loads(f.read())
        return df

    def postRequest(self, data, url, my_headers):
        import requests
        import json
        json_object = json.dumps(data,indent=4)
        response = requests.post(url=url, headers=my_headers, data=json_object, proxies={'http':'','https':''})
        print(response)
        if response.status_code not in (200, 201):
            raise Exception
        return response.json()

    # -- RestCall
    def getRestCall(self,header,url):
        import requests
        print(url)
        response = requests.get(url, proxies={'http': '', 'https': ''},headers=header)
        if response.status_code not in (200, 201):
            raise Exception
        return response

    # -- Read with pandas JSON

    # ------------------------------------ SPARK ------------------------------------
    # -- Spark Read API Files
    # def spark_API(sourceAPI, format, sep):
    #     print("Reading API Files")
    #     spark = spark_.spark_connection()
    #     df = spark.read.load(sourceAPI, format=format, sep=sep, inferSchema="true", header="true")
    #     return df
    #
    # # -- Spark Read Json
    # def spark_json(filepath, filename):
    #     inputpath = filepath + filename
    #     spark = spark_.spark_connection()
    #     df = spark.read.json(inputpath)
    #     return df
    #
    # # -- Spark read CSV
    # def spark_csv(filepath, filename):
    #     inputpath = filepath + filename
    #     spark = spark_.spark_connection()
    #     # df = spark.read.load(inputpath, format="csv", sep=":", inferSchema="true",header="true")
    #     df = spark.read.format("csv").option("header", "true").load(inputpath)
    #     return df
    #
    # # -- Spark read EXCEL
    # def spark_excel(filepath, filename):
    #     inputpath = filepath + filename
    #     spark = spark_.spark_connection()
    #     # df = spark.read.load(inputpath, format="csv", sep=":", inferSchema="true",header="true")
    #     df = spark.read.format("excel").option("header", "true").load(inputpath)
    #     return df
    #
    # # -- Spark read text
    # def spark_text(filepath, filename):
    #     inputpath = filepath + filename
    #     spark = spark_.spark_connection()
    #     # df = spark.read.load(inputpath, format="csv", sep=":", inferSchema="true",header="true")
    #     df = spark.read.text(inputpath, lineSep=",")
    #     return df
    #
    #
    # # -- Read Using PyODBC
    def pyodbc_connection(self, server, database, port, username, password, query, Driver):
         import csv
         import os
         import pyodbc
         import shutil
         import pyarrow.csv as pv
         import pyarrow.parquet as pq
         import pandas.io.sql
         import time
         import pandas as pd

         server_ = server
         database = database
         username = username
         password = password

         print("Connection")
         connect = pyodbc.connect('DRIVER={'+ Driver +'};SERVER=' + server_ + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)

         print("Curcor Connect")
         cursor = connect.cursor()
         pyodbc_data = cursor.execute(query)
         df = pd.DataFrame(pyodbc_data)
         #connect.close()
         return df
    #
    #
    # # -- Read Using JDBC
    # def JDBC_Connection(server, database, port, username, password, query):
    #     server = server
    #     port = port
    #     database = database
    #     username = username
    #     password = password
    #
    #     # Spark Connection
    #     spark = spark_.spark_connection()
    #
    #     jdbcDF = spark.read.format("jdbc") \
    #         .option("url",
    #                 f"jdbc:sqlserver://" + server + ":" + port + ";databaseName=" + database + ";encrypt=false;trustServerCertificate=true;") \
    #         .option("query", query) \
    #         .option("user", username) \
    #         .option("password", password) \
    #         .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    #         .load()
    #
    #     return jdbcDF
    #
    # # -- Read AccessDB
    # def AccessDB(tablename):
    #     import pyodbc
    #
    #     conn = pyodbc.connect(
    #         r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=C:\Users\Ron\Desktop\Test\test_database.accdb;')
    #     cursor = conn.cursor()
    #     query = "SELECT * FROM" + tablename
    #     access_data = cursor.execute(query)
    #
    #     return access_data


    # Connection Oracle DB
    def oracleConnection(self, DBName, username, password, query):
        import cx_Oracle
        import sqlalchemy
        import pandas as pd

        # Use your database credentials below
        DATABASE = DBName
        SCHEMA = username
        PASSWORD = password

        # Generating connection string
        connstr = "oracle://{}:{}@{}".format(SCHEMA, PASSWORD, DATABASE)
        conn = sqlalchemy.create_engine(connstr)
        # print(connstr)
        # Fetching the data from the selected table using SQL query
        RawData = pd.read_sql_query(query, conn)
        RawData.head()

        return RawData
