#import spark as spark_
import pandas as pd

#Generic Write
class Write:
    # init method or constructor
    # def __init__(self):
    #     self.df = None
    #     self.outputpath = None
    #     self.filename = None

    # ------------------------------------ PANDAS ------------------------------------

    # -- Write using CSV
    def pandas_csv(self,df, outputpath, filename):
        outputfile = outputpath + filename

        dataframe = pd.DataFrame(df)
        dataframe.to_csv(outputfile, index=False)
        return print("File Saved as CSV", outputfile)

    def writejson(self,filename,outputpath,data):
        filePath=outputpath+filename
        import json
        with open(filePath, 'w') as f:
            json.dump(data, f)
        print("File Saved as json", filePath)

    # -- Write using Excel
    def pandas_excel(df, outputpath, filename):
        outputfile = outputpath + filename + ".xlsx"

        dataframe = pd.DataFrame(df)
        dataframe.to_excel(outputfile, index=False)
        return print("File Saved as Excel", outputfile)

    # -- Write to JSON
    def pandas_json(df, outputpath, filename):
        outputfile = outputpath + filename

        dataframe = pd.DataFrame(df)
        dataframe.to_json(outputfile, index=False)
        return print("File Saved as JSON", outputfile)

    # ------------------------------------ SPARK ------------------------------------
    # -- Write Function CSV & PARQUET
    # def spark_cp(df, outputpath, filename):
    #     spark = spark_.spark_connection()
    #     outputfile = outputpath + filename
    #
    #     spark.coalesce(1).write.format("csv").option("header", "true").option("quoteAll", "true").mode('overwrite').save(outputfile, index=False, index_label=False)
    #     #spark_close()
    #     return print("File Stored:", outputfile)
    #
    #
    #     # -- Write Function TEXT
    # def spark_txt(df, outputpath, filename):
    #     spark = spark_.spark_connection()
    #     outputfile = outputpath + filename
    #
    #     spark.coalesce(1).write.format("txt").option("header", "true").option("quoteAll", "true").mode(
    #         'overwrite').save(outputfile, index=False, index_label=False)
    #     # spark_close()
    #     return print("File Stored:", outputfile)
    #
    # # -- Write to SQLDB
    # def write_sqlDB(df, database, username, password, tablename):
    #     # creating sparkdataframe
    #     df = spark_.sparkdataframe(df)
    #
    #     # write the dataframe into a sql table
    #     df.write.mode("overwrite") \
    #         .format("jdbc") \
    #         .option("url", f"jdbc:sqlserver://localhost:1433;databaseName={database};") \
    #         .option("dbtable", tablename) \
    #         .option("user", username) \
    #         .option("password", password) \
    #         .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    #         .save()
    #
    #     return print("File Saved")
    #
    # # -- Write to API
    # def write_api(self):
    #     pass




