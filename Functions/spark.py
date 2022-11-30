# -- Spark Connection
def spark_connection_sql():
    from pyspark import SparkContext, SparkConf, SQLContext
    master = "local"
    conf = SparkConf() \
        .setAppName("stepName") \
        .setMaster(master) \
        .set("spark.driver.extraClassPath", "/usr/local/airflow/mssql-jdbc-7.2.1.jre8.jar")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    spark = sqlContext.sparkSession
    return spark


# -- Saprk Connection using HIVE
def spark_connection_hive():
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    return spark


# -- Spark connection
def spark_connection():
    from pyspark.sql import SparkSession

    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    return spark


def spark_close(spark):
    spark.close()




# -- Spark Create Dataframe
def sparkdataframe(panada_df):
    from pyspark.sql import SQLContext
    from pyspark import SparkConf, SparkContext
    sc = SparkContext.getOrCreate()
    sqlContext = SQLContext(sc)
    spark_dff = sqlContext.createDataFrame(panada_df)
    return spark_dff


# -- Spark Rename
def sparkrename(df, exccolumn, newcolumn):
    df_renamed = df.withColumnRenamed(exccolumn, newcolumn)
    return df_renamed

# -- Spark Limit Records
def sparklimitrecords(df,N):
    df_limit = df.limit(N)
    return df_limit


# -- Spark addcolmn
def sparkaddcolumn(df, colname, values):
    df_addcolumn = df.withColumn(colname, list(values))
    return df_addcolumn

# explicit function
def unionAll(dfs):
    import functools
    return functools.reduce(lambda df1, df2: df1.union(df2.select(df1.columns)), dfs)


# -- Spark Groupby
def sparkgroupby(dataframe, column_name_group,selectColumns,aggregration, newcolumn):
    global output
    if aggregration.lower() == "count":
        output = dataframe.groupBy(column_name_group).count(selectColumns)
    elif aggregration.lower() == "mean":
        output = dataframe.groupBy(column_name_group).mean(selectColumns)
    elif aggregration.lower() == "sum":
        output = dataframe.groupBy(column_name_group).sum(selectColumns)

    li = [dataframe, output]
    return unionAll(li)


def array_contains(df, column, values):
    from pyspark.sql.functions import array_contains
    return df.filter(array_contains(df[column], values))

def isinfilter(df, column, value):
    return df.filter(df[column].isin(value))