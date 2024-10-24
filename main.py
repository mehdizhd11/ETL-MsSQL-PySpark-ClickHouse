from pyspark.sql import SparkSession


# Extractor from MsSQL
JDBC_config = "sqljdbc_12.8/enu/jars/mssql-jdbc-12.8.1.jre11.jar"
ms_url = "127.0.0.1:1433"
database_name = "Mssql-ETL"
ms_user, ms_password = "sa", "1qaz!QAZ"
table_name = "SONY_daily_data"

spark = SparkSession.builder \
    .appName("MSSQL Data Extraction") \
    .config("spark.jars", JDBC_config) \
    .getOrCreate()

jdbc_url = f"jdbc:sqlserver://{ms_url};databaseName={database_name};encrypt=false;trustServerCertificate=true"
connection_properties = {
    "user": ms_user,
    "password": ms_password,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

query = f"(SELECT * FROM {table_name}) AS temp"

df = spark.read.jdbc(url=jdbc_url, table=query, properties=connection_properties)

# Transform to pandas DataFrame
pd_df = df.toPandas()
print(pd_df)
