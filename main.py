import clickhouse_connect
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, max, year, row_number


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
df.cache()

# Transform to pandas DataFrame
pd_df = df.toPandas()

# Max High Low difference
print("Max High Low difference")
df.createOrReplaceTempView(f"{table_name}_view")
max_high_low_df = spark.sql(f"""
    SELECT DATE, HIGH - LOW AS HIGH_LOW 
    FROM {table_name}_view 
    WHERE HIGH - LOW = (SELECT MAX(HIGH - LOW) FROM {table_name}_view)
""")
max_high_low_df.show()

# DataFrame API Query
df_with_diff = df.withColumn("HIGH_LOW", col("HIGH") - col("LOW"))
max_high_low_value = df_with_diff.agg(max("HIGH_LOW")).collect()[0][0]
max_high_low_df = df_with_diff.filter(col("HIGH_LOW") == max_high_low_value)
max_high_low_df.select("DATE", "HIGH_LOW").show()

# Identify the top 3 trading days (by volume) each year,
# along with the corresponding High, Low, and the percentage change between Open and Close prices for those days.
print("Top trading days in year")
top_days_df = spark.sql(f"""
    WITH DateGroup AS (
        SELECT DATE, HIGH, LOW, VOLUME, (CLOSE - OPEN) / CLOSE * 100 AS PC,
               ROW_NUMBER() OVER (PARTITION BY YEAR(DATE) ORDER BY VOLUME DESC) AS RowNum
        FROM {table_name}_view
    )
    SELECT * 
    FROM DateGroup 
    WHERE RowNum < 4 
    ORDER BY YEAR(DATE), RowNum
""")
top_days_df.show()

# DataFrame API Query
window_spec = Window.partitionBy(year("DATE")).orderBy(col("VOLUME").desc())
df_with_row_num = df.withColumn("ROW_NUM", row_number().over(window_spec))
top_3_per_year = df_with_row_num.filter(col("ROW_NUM") <= 3)
final_top = top_3_per_year.withColumn("PC", (col("CLOSE") - col("OPEN")) / col("CLOSE") * 100)
final_top = final_top.select("DATE", "HIGH", "LOW", "VOLUME", "PC", "ROW_NUM")
final_top.show()

# Load DataFrame to ClickHouse with pandas DataFrame
ch_host = "127.0.0.1"
ch_port = 8123
ch_database = "default"
ch_table = "SONY_DF"
client = clickhouse_connect.get_client(host=ch_host, port=ch_port, database=ch_database)

create_table_query = f"""
CREATE TABLE IF NOT EXISTS {ch_table} (
    DATE String,
    HIGH Float64,
    LOW Float64,
    VOLUME Float64,
    PC Float64,
    ROW_NUM UInt64
) ENGINE = MergeTree()
ORDER BY DATE;
"""
client.query(create_table_query)

pandas_df = final_top.toPandas()
client.insert_df(table=ch_table, df=pandas_df)
