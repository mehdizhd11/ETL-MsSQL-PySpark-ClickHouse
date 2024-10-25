
---

# ETL Project with PySpark, MSSQL, and ClickHouse

This project demonstrates a simple ETL (Extract, Transform, Load) pipeline using **PySpark** to extract data from a **Microsoft SQL Server** database, transform it, and load the results into **ClickHouse**. The dataset used in this example is historical stock data for Sony, with columns for date, high, low, open, close prices, and trading volume.

## Project Setup

### Prerequisites
- **Python 3.x**
- **PySpark**
- **clickhouse-connect**
- **Microsoft SQL Server JDBC Driver**
- **Microsoft SQL Server**
- **ClickHouse Database**

### Installation
Install required Python packages with:
```bash
pip install pyspark clickhouse-connect
```

### Configuration
Set up your MSSQL server connection parameters in the script, including:
- `ms_url`: SQL Server address and port
- `database_name`: Name of the MSSQL database
- `ms_user` and `ms_password`: MSSQL username and password
- `JDBC_config`: Path to the MSSQL JDBC driver

For ClickHouse, configure:
- `ch_host`, `ch_port`: ClickHouse server address and port
- `ch_database`, `ch_table`: ClickHouse database and table name

### Data Ingestion from CSV Files
To import large historical data sets, multiple CSV files have been archived and stored in an `archive` folder. These files are periodically ingested into MSSQL for further processing and transformation. Each file in the `archive` folder is loaded into the MSSQL `SONY_daily_data` table, preparing the data for extraction through PySpark.

### Data Extraction
The data is extracted from the MSSQL database using PySpark’s JDBC connection. The following query extracts all records from the `SONY_daily_data` table:
```sql
SELECT * FROM SONY_daily_data
```

### Data Transformation
Data transformations in this project include:
1. **Max High-Low Difference**: Identify the record with the highest difference between `HIGH` and `LOW` prices.
2. **Top 3 Trading Days by Volume (per year)**: Calculate and list the top three trading days each year based on volume, including the daily high and low prices and percentage change between open and close prices.

Transformations were done using both PySpark SQL and DataFrame APIs.

### Data Loading
The final transformed data is loaded into ClickHouse, with a table structure specified as:
```sql
CREATE TABLE IF NOT EXISTS SONY_DF (
    DATE String,
    HIGH Float64,
    LOW Float64,
    VOLUME Float64,
    PC Float64,
    ROW_NUM UInt64
) ENGINE = MergeTree()
ORDER BY DATE;
```
The data is loaded using ClickHouse’s `clickhouse_connect` library, converting the PySpark DataFrame to a pandas DataFrame for compatibility.

### Running on a Spark Cluster
This project can be scaled using a Spark cluster for parallel processing, enabling efficient handling of large datasets. By running the ETL pipeline on a Spark cluster, you can leverage Spark's distributed computing capabilities, distributing transformations and computations across multiple nodes for optimized performance.

To run on a cluster, submit the job using:
```bash
spark-submit --master spark://<cluster-master-ip>:7077 main.py
```

## Usage
Run the script as follows:
```bash
python3 main.py
```

## ETL Steps Overview
1. **Extract**: Pull data from MSSQL to PySpark DataFrame.
2. **Transform**: Compute maximum High-Low differences, top 3 trading days per year by volume, and daily percentage changes.
3. **Load**: Insert the transformed data into ClickHouse for efficient querying.

---