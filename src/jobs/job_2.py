from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_2(output_table_name: str) -> str:
    query = f"""
    WITH yesterday AS (
        SELECT 
            host,
            host_activity_datelist,
            date
        FROM 
            {output_table_name}
        WHERE 
            date = DATE('2023-01-01')  -- Targeting the data of January 1, 2023.
    ),
    today AS (
        SELECT
            host,
            CAST(date_trunc('day', event_time) AS DATE) AS event_date,
            COUNT(1) AS cnt
        FROM 
            bootcamp.web_events
        WHERE 
            date_trunc('day', event_time) = DATE('2023-01-02')  -- Filtering events for January 2, 2023.
        GROUP BY 
            host,
            CAST(date_trunc('day', event_time) AS DATE)
    )
    INSERT INTO {output_table_name}
    SELECT 
        COALESCE(y.host, t.host) AS host,
        CASE 
            WHEN y.host_activity_datelist IS NOT NULL THEN
                ARRAY_CONCAT(ARRAY[t.event_date], y.host_activity_datelist)
            ELSE
                ARRAY[t.event_date]
        END AS host_activity_datelist,
        COALESCE(DATE_ADD(y.date, 1), t.event_date) AS date
    FROM 
        yesterday y
    FULL OUTER JOIN 
        today t 
    ON 
        t.host = y.host
    """
    return query

def job_2(spark_session: SparkSession, output_table_name: str) -> Optional[DataFrame]:
    output_df = spark_session.table(output_table_name)
    output_df.createOrReplaceTempView(output_table_name)
    return spark_session.sql(query_2(output_table_name))

def main():
    output_table_name: str = "videet.hosts_cumulated"  # Set your Delta table name here
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("Update Host Cumulated")
        .getOrCreate()
    )
    output_df = job_2(spark_session, output_table_name)
    output_df.write.mode("overwrite").insertInto(output_table_name)

# Note: Adjust the name of the output table and database/schema as per your actual setup.
