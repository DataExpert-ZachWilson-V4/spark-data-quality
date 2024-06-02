from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_2(output_table_name: str) -> str:
    
    query = f"""
    WITH yesterday AS ( 
        SELECT host, host_activity_datelist, date
        FROM {output_table_name}
        WHERE date = DATE('2023-01-01')
    ),
    today AS (
        SELECT
            host,
            CAST(date_trunc('day', event_time) AS DATE) AS event_date,
            COUNT(1) AS cnt
        FROM bootcamp.web_events
        WHERE date_trunc('day', event_time) = DATE('2023-01-02')
        GROUP BY host, CAST(date_trunc('day', event_time) AS DATE)
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
    FROM yesterday y
    FULL OUTER JOIN today t ON t.host = y.host
    """
    return query

def job_2(spark_session: SparkSession, output_table_name: str) -> Optional[DataFrame]:
    output_df = spark_session.sql(query_2(output_table_name))
    output_df.createOrReplaceTempView(output_table_name)
    return output_df

def main():
    """
    Main function to initialize the Spark session and execute the data merging job.
    This is the entry point when the script is executed, setting up the necessary configurations.
    """
    output_table_name: str = "videet.hosts_cumulated"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("Update Host Cumulated")
        .getOrCreate()
    )
    output_df = job_2(spark_session, output_table_name)
    output_df.write.mode("overwrite").insertInto(output_table_name)

