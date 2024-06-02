from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_2(output_table_name: str) -> str:
    """
    Constructs a SQL query to merge data from two sets of host activities (yesterday and today) into a specified table.
    This operation uses complex SQL operations to update the dataset based on activity data.

    Args:
    output_table_name (str): The table to which the merged data is inserted or updated.

    Returns:
    str: A SQL merge query formatted to execute with Spark SQL capabilities.
    """
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
    """
    Executes a SQL query that handles merging data from different dates into a single table.
    This job is critical for maintaining up-to-date and comprehensive records of host activities.

    Args:
    spark_session (SparkSession): The active Spark session to execute the job.
    output_table_name (str): The table that will be updated with the new merged data.

    Returns:
    DataFrame: The result of the SQL query, primarily for verification or additional processing.
    """
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

