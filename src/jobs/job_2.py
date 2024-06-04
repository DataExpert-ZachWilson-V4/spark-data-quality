from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import udf

def query_2() -> str:
    """
    Constructs the SQL query to update the user_devices_cumulated table.

    Returns:
    - str: The SQL query string.
    """
    query = """
    INSERT INTO bhakti.user_devices_cumulated
    WITH
        yesterday AS (
            SELECT * FROM bhakti.user_devices_cumulated
            WHERE DATE = DATE('2021-01-02') -- earliest date in web_events table
        ),
        today AS (
            SELECT
                events.user_id AS user_id,
                devices.browser_type AS browser_type,
                CAST(date_trunc('day', event_time) AS DATE) AS event_date,
                COUNT(1) AS dates_active
            FROM bootcamp.web_events events
            LEFT JOIN bootcamp.devices devices
            ON events.device_id = devices.device_id
            WHERE date_trunc('day', event_time) = DATE('2021-01-03')
            GROUP BY 1, 2, 3
        )
    SELECT
        COALESCE(y.user_id, t.user_id) AS user_id,
        COALESCE(y.browser_type, t.browser_type) AS browser_type,
        CASE
            WHEN y.dates_active IS NOT NULL AND t.event_date IS NOT NULL THEN ARRAY[t.event_date] || y.dates_active
            WHEN y.dates_active IS NOT NULL THEN y.dates_active
            WHEN t.event_date IS NOT NULL THEN ARRAY[t.event_date]
        END AS dates_active,
        DATE('2021-01-03') AS date
    FROM yesterday y
    FULL OUTER JOIN today t
    ON y.user_id = t.user_id AND y.browser_type = t.browser_type
    """
    return query

def job_2(spark_session: SparkSession) -> Optional[DataFrame]:
    """
    Executes the job to update the user_devices_cumulated table.

    Args:
    - spark_session (SparkSession): The Spark session object.

    Returns:
    - Optional[DataFrame]: The resulting DataFrame after the update.
    """
    # Execute the SQL query
    output_df = spark_session.sql(query_2())
    
    # Create or replace the temporary view with the output DataFrame
    output_df.createOrReplaceTempView("user_devices_cumulated")
    
    return output_df

def main():
    # Initialize Spark session
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_2")
        .getOrCreate()
    )
    
    # Execute the job
    job_2(spark_session)
    
    # Stop the Spark session
    spark_session.stop()

if __name__ == "__main__":
    main()