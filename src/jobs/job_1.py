from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_1(output_table_name: str) -> str:
    query = f"""
    WITH yesterday AS (
        SELECT *
        FROM user_devices_cumulated
        WHERE date = DATE '2021-02-04'  -- Last updated date
    ),
    today AS (
        SELECT
            we.user_id,
            d.browser_type,
            ARRAY_AGG(DISTINCT DATE_TRUNC('day', we.event_time)) AS dates_active
        FROM
            bootcamp.devices d
            JOIN bootcamp.web_events we ON d.device_id = we.device_id
        WHERE DATE_TRUNC('day', we.event_time) = DATE '2021-02-05'  -- The next day after the last update
        GROUP BY
            we.user_id, d.browser_type
    )
    SELECT
        COALESCE(yesterday.user_id, today.user_id) AS user_id,
        COALESCE(yesterday.browser_type, today.browser_type) AS browser_type,
        array_distinct(concat(COALESCE(yesterday.dates_active, ARRAY[]), COALESCE(today.dates_active, ARRAY[]))) AS dates_active,
        DATE('2021-02-05') AS date  -- Record the date of update to track when the last update occurred.
    FROM
        yesterday
        FULL OUTER JOIN today
    ON yesterday.user_id = today.user_id
        AND yesterday.browser_type = today.browser_type
    """
    return query

def job_1(spark_session: SparkSession, output_table_name: str) -> Optional[DataFrame]:
  output_df = spark_session.table(output_table_name)
  output_df.createOrReplaceTempView(output_table_name)
  return spark_session.sql(query_1(output_table_name))

def main():
    output_table_name: str = "jlcharbneau.user_devices_cumulated"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_1")
        .getOrCreate()
    )
    output_df = job_1(spark_session, output_table_name)
    output_df.write.mode("overwrite").insertInto(output_table_name)
