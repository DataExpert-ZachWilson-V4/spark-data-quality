from typing import Optional
from datetime import date, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

CURRENT_DATE = "2021-01-02"
def query_2(output_table_name: str, current_date: date) -> str:
    yesterday_date = current_date - timedelta(days=1)
    query = f"""
    WITH yesterday AS (
    SELECT
        *
    FROM hosts_cumulated
    WHERE date = DATE('{yesterday_date}')
    ),
    -- Today's CTE from the source table `web_events`
    today AS (
    SELECT
        host,
        CAST(date_trunc('day', event_time) AS DATE) AS event_date,
        COUNT(1)
    FROM
        web_events
    WHERE CAST(date_trunc('day', event_time) AS DATE) = DATE('{current_date}')
    GROUP BY
        host, CAST(date_trunc('day', event_time) AS DATE)
    )
    SELECT
    COALESCE(t.host, y.host) AS host,
    CASE WHEN
        -- When the host is in yesterday's table, concatenate today's host activity date
        y.host_activity_datelist IS NOT NULL THEN ARRAY(t.event_date) || y.host_activity_datelist
        -- When the host is not in yesterday's table, return today's host activity date
        ELSE ARRAY(t.event_date)
    END AS host_activity_datelist,
    DATE('{current_date}') AS date
    FROM today t
    FULL OUTER JOIN yesterday y ON t.host = y.host
    """
    return query

def job_2(spark_session: SparkSession, output_table_name: str, current_date: date) -> Optional[DataFrame]:
  output_df = spark_session.table(output_table_name)
  output_df.createOrReplaceTempView(output_table_name)
  return spark_session.sql(query_2(output_table_name, current_date))

def main():
    output_table_name: str = "user_devices_cumulated_binary"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_2")
        .getOrCreate()
    )
    output_df = job_2(spark_session, output_table_name, CURRENT_DATE)
    output_df.write.mode("overwrite").insertInto(output_table_name)
