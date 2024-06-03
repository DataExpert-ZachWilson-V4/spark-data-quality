from datetime import date, timedelta
from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

CURRENT_DATE = "2021-01-01"

def query_1(output_table_name: str, current_date: date) -> str:
    yesterday_date = current_date - timedelta(days=1)

    query = f"""
    WITH yesterday AS (
        SELECT
            *
        FROM {output_table_name}
        WHERE date = DATE('{yesterday_date}')
    ),
    -- Today's CTE from the source tables `web_events` and `devices`
    today AS (
        SELECT
            e.user_id,
            d.browser_type,
            CAST(date_trunc('day', e.event_time) AS DATE) AS event_date,
            COUNT(1) AS event_count
        FROM web_events e
        LEFT JOIN devices d ON e.device_id = d.device_id
        WHERE CAST(date_trunc('day', e.event_time) AS DATE) = DATE('{current_date}')
        GROUP BY
        e.user_id,
        d.browser_type,
        CAST(date_trunc('day', e.event_time) AS DATE)
    )

    SELECT
    COALESCE(t.user_id, y.user_id) AS user_id,
    COALESCE(t.browser_type, y.browser_type) AS browser_type,
    CASE WHEN
        -- When a user is present yesterday, concatenate the dates
        y.dates_active IS NOT NULL THEN ARRAY(t.event_date) || y.dates_active
        -- When a user is not present yesterday, create a new array
        ELSE ARRAY(t.event_date)
    END AS dates_active,
    DATE('{current_date}') AS date
    FROM today t
    FULL OUTER JOIN yesterday y ON t.user_id = y.user_id AND t.browser_type = y.browser_type
    """
    return query

def job_1(spark_session: SparkSession, output_table_name: str, current_date: date) -> Optional[DataFrame]:
  output_df = spark_session.table(output_table_name)
  output_df.createOrReplaceTempView(output_table_name)
  return spark_session.sql(query_1(output_table_name, current_date))

def main():
    output_table_name: str = "user_devices_cumulated"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_1")
        .getOrCreate()
    )
    output_df = job_1(spark_session, output_table_name, CURRENT_DATE)
    output_df.write.mode("overwrite").insertInto(output_table_name)
