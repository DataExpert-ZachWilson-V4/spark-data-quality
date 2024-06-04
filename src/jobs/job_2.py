from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_2(output_table_name: str) -> str:
    query = f"""
    WITH yesterday AS (
        SELECT
        *
        FROM
        mposada.user_devices_cumulated
        WHERE
        DATE = DATE('2023-01-01')
    ),

    today AS (
        SELECT
        w.user_id,
        d.browser_type,
        CAST(DATE_TRUNC('day', w.event_time) AS DATE) AS event_date
        FROM
        bootcamp.web_events w
        LEFT JOIN bootcamp.devices d ON d.device_id = w.device_id
        WHERE
        DATE_TRUNC('day', w.event_time) = DATE('2023-01-02')
        GROUP BY
        w.user_id,
        CAST(DATE_TRUNC('day', w.event_time) AS DATE),
        d.browser_type
    )

    SELECT
    COALESCE(y.user_id, t.user_id) AS user_id,
    COALESCE(y.browser_type, t.browser_type) AS browser_type,
    CASE
        WHEN y.dates_active IS NOT NULL THEN ARRAY_UNION(ARRAY(t.event_date), y.dates_active)
        ELSE ARRAY(t.event_date)
    END AS dates_active,
    DATE('2023-01-02') AS DATE
    FROM
    yesterday y
    FULL OUTER JOIN today t ON y.user_id = t.user_id
    """
    return query

def job_2(spark_session: SparkSession, output_table_name: str) -> Optional[DataFrame]:
  output_df = spark_session.table(output_table_name)
  output_df.createOrReplaceTempView(output_table_name)
  return spark_session.sql(query_2(output_table_name))

def main():
    output_table_name: str = "mposada.user_devices_cumulated"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_2")
        .getOrCreate()
    )
    output_df = job_2(spark_session, output_table_name)
    output_df.write.mode("overwrite").insertInto(output_table_name)
