from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


def query_2(output_table_name: str) -> str:
    query = f"""
    WITH yesterday AS (
        SELECT *
        FROM user_devices_cumulated
        WHERE date = DATE('2022-12-31')
    ),
    today AS (
        SELECT 
            we.user_id,
            dev.browser_type,
            CAST(date_trunc('day', event_time) AS DATE) AS event_date,
            COUNT(1)
        FROM web_events we
            LEFT JOIN devices dev ON we.device_id = dev.device_id
        WHERE CAST(date_trunc('day', event_time) AS DATE) = DATE('2023-01-01')
        GROUP BY we.user_id, dev.browser_type, CAST(date_trunc('day', event_time) AS DATE)
    )
    SELECT DISTINCT
        COALESCE(y.user_id, t.user_id) AS user_id,
        COALESCE(y.browser_type, t.browser_type) AS browser_type,
        CASE
            WHEN y.dates_active IS NOT NULL THEN concat(ARRAY(t.event_date), y.dates_active)
            ELSE ARRAY(t.event_date)
        END AS dates_active,
        DATE('2023-01-01') AS date
    FROM yesterday y
        FULL OUTER JOIN today t ON y.user_id = t.user_id
    """
    return query


def job_2(spark_session: SparkSession, output_table_name: str) -> Optional[DataFrame]:
    output_df = spark_session.table(output_table_name)
    output_df.createOrReplaceTempView(output_table_name)
    return spark_session.sql(query_2(output_table_name))


def main():
    output_table_name: str = "rudnickipm91007.user_devices_cumulated"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_2")
        .getOrCreate()
    )
    output_df = job_2(spark_session, output_table_name)
    output_df.write.mode("overwrite").insertInto(output_table_name)
