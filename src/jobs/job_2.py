from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from datetime import date, timedelta


def query_2(output_table_name: str, current_date: date) -> str:

    yesterday_date = current_date - timedelta(days=1)

    return f"""
    WITH yesterday_user_device_cumulated AS (
        SELECT *
        FROM {output_table_name}
        WHERE date = DATE('{yesterday_date}')
    ),
    today_events_cte AS (
        SELECT 
            user_id,
            device_id,
            CAST(date_trunc('day', event_time) AS DATE) AS event_date 
        FROM bootcamp.web_events
        WHERE CAST(date_trunc('day', event_time) AS DATE) = DATE('{current_date}')
        GROUP BY user_id, device_id, CAST(date_trunc('day', event_time) AS DATE)
    ),
    today_user_device_cumulated AS (
        SELECT
            ud.user_id AS user_id,
            dev.browser_type AS browser_type,
            ud.event_date AS event_date
        FROM today_events_cte AS ud
        LEFT OUTER JOIN bootcamp.devices AS dev 
        ON ud.device_id = dev.device_id
        GROUP BY 1, 2, 3
    )
    SELECT
        COALESCE(yud.user_id, tud.user_id) AS user_id,
        COALESCE(yud.browser_type, tud.browser_type) AS browser_type,
        CASE
            WHEN yud.dates_active IS NOT NULL THEN ARRAY[tud.event_date] || yud.dates_active
            ELSE ARRAY[tud.event_date]
        END AS dates_active,
        COALESCE(tud.event_date, date_add('day',1,yud.date)) AS DATE
    FROM yesterday_user_device_cumulated AS yud 
    FULL OUTER JOIN today_user_device_cumulated AS tud
    ON yud.user_id = tud.user_id
    """


def job_2(
    spark_session: SparkSession, output_table_name: str, current_date: date
) -> Optional[DataFrame]:
    output_df = spark_session.table(output_table_name)
    output_df.createOrReplaceTempView(output_table_name)
    return spark_session.sql(query_2(output_table_name, current_date))


def main():
    output_table_name: str = "user_devices_cumulated"
    current_date = "2021-03-20"
    spark_session: SparkSession = (
        SparkSession.builder.master("local").appName("job_2").getOrCreate()
    )
    output_df = job_2(spark_session, output_table_name, current_date)
    output_df.write.mode("overwrite").insertInto(output_table_name)
