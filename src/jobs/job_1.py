from datetime import datetime, timedelta
from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


def query_1(
      events_input: str,
      devices_input: str,
      output_table_name: str,
      current_date: str
    ) -> str:

    # get previous dt
    dt = datetime.strptime(current_date, "%Y-%m-%d")
    prev_dt = dt - timedelta(days = 1)
    last_date = prev_dt.strftime("%Y-%m-%d")

    query = f"""
    WITH yesterday_history AS (
        SELECT *
        FROM {output_table_name}
        WHERE DATE = DATE('{last_date}')

    ), today_history AS (
        SELECT
            user_id,
            browser_type,
            DATE('{current_date}') AS todays_date,
            -- activity logged dates
            ARRAY_AGG(DATE(event_time)) AS event_date
        FROM {events_input} web
        LEFT JOIN {devices_input} d
            ON web.device_id = d.device_id
        WHERE DATE(web.event_time) = DATE('{current_date}')
        GROUP BY 1, 2, 3

    )
    SELECT
        COALESCE(yh.user_id, th.user_id) AS user_id,
        COALESCE(yh.browser_type, th.browser_type) AS browser_type,
        CASE
            WHEN yh.user_id IS NULL AND th.todays_date IS NOT NULL
                THEN ARRAY((th.todays_date))
            WHEN yh.dates_active IS NOT NULL AND th.todays_date IS NOT NULL
                THEN ARRAY((th.todays_date)) || yh.dates_active
            WHEN yh.dates_active IS NOT NULL AND th.todays_date IS NULL
                THEN yh.dates_active
        ELSE NULL END AS dates_active,
        COALESCE(th.todays_date, yh.date + INTERVAL '1' DAY) AS date
    FROM yesterday_history yh
    FULL OUTER JOIN today_history th
    ON th.user_id = yh.user_id
    GROUP BY 1, 2, 3, 4
    """
    return query

def job_1(
      spark_session: SparkSession,
      events_input_table_name: str,
      devices_input_table_name: str,
      output_table_name: str,
      current_date: str
    ) -> Optional[DataFrame]:
  output_df = spark_session.table(output_table_name)
  output_df.createOrReplaceTempView(output_table_name)
  return spark_session.sql(
     query_1(events_input_table_name, devices_input_table_name, output_table_name, current_date)
    )

def main():
    events_input_table_name = "web_events"
    devices_input_table_name = "user_devices"
    output_table_name: str = "user_devices_cumulated"
    current_date: str = '2023-08-14'
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_1")
        .getOrCreate()
    )
    output_df = job_1(spark_session, events_input_table_name, devices_input_table_name, output_table_name, current_date)
    output_df.write.mode("overwrite").insertInto(output_table_name)
