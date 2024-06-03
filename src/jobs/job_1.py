from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from datetime import datetime, timedelta

def query_1(events_input_table_name: str, devices_input_table_name: str, output_table_name: str, current_date: str) -> str:
    try:
        dt = datetime.strptime(current_date, "%Y-%m-%d")
        previous_date = dt - timedelta(days=1)
        previous_date_str = previous_date.strftime("%Y-%m-%d")
    except ValueError as e:
        print(f"Error parsing date: {e}")

    query = f"""
    WITH yesterday AS (
        SELECT *
        FROM {output_table_name}
        WHERE date = DATE('{previous_date_str}')
    ),
    today AS (
        SELECT
            we.user_id,
            d.browser_type,
            DATE('{current_date}') as todays_date,
            ARRAY_AGG(DISTINCT DATE_TRUNC('day', we.event_time)) AS dates_active
        FROM
            {devices_input_table_name} d
            JOIN {events_input_table_name} we ON d.device_id = we.device_id
        WHERE DATE(we.event_time) = DATE({current_date})
        GROUP BY we.user_id, d.browser_type
    )
    SELECT
        -- Use COALESCE to handle any null values by choosing data from 'today' if available, otherwise from 'yesterday'.
        COALESCE(yesterday.user_id, today.user_id) AS user_id,
        COALESCE(yesterday.browser_type, today.browser_type) AS browser_type,

        -- Merge and deduplicate date arrays from 'yesterday' and 'today' using the concat function and array_distinct.
        -- This ensures all unique activity dates are captured in the cumulative table.
        array_distinct(concat(COALESCE(yesterday.dates_active, ARRAY[]), COALESCE(today.dates_active, ARRAY[]))) AS dates_active,

        COALESCE(today.todays_date, yesterday.date + INTERVAL '1' DAY) AS date  -- Record the date of update to track when the last update occurred.
    FROM
        yesterday
        FULL OUTER JOIN today
        ON yesterday.user_id = today.user_id
        AND yesterday.browser_type = today.browser_type
    
    """
    return query

def job_1(spark_session: SparkSession,  events_input_table_name: str, devices_input_table_name: str, output_table_name: str, current_date: str) -> Optional[DataFrame]:

  output_df = spark_session.table(output_table_name)
  output_df.createOrReplaceTempView(output_table_name)
  return spark_session.sql(query_1(events_input_table_name,devices_input_table_name, output_table_name, current_date))

def main():
    output_table_name: str = "jlcharbneau.user_devices_cumulated"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_1")
        .getOrCreate()
    )
    output_df = job_1(spark_session, "web_events", "user_devices", "user_devices_cumulated", "2023-08-20")
    output_df.write.mode("overwrite").insertInto(output_table_name)
