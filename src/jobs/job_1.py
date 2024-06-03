from typing import Optional
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.utils import AnalysisException


def query_1(
        events_input_table_name: str,
        devices_input_table_name: str,
        output_table_name: str,
        current_date: str) -> str:
    try:
        dt = datetime.strptime(current_date, "%Y-%m-%d")
        previous_date = dt - timedelta(days=1)
        previous_date_str = previous_date.strftime("%Y-%m-%d")
    except ValueError as e:
        print(f"Error parsing date: {e}")
        return ""

    query = f"""
    WITH yesterday AS (
        SELECT *
        FROM 
            {output_table_name}
        WHERE 
            date = DATE('{previous_date_str}')
    ),
    today AS (
        SELECT
            we.user_id,
            d.browser_type,
            DATE('{current_date}') as todays_date,
            ARRAY_AGG(DATE(we.event_time)) AS event_dates
        FROM
            {devices_input_table_name} d
            JOIN {events_input_table_name} we ON d.device_id = we.device_id
        WHERE DATE(we.event_time) = DATE('{current_date}')
        GROUP BY we.user_id, d.browser_type
    )
    SELECT
        COALESCE(yesterday.user_id, today.user_id) AS user_id,
        COALESCE(yesterday.browser_type, today.browser_type) AS browser_type,
        CASE 
            WHEN yesterday.user_id IS NULL AND today.todays_date IS NOT NULL THEN ARRAY(today.todays_date)
            WHEN yesterday.dates_active IS NOT NULL AND today.todays_date IS NOT NULL THEN ARRAY(today.todays_date) || yesterday.dates_active
            WHEN yesterday.dates_active IS NOT NULL AND today.todays_date IS NULL THEN yesterday.dates_active
            ELSE NULL
        END AS dates_active,
        COALESCE(today.todays_date, yesterday.date + INTERVAL 1 DAY) AS date
    FROM
        yesterday
        FULL OUTER JOIN today
        ON yesterday.user_id = today.user_id
        AND yesterday.browser_type = today.browser_type
    """
    return query


def job_1(
        spark_session: SparkSession,
        events_input_table_name: str,
        devices_input_table_name: str,
        output_table_name: str,
        current_date: str) -> Optional[DataFrame]:

    try:
        output_df = spark_session.table(output_table_name)
        output_df.createOrReplaceTempView(output_table_name)
    except AnalysisException as e:
        print(f"Error accessing table {output_table_name}: {e}")
        return None

    try:
        result_df = spark_session.sql(
            query_1(
                events_input_table_name,
                devices_input_table_name,
                output_table_name,
                current_date
            )
        )
        return result_df
    except (ValueError, TypeError) as e:
        print(f"Error executing query: {e}")
        return None


def main():
    output_table_name: str = "jlcharbneau.user_devices_cumulated"
    try:
        spark_session: SparkSession = (
            SparkSession.builder
            .master("local")
            .appName("job_1")
            .getOrCreate()
        )
    except (RuntimeError, AnalysisException, ValueError) as e:
        print(f"Error creating Spark session: {e}")
        return

    output_df = job_1(
        spark_session,
        "web_events",
        "user_devices",
        "user_devices_cumulated",
        "2023-01-14"
    )

    if output_df is not None:
        try:
            output_df.write.mode("overwrite").insertInto(output_table_name)
        except AnalysisException as e:
            print(f"Error writing to table {output_table_name}: {e}")
    else:
        print("No DataFrame to write to the output table.")


if __name__ == "__main__":
    main()
