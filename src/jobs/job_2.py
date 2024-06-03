import datetime
from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_2(cumulated_table_name: str, web_event_table_name: str, device_table_name:str, current_date: str) -> str:
    date_format = '%Y-%m-%d'
    previous_date = datetime.datetime.strptime(current_date, date_format) + datetime.timedelta(days=-1)
    previous_date = str(previous_date.date())
    query = f"""
        WITH old_data AS (
            SELECT
                *
            FROM
                {cumulated_table_name}
            WHERE
                date = '{previous_date}'
            ),
            new_data AS (
            SELECT
                we.user_id,
                d.browser_type,
                DATE(we.event_time) AS event_date
            FROM
                {web_event_table_name} we
            LEFT JOIN
                {device_table_name} d
            ON
                d.device_id = we.device_id
            WHERE
                DATE(event_time) = '{current_date}'
            GROUP BY
                we.user_id,
                d.browser_type,
                DATE(we.event_time)
            )
            SELECT
            COALESCE(nd.user_id, od.user_id) AS user_id,
            COALESCE(nd.browser_type, od.browser_type) AS browser_type,
            CASE
                WHEN od.dates_active IS NULL THEN array(CAST(nd.event_date AS STRING))
                ELSE array_union(array(CAST(nd.event_date AS STRING)), od.dates_active)
            END AS dates_active,
            '{current_date}' AS date
            FROM
            new_data nd
            FULL OUTER JOIN
            old_data od
            ON
            nd.user_id = od.user_id
            AND nd.browser_type = od.browser_type
    """
    return query

def job_2(spark_session: SparkSession, cumulated_df: DataFrame, cumulated_table_name: str, web_event_df: DataFrame, web_event_table_name: str, device_df: DataFrame, device_table_name:str, current_date: str) -> Optional[DataFrame]:
    cumulated_df.createOrReplaceTempView(cumulated_table_name)
    web_event_df.createOrReplaceTempView(web_event_table_name)
    device_df.createOrReplaceTempView(device_table_name)
    return spark_session.sql(query_2(cumulated_table_name, web_event_table_name, device_table_name, current_date))

def main():
    current_date = str(datetime.date.today())
    cumulated_table_name: str = "user_devices_cumulated"
    web_event_table_name: str = "web_events"
    device_table_name:str = 'devices'
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_2")
        .getOrCreate()
    )
    cumulated_df = spark_session.table(cumulated_table_name)
    web_event_df = spark_session.table(web_event_table_name)
    device_df = spark_session.table(device_table_name)
    output_df = job_2(spark_session, cumulated_df, cumulated_table_name, web_event_df, web_event_table_name, device_df, device_table_name, current_date)
    output_df.write.mode("overwrite").insertInto(cumulated_table_name)