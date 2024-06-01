from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
import datetime

def query_2(cumulated_table_name: str, event_table_name: str, current_date: str) -> str:
    date_format = '%Y-%m-%d'
    previous_date = datetime.datetime.strptime(current_date, date_format) + datetime.timedelta(days=-1)
    previous_date = str(previous_date.date())
    return f"""
        WITH yesterday AS (
          SELECT user_id,
            dates_active,
            date
          FROM {cumulated_table_name}
          WHERE date = '{previous_date}'
        ), today AS (
          SELECT user_id,
            CAST(DATE_TRUNC('day', event_time) AS DATE) AS event_date,
            COUNT(1)
          FROM {event_table_name}
          WHERE DATE_TRUNC('day', event_time) = '{current_date}'
          GROUP BY user_id,
            DATE_TRUNC('day', event_time)
        )
        SELECT COALESCE(y.user_id, t.user_id) AS user_id,
          CASE WHEN y.dates_active IS NOT NULL THEN array(t.event_date) || y.dates_active ELSE array(t.event_date) END AS dates_active,
            '{current_date}' AS date
        FROM yesterday y
          FULL OUTER JOIN today t ON y.user_id = t.user_id
    """

def job_2(spark_session: SparkSession, cumulated_df: DataFrame, cumulated_table_name: str, event_df: DataFrame, event_table_name: str, current_date: str) -> Optional[DataFrame]:
    cumulated_df.createOrReplaceTempView(cumulated_table_name)
    event_df.createOrReplaceTempView(event_table_name)
    return spark_session.sql(query_2(cumulated_table_name, event_table_name, current_date))

def main():
    current_date = str(datetime.date.today())
    cumulated_table_name: str = "web_users_cumulated"
    event_table_name: str = "web_events"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_2")
        .getOrCreate()
    )    
    cumulated_df = spark_session.table(cumulated_table_name)
    event_df = spark_session.table(event_table_name)
    output_df = job_2(spark_session, cumulated_df, cumulated_table_name, event_df, event_table_name, current_date)
    output_df.write.mode("overwrite").insertInto(cumulated_table_name)