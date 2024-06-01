from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
import datetime

# query_2 below is the source query to add one day to the dates_active array of the web_users_cumulated table,
# which shows dates each user was active on one of the DataExpert.io web pages
# The output of this query is loaded into a new partition of web_users_cumulated for the current date.
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

# Like job_1(), I made some edits to the main() and job_2() definitions to more closely match what Zach demonstrated in the V4 Week 4 Day 3 Lab (V3 Week 5 Day 1).
# Here we are querying the web_events table and building a dataframe concatenating that result into an array of active dates for each user in web_users_cumulated.
# producing a cumulative table of one row per user with an array of dates that user was active.
# That dataframe then will be loaded to the current partition of web_users_cumulated, which is a cumulative table of one row per user with an array of dates that user was active.
# I have updated the varable names appropriately:
#   event_table_name - the source table for the current day's active users, web_events
#   cumulated_table_name - the target table of the Spark job, web_users_cumulated
#   event_df - definition of the source table (web_events) for the Spark Session
#   cumulated_df - definition of the existing cumulated table (web_users_cumulated) for the Spark Session (as of yesterday)
#   output_df - Dataframe containing results of query_2, to be written to the current partition of web_users_cumulated, with overwrite option. This adds today's data
#       to web_users_cumulated
# I have also added logic so that dates can be passed in dynamically, with yesterday's date calculated from today's date and formatted for use with SparkSQL
# Having a standalone function (here - job_2()) which creates the output Dataframe allows the query logic to be subjected to unit tests.
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