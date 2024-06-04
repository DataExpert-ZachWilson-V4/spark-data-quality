from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_2(cumulated_table_name: str, event_table_name: str) -> str:
    query = f"""
    WITH
  yesterday AS (
    SELECT
      host,host_activity_datelist,date
    FROM
      {cumulated_table_name}
    WHERE
      DATE = DATE('2023-01-01')
  ),
  today AS (
    SELECT
      we.host,
      CAST(date_trunc('day', we.event_time) AS DATE) AS event_date,
      COUNT(1)
    FROM
      {event_table_name} AS we
    WHERE
      date_trunc('day', we.event_time) = DATE('2023-01-02')
    GROUP BY
      we.host,
      CAST(date_trunc('day', we.event_time) AS DATE)
  )
SELECT
  COALESCE(y.host, t.host) AS host,
  CASE
    WHEN y.host_activity_datelist IS NOT NULL THEN ARRAY(t.event_date) || y.host_activity_datelist
    ELSE ARRAY(t.event_date)
  END AS host_activity_datelist,
  '2023-01-02' AS date
FROM
  yesterday y
  FULL OUTER JOIN today t ON y.host= t.host
    """
    return query

def job_2(spark_session: SparkSession, cumulated_df: DataFrame, 
cumulated_table_name: str, event_df: DataFrame, event_table_name: str) -> Optional[DataFrame]:
  cumulated_df.createOrReplaceTempView(cumulated_table_name)
  event_df.createOrReplaceTempView(event_table_name)
  return spark_session.sql(query_2(cumulated_table_name, event_table_name))

def main():
    cumulated_table_name: str = "hosts_cumulated"
    event_table_name: str = "web_events"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_2")
        .getOrCreate()
    )
    cumulated_df = spark_session.table(cumulated_table_name)
    event_df = spark_session.table(event_table_name)
    output_df = job_2(spark_session, cumulated_df, cumulated_table_name, event_df, event_table_name)
    output_df.write.mode("overwrite").insertInto(cumulated_table_name)
