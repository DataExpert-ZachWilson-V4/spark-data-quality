from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


# CUMULATIVE TABLE [INCREMENTAL LOAD] => Below query populates the hosts_cumulated table one day at a time
# LOGIC => 1) USING FULL OUTER JOIN to get data from both yesterday(2023-01-01) and today(2023-01-02)
#          2) LOAD host_activity_datelist (ARRAY) from date for host
def query_2(input_table: str, output_table: str, current_date: str) -> str:
    query = f"""
    WITH yesterday AS (
      SELECT *
      FROM {output_table}
      WHERE date = DATE_SUB('{current_date}', 1)
    ),
    today AS (
      SELECT we.host, CAST(DATE_TRUNC('day', we.event_time) AS DATE) AS event_date
      FROM {input_table} we
      WHERE CAST(DATE_TRUNC('day', we.event_time) AS DATE) = DATE('{current_date}')
      GROUP BY we.host, CAST(DATE_TRUNC('day', we.event_time) AS DATE)
    )
    SELECT COALESCE(y.host,t.host) as host,
      CASE
        WHEN y.host_activity_datelist IS NOT NULL THEN ARRAY_UNION(ARRAY(t.event_date), y.host_activity_datelist)
        ELSE ARRAY(t.event_date)
      END AS host_activity_datelist,
      COALESCE(t.event_date, DATE_ADD(y.date, 1)) AS date
    FROM yesterday y
    FULL OUTER JOIN today t on y.host = t.host
    """
    return query


def job_2(spark_session: SparkSession, input_table: str, output_table: str, current_date: str) -> Optional[DataFrame]:
    return spark_session.sql(query_2(input_table, output_table, current_date))


def main():
    input_table: str = "bootcamp.web_events"
    output_table: str = "tharwaninitin.hosts_cumulated"
    current_date: str = "2022-12-02"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_2")
        .getOrCreate()
    )
    output_df = job_2(spark_session, input_table, output_table, current_date)
    output_df.writeTo(output_table).overwritePartitions()
