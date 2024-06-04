from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_2(output_table_name: str) -> str:
    query = f"""
            WITH
        yesterday AS (
            SELECT *
            FROM
                {output_table_name}
            WHERE
                date = DATE('2022-12-31')
        ),
        today AS (
            SELECT
                host,
                CAST(DATE_TRUNC('day', event_time) AS DATE) AS event_date,
                COUNT(*) AS count
            FROM
                web_events
            WHERE
                DATE_TRUNC('day', event_time) = DATE('2023-01-01')
            GROUP BY
                host,
                CAST(DATE_TRUNC('day', event_time) AS DATE)
        )
        SELECT
            COALESCE(y.host, t.host) AS host,
            CASE
                WHEN
                    y.host_activity_datelist IS NOT NULL
                    THEN array(t.event_date) || y.host_activity_datelist
                ELSE array(t.event_date)
            END AS host_activity_datelist,
            DATE('2023-01-01') AS date
        FROM
            yesterday AS y
        FULL OUTER JOIN today AS t ON y.host = t.host
    """
    return query

def job_2(spark_session: SparkSession, output_table_name: str) -> Optional[DataFrame]:
  output_df = spark_session.table(output_table_name)
  output_df.createOrReplaceTempView(output_table_name)
  return spark_session.sql(query_2(output_table_name))

def main():
    output_table_name: str = "hosts_cumulated"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_2")
        .getOrCreate()
    )
    output_df = job_2(spark_session, output_table_name)
    output_df.write.mode("overwrite").insertInto(output_table_name)
