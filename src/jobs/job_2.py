from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_2(output_table_name: str) -> str:
    query = f"""
    WITH
        yesterday AS (
                SELECT
                    *
                FROM
                    {output_table_name}
                WHERE
                    date = DATE ('2023-01-01')
            ),
        today AS (
                SELECT
                    we.user_id,
                    d.browser_type,
                    COLLECT_LIST (
                        DISTINCT CAST(date_trunc ('day', we.event_time) AS DATE)
                    ) AS dates_active,
                    count(1) as event_count
                FROM
                    devices d
                    LEFT JOIN web_events we ON we.device_id = d.device_id
                WHERE
                    date_trunc ('day', we.event_time) = DATE ('2023-01-02')
                GROUP BY
                    we.user_id,
                    d.browser_type
            )
        SELECT
            COALESCE(y.user_id, t.user_id) AS user_id,
            COALESCE(y.browser_type, t.browser_type) AS browser_type,
            CASE
                WHEN y.dates_active IS NOT NULL THEN t.dates_active || y.dates_active
                ELSE t.dates_active
            END AS dates_active,
            DATE ('2023-01-02') AS date
        FROM
            yesterday y
            FULL OUTER JOIN today t ON y.user_id = t.user_id
    """
    return query

def job_2(spark_session: SparkSession, output_table_name: str) -> Optional[DataFrame]:
  output_df = spark_session.table(output_table_name)
  output_df.createOrReplaceTempView(output_table_name)
  return spark_session.sql(query_2(output_table_name))

def main():
    output_table_name: str = "user_devices_cumulated"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_2")
        .getOrCreate()
    )
    output_df = job_2(spark_session, output_table_name)
    output_df.write.mode("overwrite").insertInto(output_table_name)
