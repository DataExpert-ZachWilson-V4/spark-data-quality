from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

month_start = '2023-08-01'
def query_2(output_table_name: str, month_start: str) -> str:
    query = f"""
    WITH
    yesterday AS (
        SELECT
            *
        FROM
            {output_table_name}
        WHERE
            month_start = '{month_start}'
    ),
    today AS (
        SELECT
            *
        FROM
            barrocaeric.daily_web_metrics
        WHERE
            date = DATE('{month_start}')
    )
SELECT
    COALESCE(t.host, y.host) as host,
    COALESCE(t.metric_name, y.metric_name),
    COALESCE(
        y.metric_array,
        REPEAT(
            null,
            CAST(
                DATE_DIFF('day', DATE('{month_start}'), t.date) AS INTEGER
            )
        )
    ) || ARRAY[t.metric_value] as metric_array,
    '{month_start}' as month_start
FROM
    today t
    FULL OUTER JOIN yesterday y ON t.host = y.host
    AND t.metric_name = y.metric_name
    """
    return query

def job_2(spark_session: SparkSession, output_table_name: str, month_start: str) -> Optional[DataFrame]:
  output_df = spark_session.table(output_table_name)
  output_df.createOrReplaceTempView(output_table_name)
  return spark_session.sql(query_2(output_table_name, month_start))

def main():
    output_table_name: str = "barrocaeric.host_activity_reduced"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_2")
        .getOrCreate()
    )
    output_df = job_2(spark_session, output_table_name, month_start)
    output_df.write.mode("overwrite").insertInto(output_table_name)
