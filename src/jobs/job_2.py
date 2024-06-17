from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_2(output_table_name: str) -> str:
    query = f"""

WITH
  yesterday AS (
    SELECT
      host,
      metric_name,
      metric_array,
      CAST(month_start AS DATE) AS month_start
    FROM
      raj.host_activity_reduced
    WHERE
      CAST(month_start AS DATE) = DATE '2023-01-01'
  ),
  today AS (
    SELECT
      host,
      metric_name,
      CAST(FLOOR(CAST(metric_value AS DOUBLE)) AS INT) AS metric_value, -- Adjust casting here
      CAST(DATE AS DATE) AS DATE
    FROM
      raj.daily_web_metrics
    WHERE
      CAST(DATE AS DATE) = DATE '2023-01-02'
  )
SELECT
  COALESCE(y.host, t.host) AS host,
  COALESCE(y.metric_name, t.metric_name) AS metric_name,
  COALESCE(
    y.metric_array,
    TRANSFORM(
      SEQUENCE(
        1,
        ABS(DATE_DIFF('day', DATE '2023-01-01', t.date))
      ),
      x -> CAST(NULL AS INTEGER)
    )
  ) || ARRAY[t.metric_value] AS metric_array,
  DATE '2023-01-01' AS month_start
FROM
  yesterday y
  FULL OUTER JOIN today t ON y.host = t.host
  AND y.metric_name = t.metric_name

    """

    return query

def job_2(spark_session: SparkSession, output_table_name: str) -> Optional[DataFrame]:
  output_df = spark_session.table(output_table_name)
  output_df.createOrReplaceTempView(output_table_name)
  return spark_session.sql(query_2(output_table_name))

def main():
    output_table_name: str = "raj.host_activity_reduced"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_2")
        .getOrCreate()
    )
    output_df = job_2(spark_session, output_table_name)
    output_df.write.mode("overwrite").insertInto(output_table_name)
