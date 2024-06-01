from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

month_start = '2023-08-01'
def query_2(output_table_name: str, month_start: str, current_date: str) -> str:
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
            daily_web_metrics
        WHERE
            date = DATE('{current_date}')
    )
SELECT
    COALESCE(t.host, y.host) as host,
    COALESCE(t.metric_name, y.metric_name) as metric_name,
    COALESCE(
        y.metric_array,
        array_repeat(
            null,
            CAST(
                DATE_DIFF(DAY, DATE('{month_start}'), t.date) AS INTEGER
            )
        )
    ) || array(t.metric_value) as metric_array,
    '{month_start}' as month_start
FROM
    today t
    FULL OUTER JOIN yesterday y ON t.host = y.host
    AND t.metric_name = y.metric_name
    """
    return query

def job_2(spark_session: SparkSession, output_table_name: str, month_start: str, current_date:str) -> Optional[DataFrame]:
  output_df = spark_session.table(output_table_name)
  output_df.createOrReplaceTempView(output_table_name)
  return spark_session.sql(query_2(output_table_name, month_start, current_date))

def main():
    output_table_name: str = "host_activity_reduced"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_2")
        .getOrCreate()
    )
    output_df = job_2(spark_session, output_table_name, month_start, month_start)
    output_df.write.mode("overwrite").insertInto(output_table_name)
