from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_2(output_table_name_1: str, output_table_name_2: str) -> str:
    query = f"""
            WITH yesterday AS (
            SELECT * FROM {output_table_name_1}
            WHERE month_start = DATE('2023-08-01')
            ),

            today as (
            SELECT 
                *
            FROM {output_table_name_2}
            WHERE date = DATE('2023-08-03')
            )
            SELECT 
            COALESCE(y.host, t.host) AS host,
            COALESCE(y.metric_name, t.metric_name) AS metric_name,
            COALESCE(y.metric_array, ARRAY_REPEAT(CAST(null AS BIGINT), CAST(DATEDIFF(t.date, y.month_start) AS INT))) || ARRAY(CAST(t.metric_value AS BIGINT)) AS metric_array,
        DATE('2023-08-01') AS month_start
            FROM yesterday y
            FULL OUTER JOIN today t
            ON y.host = t.host AND y.metric_name = t.metric_name
            """
    return query

def job_2(spark_session: SparkSession, output_table_name_1: str, output_table_name_2: str) -> Optional[DataFrame]:
  output_df_1 = spark_session.table(output_table_name_1)
  output_df_1.createOrReplaceTempView(output_table_name_1)

  output_df_2 = spark_session.table(output_table_name_2)
  output_df_2.createOrReplaceTempView(output_table_name_2)

  return spark_session.sql(query_2(output_table_name_1, output_table_name_2))

def main():
    output_table_name: str = "renee_nba_game_details_ranking"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_2")
        .getOrCreate()
    )
    output_df = job_2(spark_session, output_table_name)
    output_df.write.mode("overwrite").insertInto(output_table_name)
