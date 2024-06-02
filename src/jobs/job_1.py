from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_1(input_table_name: str) -> str:
    query = f"""
    WITH
        lagged AS (
        SELECT
        actor,
        quality_class,
        is_active,
        LAG(is_active, 1) OVER (PARTITION BY actor_id ORDER BY current_year) AS is_active_last_year,
        LAG(quality_class, 1) OVER (PARTITION BY actor_id ORDER BY current_year) AS quality_class_last_year,
        current_year
        FROM
        {input_table_name}
        WHERE
        current_year <= 2021
    ),
    streaked AS (
        SELECT
        *,
        SUM(
            CASE
            WHEN is_active <> is_active_last_year OR quality_class <> quality_class_last_year THEN 1
            ELSE 0
            END) OVER ( PARTITION BY actor ORDER BY current_year
        ) AS streak_identifier
        FROM
        lagged
    )
    SELECT
    actor,
    quality_class,
    is_active,
    MIN(current_year) AS start_date,
    MAX(current_year) AS end_date,
    2021 AS current_year

    FROM
    streaked
    
    GROUP BY
    actor,
    quality_class,
    is_active,
    streak_identifier
    """
    return query

def job_1(spark_session: SparkSession, input_table_name: str, output_table_name: str) -> Optional[DataFrame]:
  
  # this is the base source of the query (lagged)
  input_df = spark_session.table(input_table_name)
  input_df.createOrReplaceTempView(input_table_name)

  # this is the table in which the results will be saved
  output_df = spark_session.table(output_table_name)
  output_df.createOrReplaceTempView(output_table_name)
  return spark_session.sql(query_1(input_table_name))

def main():
    input_table_name: str = "actors"
    output_table_name: str = "spark_actors_history"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_1")
        .getOrCreate()
    )
    output_df = job_1(spark_session, input_table_name)
    output_df.write.mode("overwrite").insertInto(output_table_name)
