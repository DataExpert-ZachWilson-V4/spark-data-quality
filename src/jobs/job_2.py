from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_2(output_table_name: str) -> str:
    query = f"""
        WITH last_year AS ( -- CTE to extract the changing dimensions tracked for current and previous year
        SELECT
            actor,
            actor_id,
            quality_class,
            lag(quality_class) OVER (
                PARTITION BY actor
                ORDER BY current_year
            ) AS prev_quality_class,
            is_active,
            lag(is_active) OVER (
                PARTITION BY actor
                ORDER BY current_year
            ) AS prev_is_active,
            current_year
        FROM {output_table_name}
        WHERE current_year = 2003
    ),
    result AS ( -- CTE to track if anything changed between previous and current year
        SELECT
            actor,
            actor_id,
            quality_class,
            prev_quality_class,
            is_active,
            prev_is_active,
            SUM(
                CASE
                    WHEN quality_class <> prev_quality_class
                    OR is_active <> prev_is_active THEN 1
                    ELSE 0
                END
            ) OVER (
                PARTITION BY actor
                ORDER BY
                    current_year
            ) AS did_change,
            current_year
        FROM last_year
    )
    -- Build the final query to be loaded into SCD table
    SELECT
        DISTINCT actor,
        actor_id,
        quality_class,
        is_active,
        min(current_year) OVER (PARTITION BY actor, did_change) AS start_date,
        max(current_year) OVER (PARTITION BY actor, did_change) AS end_date,
        max(current_year) OVER () AS current_year
    FROM result
    """
    return query

def job_2(spark_session: SparkSession, output_table_name: str) -> Optional[DataFrame]:
  output_df = spark_session.table(output_table_name)
  output_df.createOrReplaceTempView(output_table_name)
  return spark_session.sql(query_2(output_table_name))

def main():
    output_table_name: str = "actors"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_2")
        .getOrCreate()
    )
    output_df = job_2(spark_session, output_table_name)
    output_df.write.mode("overwrite").insertInto(output_table_name)
