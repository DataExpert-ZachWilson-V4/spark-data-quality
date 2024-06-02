from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_1(output_table_name: str) -> str:
    query = f"""WITH lagged AS (SELECT actor, actor_id, quality_class, LAG(quality_class, 1) OVER (PARTITION BY actor ORDER BY current_year) AS quality_class_last_year, is_active, current_year FROM actors WHERE current_year <= 1921), changed AS (SELECT *, SUM(CASE WHEN quality_class = quality_class_last_year OR (quality_class IS NULL AND quality_class_last_year IS NULL) THEN 0 ELSE 1 END) OVER (PARTITION BY actor ORDER BY current_year) AS streak FROM lagged) SELECT actor, actor_id, MAX(quality_class) AS quality_class, MAX(is_active) AS is_active, MIN(current_year) AS start_date, MAX(current_year) AS end_date, 1921 AS current_year FROM changed GROUP BY actor, actor_id, streak"""
    return query
def job_1(spark_session: SparkSession, output_table_name: str) -> Optional[DataFrame]:
  output_df = spark_session.table(output_table_name)
  output_df.createOrReplaceTempView(output_table_name)
  return spark_session.sql(query_1(output_table_name))
def main():
    output_table_name: str = "spark_actors_history_scd"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_1")
        .getOrCreate()
    )
    output_df = job_1(spark_session, output_table_name)
    output_df.write.mode("overwrite").insertInto(output_table_name)
