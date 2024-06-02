from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_1(output_table_name: str) -> str:
    query = f"""
            WITH lagged AS (
                SELECT 
                actor,
                actor_id,
                quality_class,
                is_active,
                LAG(is_active, 1) OVER (partition by actor, actor_id ORDER BY current_year) AS is_active_last_year,
                current_year
                FROM {output_table_name}
                ),

                streaked AS (
                SELECT 
                * ,
                SUM(CASE WHEN is_active <> is_active_last_year THEN 1 ELSE 0 END) OVER (partition by actor, actor_id ORDER BY current_year) AS streak_identifier
                FROM lagged
                )

                SELECT 
                actor, 
                actor_id, 
                quality_class, 
                MAX(is_active) AS is_active,
                MIN(current_year) AS start_date,
                MAX(current_year) AS end_date,
                current_year
                FROM streaked
                GROUP BY actor, actor_id, quality_class, streak_identifier, current_year
    """
    return query

def job_1(spark_session: SparkSession, output_table_name: str) -> Optional[DataFrame]:
  output_df = spark_session.table(output_table_name) 
  output_df.createOrReplaceTempView(output_table_name)
  return spark_session.sql(query_1(output_table_name))

def main():
    output_table_name: str = "renee_actors_history_scd"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_1")
        .getOrCreate()
    )
    output_df = job_1(spark_session, output_table_name)
    output_df.write.mode("overwrite").insertInto(output_table_name)
