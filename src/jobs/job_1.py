from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SQLContext

def query_1(input_table_name: str, output_table_name: str) -> str:
    query = f"""
                WITH actor_changes AS (
                -- Get distinct years for which we have actor data
                SELECT DISTINCT current_year
                FROM {input_table_name}
            ),
            actor_history AS (
                -- Generate historical records for each actor for each year
                SELECT
                    a.actor_id,
                    a.quality_class,
                    a.is_active,
                    a.current_year AS start_date,
                    a.current_year AS end_date,
                    (SELECT EXTRACT(YEAR FROM current_date)) AS `current_date`
                FROM {input_table_name} a
                JOIN actor_changes ac ON a.current_year = ac.current_year
            ),
            final_history AS (
                SELECT
                    actor_id,
                    quality_class,
                    is_active,
                    start_date,
                    end_date,
                    `current_date`
                FROM actor_history
                UNION ALL
                -- Handle changes in actors' status or quality class over the years
                SELECT
                    a.actor_id,
                    a.quality_class,
                    a.is_active,
                    MIN(a.start_date) AS start_date,
                    MAX(a.end_date) AS end_date,
                    a.`current_date`
                FROM actor_history a
                GROUP BY
                    a.actor_id,
                    a.quality_class,
                    a.is_active,
                    a.`current_date`
            )
            -- Select final history records for insertion
            SELECT DISTINCT
                actor_id,
                quality_class,
                is_active,
                start_date,
                end_date,
                CAST(`current_date` AS INTEGER) AS `current_date`
            FROM final_history
    """
    return query

def job_1(spark_session: SparkSession, input_table_name: str, output_table_name: str) -> Optional[DataFrame]:
  output_df = spark_session.table(output_table_name)
  output_df.createOrReplaceTempView(output_table_name)
  return spark_session.sql(query_1(input_table_name,output_table_name))

def main():
    output_table_name: str = "actor_history_scd"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_1")
        .getOrCreate()
    )
    output_df = job_1(spark_session, output_table_name)
    output_df.write.mode("overwrite").insertInto(output_table_name)
