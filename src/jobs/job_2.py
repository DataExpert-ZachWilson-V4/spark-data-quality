from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


def query_2(output_table_name: str) -> str:
    query = f"""
            WITH lagged AS (
                SELECT 
                    actor,
                    actor_id,
                    quality_class,
                    is_active,
                    LAG(is_active, 1) OVER (partition by actor, actor_id ORDER BY current_year) AS is_active_last_year,
                    current_year
                from 
                    {output_table_name}
                ),
                streaked AS (
                SELECT 
                    * ,
                    SUM(CASE WHEN is_active <> is_active_last_year THEN 1 ELSE 0 END) OVER (partition by actor, actor_id ORDER BY current_year) AS streak_identifier
                FROM 
                    lagged
                )
                SELECT 
                    actor, 
                    actor_id, 
                    quality_class, 
                    MAX(is_active) AS is_active,
                    MIN(current_year) AS start_date,
                    MAX(current_year) AS end_date,
                    current_year
                FROM 
                    streaked
                GROUP BY 
                    actor, 
                    actor_id, 
                    quality_class, 
                    streak_identifier, 
                    current_year
    """
    return query


def job_2(spark: SparkSession, output_table_name: str) -> Optional[DataFrame]:
    output_df = spark.table(output_table_name)
    output_df.createOrReplaceTempView(output_table_name)
    return spark.sql(query_2(output_table_name))

def main():
    """
    Main executable function to trigger the upsert job using a Spark session.
    This function setups the Spark session, calls the job function, and handles the output.
    """

    output_table_name: str = "actors"  # Designate the target table name
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
    )
    output_df = job_2(spark_session, output_table_name)
    output_df.write.mode("overwrite").insertInto(output_table_name)
