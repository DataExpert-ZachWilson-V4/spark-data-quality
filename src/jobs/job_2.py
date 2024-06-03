from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_2(input_table_name: str) -> str:
    query = f"""
    WITH lagged AS (
            SELECT actor,
                actor_id,
                average_rating,
                quality_class,
                CASE
                    WHEN is_active THEN 1 -- Convert is_active boolean to integer 
                    ELSE 0
                END AS is_active,
                CASE
                    WHEN LAG (is_active, 1) OVER (
                        PARTITION BY actor_id
                        ORDER BY current_year ASC
                    ) THEN 1
                    ELSE 0
                END AS is_active_last_year,
                -- Check if the actor was active last year using LAG function
            CAST(current_year AS int) AS current_year
            FROM {input_table_name}
        ),
        -- CTE to calculate streak identifiers based on changes in is_active status
        streaked AS (
            SELECT *,
                SUM(
                    CASE
                        WHEN is_active <> is_active_last_year THEN 1 -- Count streak changes in is_active status
                        ELSE 0
                    END
                ) OVER (
                    PARTITION BY actor_id
                    ORDER BY current_year
                ) AS streak_identifier
            FROM lagged
        )
        -- Main query to aggregate data based on streak identifiers
        SELECT
            actor,
            actor_id,
            CAST(MAX(is_active)AS BOOLEAN)  AS is_active, -- Take maximum is_active value as indicator of current activity
            COALESCE(AVG(average_rating), 0) AS average_rating, -- Calculate average rating with COALESCE to handle NULL values
            COALESCE(MAX(quality_class), 'unknown') AS quality_class, -- Get maximum quality_class with COALESCE for NULL handling
            MIN(current_year) AS start_date, -- Get earliest current film year as start_date
            MAX(current_year) AS end_date, -- Get latest current film year as end_date
            2012 AS current_year -- Set current_year value to 2012
        FROM
            streaked -- Use streaked CTE for aggregation
        GROUP BY
            actor,
            actor_id, -- group by actor unique identifier
            streak_identifier -- Group by streak_identifier to maintain streak boundaries
    """
    return query
def job_2(spark_session: SparkSession, output_table_name: str) -> Optional[DataFrame]:
  input_table_name = "actors"
  input_df.createOrReplaceTempView(input_table_name)
  input_df = spark_session.sql(query_2(input_table_name))
  return input_df
def main():
    output_table_name: str = "sagararora492.actors_history_scd"
    spark_session: SparkSession = (
        SparkSession.builder.master("local").appName("job_2").getOrCreate()
    )
    output_df = job_2(spark_session, spark_session.table(output_table_name))
    output_df.write.mode("overwrite").insertInto(output_table_name)
