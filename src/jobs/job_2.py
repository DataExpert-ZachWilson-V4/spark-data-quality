from pyspark.sql import SparkSession

def job_2(spark_session: SparkSession, output_table_name: str):
    query = f"""
    CREATE OR REPLACE TEMP VIEW {output_table_name} AS
    WITH lagged AS (
        SELECT
            actor,
            CASE
                WHEN rating >= 7 THEN 'A'
                WHEN rating >= 5 THEN 'B'
                ELSE 'C'
            END AS quality_class,
            CASE
                WHEN current_year >= 2023 THEN 1
                ELSE 0
            END AS is_active,
            current_year,
            LAG(CASE
                WHEN current_year >= 2023 THEN 1
                ELSE 0
            END) OVER (PARTITION BY actor ORDER BY current_year ASC) AS is_active_last_year
        FROM actors
        WHERE current_year <= 2023
    ),
    tally_history AS (
        SELECT
            *,
            SUM(CASE
                WHEN is_active != is_active_last_year THEN 1
                ELSE 0
            END) OVER (PARTITION BY actor ORDER BY current_year ASC) AS actor_identifier
        FROM lagged
    )
    SELECT
        actor,
        quality_class,
        actor_identifier,
        MIN(current_year) AS start_date,
        MAX(current_year) AS end_date,
        2023 AS current_year
    FROM tally_history
    GROUP BY actor, quality_class, actor_identifier
    """
    spark_session.sql(query)
    result_df = spark_session.sql(f"SELECT * FROM {output_table_name}")
    return result_df



