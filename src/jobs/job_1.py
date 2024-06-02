from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_1(output_table_name: str) -> str:
    """
    Constructs a SQL query to perform an upsert operation on the specified table. This query merges existing data with new data,
    updates existing records based on the new data, and inserts new records when no existing record matches.

    Args:
    output_table_name (str): The name of the table to perform the merge operation on.

    Returns:
    str: A SQL query string configured for performing an upsert (merge) operation.
    """
    query = f"""
    MERGE INTO {output_table_name} AS target
    USING (
        SELECT
            COALESCE(ls.actor, ts.actor) AS actor,
            COALESCE(ls.actor_id, ts.actor_id) AS actor_id,
            CASE
                WHEN ts.films IS NULL THEN ls.films
                ELSE array_union(ls.films, ts.films)
            END AS films,
            CASE
                WHEN ts.avg_rating > 8 THEN 'star'
                WHEN ts.avg_rating BETWEEN 7 AND 8 THEN 'good'
                WHEN ts.avg_rating BETWEEN 6 AND 7 THEN 'average'
                WHEN ts.avg_rating <= 6 THEN 'bad'
            END AS quality_class,
            ts.actor_id IS NOT NULL AS is_active,
            COALESCE(ts.year, ls.current_year + 1) AS current_year
        FROM
            (SELECT * FROM {output_table_name} WHERE current_year = 2020) ls
        FULL OUTER JOIN
            (
                SELECT
                    actor,
                    actor_id,
                    year,
                    array_agg(struct(film, votes, rating, film_id, year)) AS films,
                    avg(rating) AS avg_rating
                FROM bootcamp.actor_films
                WHERE year = 2021
                GROUP BY actor, actor_id, year
            ) ts ON ts.actor_id = ls.actor_id
    ) AS source
    ON target.actor_id = source.actor_id
    WHEN MATCHED THEN UPDATE SET
        actor = source.actor,
        films = source.films,
        quality_class = source.quality_class,
        is_active = source.is_active,
        current_year = source.current_year
    WHEN NOT MATCHED THEN INSERT (
        actor,
        actor_id,
        films,
        quality_class,
        is_active,
        current_year
    ) VALUES (
        source.actor,
        source.actor_id,
        source.films,
        source.quality_class,
        source.is_active,
        source.current_year
    )
    """
    return query

def job_1(spark_session: SparkSession, output_table_name: str) -> Optional[DataFrame]:
    """
    Executes the SQL upsert query on the specified table within a Spark session.
    This job function is pivotal in ensuring that the table data is current and accurately reflects the latest available data.

    Args:
    spark_session (SparkSession): The active Spark session to execute the job.
    output_table_name (str): The name of the table on which the upsert operation will be performed.

    Returns:
    DataFrame: The DataFrame representing the updated table for further use or verification.
    """
    output_df = spark_session.sql(query_1(output_table_name))
    output_df.createOrReplaceTempView(output_table_name)
    return output_df

def main():
    """
    Main executable function to trigger the upsert job using a Spark session.
    This function setups the Spark session, calls the job function, and handles the output.
    """
    output_table_name: str = "videet.actors"  # Designate the target table name
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("Upsert Job")
        .getOrCreate()
    )
    output_df = job_1(spark_session, output_table_name)
    output_df.write.mode("overwrite").insertInto(output_table_name)


