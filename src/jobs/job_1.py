from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_1(output_table_name: str) -> str:
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
    output_df = spark_session.sql(query_1(output_table_name))
    output_df.createOrReplaceTempView(output_table_name)
    return output_df

def main():
    output_table_name: str = "videet.actors"  # Set your Delta table name here
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("Upsert Job")
        .getOrCreate()
    )
    output_df = job_1(spark_session, output_table_name)
    output_df.write.mode("overwrite").insertInto(output_table_name)

# Note: This assumes you have Delta Lake setup in your Spark environment. Modify `format` and methods accordingly if using different configurations.
