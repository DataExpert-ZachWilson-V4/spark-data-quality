from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


def query_1(output_table_name: str) -> str:
    query = f"""
    WITH last_year AS (
        SELECT *
        FROM {output_table_name}
        WHERE current_year = 2014
    ),
    this_year AS (
        SELECT
            actor,
            actor_id,
            year,
            ARRAY_AGG(ARRAY(film, film_id, year, votes, rating)) AS films,
        CASE
            WHEN AVG(rating) > 8 THEN 'star'
            WHEN AVG(rating) > 7 AND AVG(rating) <= 8 THEN 'good'
            WHEN AVG(rating) > 6 AND AVG(rating) <= 7 THEN 'average'
            WHEN AVG(rating) <= 6 THEN 'bad'
        END AS quality_class
        FROM actor_films
        WHERE year = 2015
        GROUP BY
            actor,
            actor_id,
            year
    )
    SELECT
        COALESCE(ly.actor, ty.actor) AS actor,
        COALESCE(ly.actor_id, ty.actor_id) AS actor_id,
        COALESCE(ty.quality_class, ly.quality_class) AS quality_class,
        CASE
            WHEN ty.year IS NULL THEN ly.films
            WHEN ty.year IS NOT NULL AND ly.films IS NULL THEN ty.films
            WHEN ty.year IS NOT NULL AND ly.films IS NOT NULL
                THEN ly.films || ty.films
        END AS films,
        ty.year IS NOT NULL AS is_active,
        COALESCE(ty.year, ly.current_year + 1) AS current_year
    FROM last_year ly
    FULL OUTER JOIN this_year ty ON ly.actor_id = ty.actor_id
    """

    return query


def job_1(
    spark_session: SparkSession, output_table_name: str
) -> Optional[DataFrame]:

    output_df = spark_session.table(output_table_name)
    output_df.createOrReplaceTempView(output_table_name)

    return spark_session.sql(query_1(output_table_name))


def main():
    output_table_name: str = "actors"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_1")
        .getOrCreate()
    )

    output_df = job_1(spark_session, output_table_name)
    output_df.write.mode("overwrite").insertInto(output_table_name)
