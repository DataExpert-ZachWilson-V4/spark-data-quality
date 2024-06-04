from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_1(output_table_name: str) -> str:
    query = f"""
    INSERT INTO
    phabrahao.actors
    WITH
    last_year AS (
        SELECT
        *
        FROM
        phabrahao.actors
        WHERE
        current_year = 1929
    ),
    this_year AS (
        SELECT
        actor,
        actor_id,
        YEAR,
        sum(votes * rating) / sum(votes) AS avg_rating,
        array_agg(ROW(film, votes, rating, film_id)) AS film
        FROM
        bootcamp.actor_films
        WHERE
        YEAR = 1930
        GROUP BY
        actor,
        actor_id,
        YEAR
    )
    SELECT
    COALESCE(ly.actor, ty.actor) AS actor,
    COALESCE(ly.actor_id, ty.actor_id) AS actor_id,
    CASE
        WHEN ty.film IS NULL THEN ly.films
        WHEN ty.film IS NOT NULL
        AND ly.films IS NULL THEN ty.film
        WHEN ty.film IS NOT NULL
        AND ly.films IS NOT NULL THEN ty.film || ly.films
    END AS films,
    CASE
        WHEN avg_rating > 8 THEN 'star'
        WHEN avg_rating > 7 THEN 'good'
        WHEN avg_rating > 6 THEN 'average'
        WHEN avg_rating <= 6 THEN 'bad'
    END AS quality_class,
    ty.actor IS NOT NULL AS is_active,
    COALESCE(ty.year, ly.current_year + 1) AS current_year
    FROM
    last_year ly
    FULL OUTER JOIN this_year ty ON ly.actor_id = ty.actor_id
    """
    return query

def job_1(spark_session: SparkSession, output_table_name: str) -> Optional[DataFrame]:
  output_df = spark_session.table(output_table_name)
  output_df.createOrReplaceTempView(output_table_name)
  return spark_session.sql(query_1(output_table_name))

def main():
    output_table_name: str = "phabrahao.actors"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_1")
        .getOrCreate()
    )
    output_df = job_1(spark_session, output_table_name)
    output_df.write.mode("overwrite").insertInto(output_table_name)
