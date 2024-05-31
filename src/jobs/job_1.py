from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

current_year: int = 1999

def query_1(output_table_name: str, current_year: int) -> str:
    query = f"""
    WITH
    last_year AS (
        SELECT
            *
        FROM
            {output_table_name}
        WHERE
            current_year = {current_year}
    ),
    -- Moved aggregations for this CTE for simplicity and readability,
    -- it was necessary to aggregate data since one actor can have many films in 1 year.
    -- I think it is possible to achieve the same results using reduce or map instead of group by.
    this_year AS (
        SELECT
            actor_id,
            actor,
            array_agg((film, votes, rating, film_id, year)) FILTER(
                WHERE
                    film IS NOT NULL
            ) as film,
            AVG(rating) as rating,
            MAX(year) as year
        FROM
            bootcamp.actor_films
        WHERE
            year = {current_year + 1}
        group by
            actor_id,
            actor
    )
SELECT
    COALESCE(ls.actor, ts.actor) as actor,
    COALESCE(ls.actor_id, ts.actor_id) as actor_id,
    CASE
        WHEN ts.film IS NULL THEN ls.films
        WHEN ts.film IS NOT NULL
        AND ls.films IS NULL THEN ts.film
        WHEN ts.film IS NOT NULL
        AND ls.films IS NOT NULL THEN ts.film || ls.films
    END as films,
    -- If there is no record for the actor in this year we should use last year quality class
    -- else we just have to compute the average of the current year ratings to classify the actor quality
    CASE
        WHEN ts.actor_id IS NULL THEN ls.quality_class
        WHEN ts.rating > 8 THEN 'star'
        WHEN ts.rating > 7 THEN 'good'
        WHEN ts.rating > 6 THEN 'average'
        ELSE 'bad'
    END as quality_class,
    ts.actor_id IS NOT NULL AS is_active,
    COALESCE(ts.year, ls.current_year + 1) as current_year
FROM
    last_year ls
    FULL OUTER JOIN this_year ts ON ls.actor_id = ts.actor_id
    """
    return query

def job_1(spark_session: SparkSession, output_table_name: str, current_year: int) -> Optional[DataFrame]:
  output_df = spark_session.table(output_table_name)
  output_df.createOrReplaceTempView(output_table_name)
  return spark_session.sql(query_1(output_table_name, current_year))

def main():
    output_table_name: str = "barrocaeric.actors"

    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_1")
        .getOrCreate()
    )
    output_df = job_1(spark_session, output_table_name, current_year)
    output_df.write.mode("overwrite").insertInto(output_table_name)
