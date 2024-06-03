from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_1(input_table: str, output_table_name: str, current_year: int) -> str:
    query = f"""
    WITH
      last_year AS (
        SELECT *
        FROM {output_table_name}
        WHERE current_year = {current_year}
      ),
      this_year AS (
        SELECT actor, actor_id, year,
          ARRAY_AGG( (film, votes, rating, film_id) ) films,
          CASE WHEN avg(rating) > 8 THEN 'star'
            WHEN avg(rating) > 7 and avg(rating) <=8 THEN 'good'
            WHEN avg(rating) > 6 and avg(rating) <= 7 THEN 'average'
            WHEN avg(rating) <= 6 THEN 'bad'
          END as quality_class
        FROM {input_table}
        WHERE year = {current_year} + 1
        GROUP BY actor, actor_id, year
      )
    SELECT
      COALESCE(ty.actor, ly.actor) AS actor,
      COALESCE(ty.actor_id, ly.actor_id) AS actor_id,
      CASE
        WHEN ty.films IS NULL THEN ly.films
        WHEN ty.films IS NOT NULL AND ly.films IS NULL THEN ty.films
        WHEN ty.films IS NOT NULL AND ly.films IS NOT NULL THEN ty.films || ly.films
      END AS films,
      COALESCE(ty.quality_class, ly.quality_class) AS quality_class,
      ty.year IS NOT NULL AS is_active,
      COALESCE(ty.year, ly.current_year + 1) AS current_year
    FROM last_year ly
    FULL OUTER JOIN this_year ty ON ly.actor_id = ty.actor_id
    """
    return query

def job_1(spark_session: SparkSession, input_table: str, output_table_name: str, current_year: int) -> Optional[DataFrame]:
    return spark_session.sql(query_1(input_table, output_table_name, current_year))

def main():
    input_table: str = "bootcamp.actor_films"
    output_table: str = "tharwaninitin.actors"
    current_year: int = 1913
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_1")
        .getOrCreate()
    )
    output_df = job_1(spark_session, input_table, output_table, current_year)
    output_df.writeTo(output_table).overwritePartitions()
