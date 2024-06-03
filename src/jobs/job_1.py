from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_1(output_table_name: str) -> str:
    query = f"""
WITH
  actor_recent_rating AS (
    SELECT
      actor,
      actor_id,
      ARRAY_AGG(ROW(YEAR, film, votes, rating, film_id)) AS films,
      AVG(rating) AS avg_rating,
      YEAR
    FROM
      actor_films
    WHERE
      YEAR = 1914 AND actor_id IS NOT NULL
    GROUP BY
      actor,
      actor_id,
      YEAR
  ),
  this_year AS (
    SELECT
      actor,
      actor_id,
      films,
      CASE
        WHEN avg_rating <= 6 THEN 'bad'
        WHEN avg_rating > 6
        AND avg_rating <= 7 THEN 'average'
        WHEN avg_rating > 7
        AND avg_rating <= 8 THEN 'good'
        ELSE 'star'
      END AS quality_class,
      YEAR
    FROM
      actor_recent_rating
  ),
  last_year AS (
    SELECT
      *
    FROM
      {output_table_name}
    WHERE
      current_year = 1913 AND actor_id IS NOT NULL
  )
  
SELECT
  COALESCE(ty.actor, ly.actor) AS actor,
  COALESCE(ty.actor_id, ly.actor_id) AS actor_id,
  CASE
    WHEN ty.year IS NULL THEN ly.films
    WHEN ty.year IS NOT NULL
    AND ly.current_year IS NULL THEN ty.films
    WHEN ty.year IS NOT NULL
    AND ly.current_year IS NOT NULL THEN ty.films || ly.films
  END AS films,
  CASE
    WHEN ty.quality_class IS NULL THEN ly.quality_class
    ELSE ty.quality_class
  END AS quality_class,
  CASE
    WHEN ty.year IS NOT NULL THEN TRUE
    ELSE FALSE
  END AS is_active,
  COALESCE(ty.year, ly.current_year + 1) AS current_year
FROM
  last_year AS ly
  FULL OUTER JOIN this_year AS ty ON ly.actor_id = ty.actor_id
    """
    return query

def job_1(spark_session: SparkSession, output_table_name: str) -> Optional[DataFrame]:
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
