from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_1(output_table_name: str) -> str:
    query = f"""
    WITH
  last_year AS (
    SELECT
      *
    FROM
      {output_table_name}
    WHERE
      current_year = 2017
  ),
  this_year AS (
    SELECT
      actor,
      actor_id,
      COLLECT_LIST(ARRAY(
          film,
          votes,
          rating,
          film_id
        )
      ) AS films,
      AVG(rating) AS avg_rating,
      year
    FROM
       actor_films 
    WHERE
      year = 2018 
    GROUP BY
      actor,
      actor_id,
      year
  )
SELECT
  COALESCE(ly.actor, ty.actor) AS actor,
  COALESCE(ly.actor_id, ty.actor_id) AS actor_id,
  CASE
    WHEN ty.year IS NULL THEN ly.films
    WHEN ty.year IS NOT NULL
      AND ly.films IS NULL THEN ty.films
    WHEN ty.year IS NOT NULL
      AND ly.films IS NOT NULL THEN ty.films || ly.films
  END AS films,
  CASE
    WHEN ty.avg_rating IS NULL THEN ly.quality_class
    ELSE
      CASE
        WHEN ty.avg_rating > 8 THEN 'star'
        WHEN ty.avg_rating > 7 AND ty.avg_rating <= 8 THEN 'good'
        WHEN ty.avg_rating > 6 AND ty.avg_rating <= 7 THEN 'average'
        WHEN ty.avg_rating <= 6 THEN 'bad'
      END
  END AS quality_class,
  ty.year IS NOT NULL AS is_active,
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
    output_table_name: str = "actors"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_1")
        .getOrCreate()
    )
    output_df = job_1(spark_session, output_table_name)
    output_df.write.mode("overwrite").insertInto(output_table_name)
