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
      current_year = 1928
  ),
  this_year AS (
    SELECT
      a.actor,
      a.actor_id,
      COLLECT_LIST(ARRAY(a.film, a.votes, a.rating, a.film_id)) AS films,
      avg(a.rating) AS avg_rating,
      avg(a.votes) AS avg_votes,
      a.YEAR
    FROM
      bootcamp.actor_films a
    WHERE
      a.YEAR = 1929
    GROUP BY
      a.actor,
      a.actor_id,
      a.YEAR
  )
SELECT
  coalesce(ls.actor, ts.actor) AS actor,
  coalesce(ls.actor_id, ts.actor_id) AS actor_id,
  CASE
    WHEN ts.year IS NULL THEN ls.films
    WHEN ts.year IS NOT NULL
    AND ls.films IS NULL THEN ts.films
    WHEN ts.year IS NOT NULL
    AND ls.films IS NOT NULL THEN ts.films || ls.films
  END AS films,
  coalesce(
    CASE
      WHEN avg_rating > 8.0 THEN 'star'
      WHEN avg_rating > 7.0
      AND avg_rating <= 8.0 THEN 'good'
      WHEN avg_rating > 6.0
      AND avg_rating <= 7.0 THEN 'average'
      WHEN avg_rating <= 6 THEN 'bad'
    END,
    ls.quality_class
  ) AS quality_class,
  ts.actor IS NOT NULL AS is_active,
  COALESCE(ts.year, ls.current_year + 1) current_year
FROM
  last_year ls
  FULL OUTER JOIN this_year ts ON ls.actor_id = ts.actor_id  
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
