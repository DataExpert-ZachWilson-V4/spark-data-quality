from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_1(output_table_name: str) -> str:
    query = f"""
    WITH last_year AS (
    SELECT 
        * 
    FROM {output_table_name}
    WHERE current_year = 1999
    ), 
    this_year AS (
    SELECT
        actor,
        actor_id,
        ARRAY_AGG(ROW(film, votes, rating, film_id, year)) AS films,
        AVG(rating) AS avg_rating,
        MAX(year) AS year
    FROM actor_films
    WHERE year = 2000
    GROUP BY actor, actor_id
    )
    SELECT 
        COALESCE(ly.actor, ty.actor) as actor,
        COALESCE(ly.actor_id, ty.actor_id) as actor_id,
        CASE
            WHEN ty.films IS NULL THEN ly.films
            WHEN ty.films IS NOT NULL and ly.films IS NULL THEN ty.films
            WHEN ty.films IS NOT NULL and ly.films IS NOT NULL THEN ty.films || ly.films 
        END as films,
        CASE
            WHEN ty.avg_rating > 8 THEN 'star'
            WHEN ty.avg_rating > 7 and ty.avg_rating <= 8 THEN 'good'
            WHEN ty.avg_rating > 6 and ty.avg_rating <= 7 THEN 'average'
            ELSE 'bad'
        END as quality_class,
        ty.year IS NOT NULL as is_active,
        COALESCE(ty.year, ly.current_year + 1) as current_year
    FROM last_year ly
    FULL OUTER JOIN this_year ty
    on ly.actor_id = ty.actor_id

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

