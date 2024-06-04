from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
current_year = 1999

def query_1(output_table_name: str) -> str:
    query = f"""
    WITH 
    temp as (
    Select
        Actor,
        Actor_id,
        ARRAY_AGG(ROW(film, votes, rating, film_id, YEAR)) AS films,
        AVG(rating) AS avg_rating,
        YEAR
    FROM
      bootcamp.actor_films
    WHERE
        Year = {year}
    Group by
        Actor,
        actor_ID,
        Year
    ),

    This_year as(
        SELECT actor,
        actor_id,
        year,
        films,
        Case
        when avg_Rating > 8 THEN 'star'
        when avg_Rating > 7 then 'Good'
        when avg_Rating > 6 then 'average'
        else 'bad' end as quality_class
        FROM 
            temp

    ),

    last_year AS (
        SELECT 
            *
        FROM
            {output_table_name}
        WHERE
            current_year = {year-1}
        )
-- COALESCE all the values that are not changing to handle NULLS
    SELECT
    COALESCE(ly.actor, ty.actor) as actor,
    COALESCE(ly.actor_ID, ty.actor_ID) as actor_ID,
    CASE 
        WHEN ty.year IS NULL THEN ly.films 
        WHEN ty.year IS NOT NULL and ly.films IS NULL THEN ty.films

        WHEN ty.year IS NOT NULL and ly.films IS NOT NULL THEN ly.films || ty.films

        END as films,
    COALESCE(ty.quality_class, ly.quality_class) as quality_class,
    (ty.year IS NOT NULL) as is_active,
    ty.year as current_year
    FROM last_year ly
    FULL OUTER JOIN This_year ty
    ON ly.actor_ID = ty.actor_ID
    """
    return query

def job_1(spark_session: SparkSession, output_table_name: str, year: int) -> Optional[DataFrame]:
  output_df = spark_session.table(output_table_name)
  output_df.createOrReplaceTempView(output_table_name)
  return spark_session.sql(query_1(output_table_name))

def main():
    output_table_name: str = "mymah592.actors"
    year: int = 2000
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_1")
        .getOrCreate()
    )
    output_df = job_1(spark_session, output_table_name, year)
    output_df.write.mode("overwrite").insertInto(output_table_name)
