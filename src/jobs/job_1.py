from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_1(output_table_name: str, input_table_name: str) -> str:
    query = f"""
    WITH last_year AS (
        SELECT
            *
        FROM
            actors
        WHERE
            current_year = 1913
    ),
    -- CTE for new data coming from actor_films for current year
    this_year AS (
        SELECT
            actor,
            actor_id, 
            ARRAY_AGG(ROW(
                year, 
                film, 
                votes, 
                rating, 
                film_id
            )) as films,
            AVG(rating) as avg_rating,
            MAX(year) as current_year
        FROM bootcamp.actor_films
        WHERE year = 1914
        GROUP BY
            actor, 
            actor_id, 
            year
    )
    SELECT
        COALESCE(ly.actor, ty.actor) AS actor,
        COALESCE(ly.actor_id, ty.actor_id) AS actor_id,
        CASE
            -- Actor data isn't present in incoming data for this year
            WHEN ty.current_year IS NULL THEN ly.films
            -- New actor data coming in 
            WHEN ty.current_year IS NOT NULL AND ly.films IS NULL THEN ty.films
            -- Existing actor with new data for this year
            WHEN ty.current_year IS NOT NULL AND ly.films IS NOT NULL THEN ty.films || ly.films
        END AS films,
        CASE 
            WHEN ty.avg_rating is NOT NULL THEN (
                CASE
                    WHEN ty.avg_rating > 8 THEN 'star'
                    WHEN ty.avg_rating > 7 THEN 'good'
                    WHEN ty.avg_rating > 6 THEN 'average'
                    ELSE 'bad'
                END 
            )
            ELSE ly.quality_class
        END as quality_class,
        ty.current_year IS NOT NULL AS is_active,
        COALESCE(ty.current_year, ly.current_year + 1) as current_year
    FROM  last_year ly
    FULL OUTER JOIN this_year ty 
        ON ly.actor_id = ty.actor_id
    """
    return query

def job_1(spark_session: SparkSession, output_table_name: str, input_table_name: str) -> Optional[DataFrame]:
  output_df = spark_session.table(output_table_name)
  output_df.createOrReplaceTempView(output_table_name)
  return spark_session.sql(query_1(output_table_name, input_table_name))

def main():
    output_table_name: str = "actors"
    input_table_name = "actor_films"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_1")
        .getOrCreate()
    )
    output_df = job_1(spark_session, output_table_name, input_table_name)
    output_df.write.mode("overwrite").insertInto(output_table_name)
