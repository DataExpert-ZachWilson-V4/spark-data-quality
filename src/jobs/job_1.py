from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_1(output_table_name: str) -> str:
    query = f"""
WITH last_year AS (
    SELECT * FROM rajkgupta091041107.actors
    WHERE current_year = 2019
),
this_year AS (
    SELECT 
        actor,
        actor_id,
        year,
        COLLECT_LIST(NAMED_STRUCT("film", film, "votes", votes, "rating", rating, "film_id", film_id)) AS films,
        AVG(rating) AS avg_rating 
    FROM bootcamp.actor_films
    WHERE year = 2020
    GROUP BY actor, actor_id, year
)
SELECT 
    COALESCE(ly.actor, ty.actor) AS actor,          
    COALESCE(ly.actor_id, ty.actor_id) AS actor_id,              
    CASE
        WHEN ty.films IS NULL THEN ly.films
        WHEN ly.films IS NULL THEN ty.films
        WHEN ty.films IS NOT NULL AND ly.films IS NOT NULL THEN CONCAT(ty.films, ly.films)
    END AS films,
    CASE 
        WHEN avg_rating IS NULL THEN NULL
        WHEN avg_rating > 8 THEN 'star'
        WHEN avg_rating > 7 AND avg_rating <= 8 THEN 'good'
        WHEN avg_rating > 6 AND avg_rating <= 7 THEN 'average'
        ELSE 'bad'
    END AS quality_class,
    CASE 
        WHEN ty.actor_id IS NOT NULL THEN TRUE 
        ELSE FALSE
    END AS is_active,
    COALESCE(ty.year, ly.current_year + 1) AS current_year
FROM last_year ly
FULL OUTER JOIN this_year ty ON ly.actor_id = ty.actor_id

    """
    return query

def job_1(spark_session: SparkSession, output_table_name: str) -> Optional[DataFrame]:
  output_df = spark_session.table(output_table_name)
  output_df.createOrReplaceTempView(output_table_name)
  return spark_session.sql(query_1(output_table_name))

def main():
    output_table_name: str = "rajkgupta091041107.actors"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_1")
        .getOrCreate()
    )
    output_df = job_1(spark_session, output_table_name)
    output_df.write.mode("overwrite").insertInto(output_table_name)
