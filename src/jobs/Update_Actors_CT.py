from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def Actors_CT(output_table_name: str, year: int) -> str:
    query = f"""    
WITH last_year AS (
    SELECT * FROM {output_table_name}
    WHERE current_year = {year-1}
),
this_year_stage AS (
    SELECT
        actor,
        actor_id,
        ARRAY_AGG(ROW(
            film,
            votes,
            rating,
            film_id
        )) AS films,
        CASE 
            WHEN AVG(rating) > 8 THEN 'star'
            WHEN AVG(rating) > 7 AND AVG(rating) <= 8 THEN 'good'
            WHEN AVG(rating) > 6 AND AVG(rating) <= 7 THEN 'average'
            WHEN AVG(rating) <= 6 THEN 'bad'
            ELSE NULL
        END AS quality_class,
        year as current_year
    FROM bootcamp.actor_films
    WHERE year = {year}
    GROUP BY actor, actor_id, year
)
SELECT
    COALESCE(ly.actor, ty.actor) AS actor,
    COALESCE(ly.actor_id, ty.actor_id) AS actor_id,
    CASE
        WHEN ly.films IS NOT NULL AND ty.films IS NOT NULL THEN ly.films || ty.films
        WHEN ty.films IS NULL THEN ly.films
        WHEN ly.films IS NULL THEN ty.films
        ELSE null
    END AS films,
    COALESCE(ty.quality_class, ly.quality_class) AS quality_class,
    (ty.actor_id IS NOT NULL) AS is_active,
    COALESCE(ty.current_year, ly.current_year + 1) AS current_year
FROM
    last_year ly
    FULL OUTER JOIN this_year_stage ty
    ON ly.actor_id = ty.actor_id
    """
    return query

def Update_Actors_CT(spark_session: SparkSession, output_table_name: str, year: int) -> Optional[DataFrame]:
  output_df = spark_session.table(output_table_name)
  output_df.createOrReplaceTempView(output_table_name)
  return spark_session.sql(Actors_CT(output_table_name, year))

def main():
    output_table_name: str = "danieldavid.actors"
    year: int = 1915
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("Update_Actors_CT")
        .getOrCreate()
    )
    output_df = Update_Actors_CT(spark_session, output_table_name, year)
    output_df.write.mode("overwrite").insertInto(output_table_name)
