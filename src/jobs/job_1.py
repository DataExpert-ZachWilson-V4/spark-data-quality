from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_1(output_table_name: str) -> str:
    query = f"""
    WITH last_year AS (
        SELECT
            actor,
            actor_id,
            films,
            current_year,
            quality_class
        FROM
            mposada.actors
        WHERE
            current_year = 1913
    ),
    this_year AS (
        SELECT
            actor,
            actor_id,
            year,
            COLLECT_LIST(STRUCT(film, votes, rating, film_id)) AS films,  -- Aggregate film details into an array
            AVG(rating) AS avg_rating  -- Calculate average rating
        FROM
            bootcamp.actor_films
        WHERE
            year = 1914  -- this is the first year we have data for
        GROUP BY
            actor, actor_id, year
    )
    SELECT
        COALESCE(ly.actor, ty.actor) AS actor,  -- Use actor name from either last year or this year
        COALESCE(ly.actor_id, ty.actor_id) AS actor_id,  -- Use actor ID from either last year or this year
        CASE
            WHEN ty.year IS NULL THEN ly.films  -- If no films this year, use last year's films
            WHEN ty.year IS NOT NULL AND ly.films IS NULL THEN ty.films  -- If new films but no films last year, use this year's films
            WHEN
                ty.year IS NOT NULL AND ly.films IS NOT NULL
                THEN ARRAY_UNION(ty.films, ly.films)  -- Concatenate films from both years if available
        END AS films,
        CASE
            WHEN ty.year IS NULL THEN ly.quality_class  -- If no data for this year, use last year's quality class
            WHEN ty.year IS NOT NULL THEN
                CASE  -- Determine quality class based on average rating
                    WHEN ty.avg_rating > 8 THEN 'star'
                    WHEN ty.avg_rating > 7 THEN 'good'
                    WHEN ty.avg_rating > 6 THEN 'average'
                    ELSE 'bad'
                END
        END AS quality_class,
        ty.year IS NOT NULL AS is_active,  -- Actor is active if there's data for this year
        COALESCE(ty.year, ly.current_year + 1) AS current_year  -- Use this year or increment last year's year by 1
    FROM last_year AS ly
    FULL OUTER JOIN this_year AS ty ON ly.actor_id = ty.actor_id  -- Join on actor ID

    """
    return query

def job_1(spark_session: SparkSession, output_table_name: str) -> Optional[DataFrame]:
  output_df = spark_session.table(output_table_name)
  output_df.createOrReplaceTempView(output_table_name)
  return spark_session.sql(query_1(output_table_name))

def main():
    output_table_name: str = "mposada.actors"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_1")
        .getOrCreate()
    )
    output_df = job_1(spark_session, output_table_name)
    output_df.write.mode("overwrite").insertInto(output_table_name)
