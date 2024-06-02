from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


# The query that generated table called siawayforward.actors from assignment # 1
def query_2(
      input_table_name: str,
      output_table_name: str,
      current_year: int
    ) -> str:
    query = f"""
    -- Increment to 1914
    WITH last_year AS (
        SELECT *
        FROM {output_table_name}
        WHERE current_year = {current_year - 1}

    ), this_year AS (
        SELECT
        actor_id,
        actor,
        year,
        -- we need this to assign quality_rating category
        AVG(rating) AS avg_rating,
        -- actors can have more than one film a year
        COLLECT_LIST(ARRAY(film_id, film, year, votes, rating)) AS current_year_films
        FROM {input_table_name}
        WHERE year = {current_year}
        GROUP BY 1, 2, 3

    )
    SELECT
        COALESCE(ty.actor_id, ly.actor_id) AS actor_id,
        COALESCE(ty.actor, ly.actor) AS actor,
        CASE
            -- actor has no new releases this year
            WHEN ty.year IS NULL THEN ly.films
            -- actor is having the first year of releases
            WHEN ty.year IS NOT NULL AND ly.films IS NULL
            THEN ty.current_year_films
            -- actor had prior releases and new ones this year
            WHEN ty.year IS NOT NULL AND ly.films IS NOT NULL
            THEN ty.current_year_films || ly.films
        ELSE NULL END AS films,
        CASE
            -- actor is having releases in the current year
            WHEN ty.actor_id IS NOT NULL
            THEN
                -- get the class based on average rating that year
                CASE
                    WHEN ty.avg_rating <= 6 THEN 'bad'
                    WHEN ty.avg_rating <= 7 THEN 'average'
                    WHEN ty.avg_rating <= 8 THEN 'good'
                    WHEN ty.avg_rating > 8 THEN 'star'
                ELSE NULL END
        -- no releases mean no ratings category
        ELSE NULL END AS quality_class,
        ty.year IS NOT NULL AS is_active,
        COALESCE(ty.year, ly.current_year + 1) AS current_year
    FROM last_year ly
    FULL OUTER JOIN this_year ty
        ON ly.actor_id = ty.actor_id
    """
    return query

def job_2(
      spark_session: SparkSession,
      input_table_name: str,
      output_table_name: str,
      load_year: int
    ) -> Optional[DataFrame]:
  output_df = spark_session.table(output_table_name)
  output_df.createOrReplaceTempView(output_table_name)
  return spark_session.sql(query_2(input_table_name, output_table_name, load_year))

def main():
    input_table_name: str = "bootcamp.actor_films"
    output_table_name: str = "siawayforward.actors"
    load_year: int = 1914
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_2")
        .getOrCreate()
    )
    output_df = job_2(spark_session, input_table_name, output_table_name, load_year)
    output_df.write.mode("overwrite").insertInto(output_table_name)
