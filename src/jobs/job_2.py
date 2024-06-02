from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_2(input_table_name: str, output_table_name: str, current_year: int) -> str:
    query = f"""
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
        COLLECT_LIST(ARRAY(film_id, film, year, votes, rating)) AS current_year_films
        FROM {input_table_name}
        WHERE year = {current_year}
        GROUP BY 1, 2, 3
    )
    SELECT
        COALESCE(ty.actor_id, ly.actor_id) AS actor_id,
        COALESCE(ty.actor, ly.actor) AS actor,
        CASE
            WHEN ty.year IS NULL THEN ly.films
            WHEN ty.year IS NOT NULL AND ly.films IS NULL
            THEN ty.current_year_films
            WHEN ty.year IS NOT NULL AND ly.films IS NOT NULL
            THEN ty.current_year_films || ly.films
        ELSE NULL END AS films,
        CASE
            WHEN ty.actor_id IS NOT NULL
            THEN
                CASE
                    WHEN ty.avg_rating <= 6 THEN 'bad'
                    WHEN ty.avg_rating <= 7 THEN 'average'
                    WHEN ty.avg_rating <= 8 THEN 'good'
                    WHEN ty.avg_rating > 8 THEN 'star'
                ELSE NULL END
        ELSE NULL END AS quality_class,
        ty.year IS NOT NULL AS is_active,
        COALESCE(ty.year, ly.current_year + 1) AS current_year
    FROM last_year ly
    FULL OUTER JOIN this_year ty
        ON ly.actor_id = ty.actor_id
    """
    return query

def job_2(spark_session: SparkSession, input_table_name: str, output_table_name: str, load_year: int) -> Optional[DataFrame]:
  output_df = spark_session.table(output_table_name)
  output_df.createOrReplaceTempView(output_table_name)
  return spark_session.sql(query_2(input_table_name, output_table_name, load_year))

def main():
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_2")
        .getOrCreate()
    )
    output_df = job_2(spark_session, "actor_films", "actors", 1914)
    output_df.write.mode("overwrite").insertInto("actors")
