from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_1(output_table_name: str) -> str:
    query = f"""
        WITH last_year 
        AS 
        (
            SELECT
             * 
             FROM {output_table_name} 
            WHERE current_year = 1928
        ),
    this_year AS 
        (
            SELECT
                actor,
                actor_id,
                year AS current_year,
                COLLECT_LIST(STRUCT("film", film, "votes", votes, "rating", rating, "film_id", film_id)) AS films,
                AVG(rating) AS rating
            FROM actor_films
            WHERE year = 1929
            GROUP BY actor,
                     actor_id,
                     year
        )
    SELECT
    COALESCE(ly.actor, ty.actor) AS actor,
    COALESCE(ly.actor_id, ty.actor_id) AS actor_id,
    CASE
        WHEN ty.current_year IS NULL 
        THEN ly.films
        WHEN ty.current_year IS NOT NULL AND ly.films IS NULL 
        THEN ty.films
        WHEN ty.current_year IS NOT NULL AND ly.films IS NOT NULL 
        THEN ty.films || ly.films
    END AS films,
    CASE
        WHEN ty.current_year IS NULL THEN ly.quality_class
        WHEN ty.current_year IS NOT NULL THEN
        CASE
            WHEN ty.rating > 8.0 
            THEN 'star'
            WHEN ty.rating > 7.0 AND ty.rating <= 8.0 
            THEN 'good'
            WHEN ty.rating > 6.0 AND ty.rating <= 7.0 
            THEN 'average'
            ELSE 'bad'
        END
    END AS quality_class,
    ty.actor IS NOT NULL AS is_active,
    COALESCE(ly.current_year + 1, ty.current_year) AS current_year
    FROM 
    last_year ly
    FULL OUTER JOIN this_year ty
    ON ly.actor_id = ty.actor_id AND ly.actor = ty.actor
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
