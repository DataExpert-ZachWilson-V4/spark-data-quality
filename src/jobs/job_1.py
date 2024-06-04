from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_1(output_table_name: str) -> str:
    query = f"""
        WITH last_year AS (
            SELECT * FROM {output_table_name}
            WHERE current_year = 1914
            UNION ALL
            SELECT NULL, NULL, NULL, NULL, NULL, NULL
            WHERE NOT EXISTS (SELECT * FROM {output_table_name} WHERE current_year = 1914)
        ),
        this_year AS (
            SELECT
                a.actor,
                a.actor_id,
                year AS current_year,
                true AS is_active,
                collect_list(named_struct('year', a.year, 'film', a.film, 'votes', a.votes, 'rating', a.rating, 'film_id', a.film_id)) AS films,
                AVG(rating) AS avg_rating
            FROM
                actor_films AS a
            WHERE a.year = 1915
            GROUP BY
                
                a.actor_id, a.actor, year
        )
        SELECT
            COALESCE(ly.actor, ty.actor) AS actor,
            COALESCE(ly.actor_id, ty.actor_id) AS actor_id,
            CASE
                WHEN ty.films IS null THEN ly.films
                WHEN
                    ty.films IS NOT null
                    AND ly.films IS null THEN ty.films
                WHEN
                    ty.films IS NOT null
                    AND ly.films IS NOT null THEN ty.films || ly.films
            END AS films,
            CASE
                WHEN ty.films IS null THEN ly.quality_class
                WHEN ty.avg_rating > 8 THEN 'star'
                WHEN ty.avg_rating > 7 AND ty.avg_rating <= 8 THEN 'good'
                WHEN ty.avg_rating > 6 AND ty.avg_rating <= 7 THEN 'average'
                WHEN ty.avg_rating <= 6 THEN 'bad'
            END AS quality_class,
            ty.current_year IS NOT null AS is_active,
            COALESCE(ty.current_year, ly.current_year + 1) AS current_year
        FROM last_year AS ly
        FULL OUTER JOIN this_year AS ty
            ON ly.actor_id = ty.actor_id
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
    output_df.write.mode("overwrite").insertInto(output_table_name)