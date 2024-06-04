from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_1(input_table_name: str, output_table_name: str) -> str:
    query = f"""
    WITH last_year AS (
    SELECT * FROM {output_table_name} 
    WHERE current_year = 2000
    ),
    this_year AS (
        SELECT
            actor,
            actor_id,
            AVG(rating) AS avg_rating,
            COLLECT_LIST(NAMED_STRUCT("year", year, "film", film, "votes", votes, "rating", rating, "film_id", film_id)) AS films,
            year
        FROM
            {input_table_name}
        WHERE year = 2001
        GROUP BY actor, actor_id, year 
    )
    SELECT
        COALESCE(ty.actor, ly.actor) as actor,
        COALESCE(ty.actor_id, ly.actor_id) as actor_id,
        CASE
            WHEN ty.year IS NULL THEN ly.films
            WHEN ty.year IS NOT NULL AND ly.films IS NOT NULL THEN ty.films || ly.films
            WHEN ty.year IS NOT NULL AND ly.films IS NULL THEN ty.films
        END as films,
        CASE
            WHEN ty.avg_rating > 8 THEN 'star'
            WHEN ty.avg_rating > 7 THEN 'good'
            WHEN ty.avg_rating > 6 THEN 'average'
            ELSE 'bad'
        END as quality_class,
        ty.year IS NOT NULL AS is_active,
        COALESCE(ty.year, ly.current_year + 1) as current_year
    FROM
        last_year ly FULL OUTER JOIN this_year ty ON ty.actor_id = ly.actor_id
    """
    return query

def job_1(spark_session: SparkSession, input_table_name: str, output_table_name: str) -> Optional[DataFrame]:
  input_df = spark_session.table(input_table_name)
  input_df.createOrReplaceTempView(input_table_name)
  output_df = spark_session.table(output_table_name)
  output_df.createOrReplaceTempView(output_table_name)
  return spark_session.sql(query_1(input_table_name, output_table_name))

def main():
    input_table_name: str = "bootcamp.actor_films"
    output_table_name: str = "williampbassett.actors"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_1")
        .getOrCreate()
    )
    output_df = job_1(spark_session, input_table_name, output_table_name)
    output_df.write.mode("overwrite").insertInto(output_table_name)


