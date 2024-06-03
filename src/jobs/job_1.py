#importing libraries
from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_1(output_table_name: str, input_table_name: str, this_year: str) -> str:
    previous_year = str(int(this_year)-1)

    query = f"""
        WITH last_year 
        AS 
        (
            SELECT
             * 
             FROM {output_table_name} 
            WHERE current_year = {previous_year}
        ),
    this_year AS 
        (
            SELECT
                actor,
                actor_id,
                year AS current_year,
                COLLECT_LIST(NAMED_STRUCT("film", film, "votes", votes, "rating", rating, "film_id", film_id)) AS films,
                AVG(rating) AS rating
            FROM {input_table_name}
            WHERE year = {this_year}
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

def job_1(spark_session: SparkSession,output_df: DataFrame, output_table_name: str,input_df: DataFrame, input_table_name: str, this_year: str ) -> Optional[DataFrame]:
  output_df.createOrReplaceTempView(output_table_name)
  input_df.createOrReplaceTempView(input_table_name)
  return spark_session.sql(query_1(output_table_name, input_table_name,this_year))

def main():
    output_table_name: str = "actors"
    input_table_name: str = "actor_films"
    this_year: str = "1929"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_1")
        .getOrCreate()
    )
    output_df = spark_session.table(output_table_name)
    input_df = spark_session.table(input_table_name)
    output = job_1(spark_session,output_df,output_table_name, input_df,input_table_name, this_year)
    output.write.mode("overwrite").insertInto(output_table_name)
