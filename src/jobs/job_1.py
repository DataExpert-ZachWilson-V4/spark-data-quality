from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, IntegerType, DoubleType, BooleanType


def query_1(input_table_name_actors: str, input_table_name_films: str) -> str:
    query = f"""
    -- Query loads and inserts data on actors grouped by the film year
    WITH last_year_films AS (
        -- Gets the data of actors from the previous years stored in local actors table
        SELECT   
            actor,
            actor_id,
            films,
            average_rating,
            quality_class,
            is_active,
            current_year
        FROM {input_table_name_actors}
        WHERE current_year = 2011 -- previous reporting year
    ),
    this_year_films AS (
        -- Selects actors, their IDs, film year, average ratings, and aggregates films into arrays
        SELECT
            actor,
            actor_id,
            year,
            AVG(rating) AS average_rating, -- generates the average rating across all films for the specific actor that year
            COLLECT_LIST(
                STRUCT(
                    ts.film,
                    CAST(ts.year AS INT) AS year,
                    CAST(ts.votes AS INT) AS votes,
                    ts.rating,
                    ts.film_id
                )
            ) AS films
        FROM {input_table_name_films} ts
        WHERE year = 2012 -- current reporting year
        GROUP BY ts.actor, ts.actor_id, ts.year -- groups by the actor, its id and the year of the film
    )
    SELECT
        COALESCE(ls.actor, ts.actor) AS actor,  -- Coalesce actor to handle NULL values
        COALESCE(ls.actor_id, ts.actor_id) AS actor_id,  -- Coalesce actor to handle NULL values
        CASE
            WHEN ts.year IS NULL THEN ls.films  -- Use last year's films if no films for the current year
            WHEN ts.year IS NOT NULL AND ls.films IS NULL THEN ts.films  -- Use current year's films if no last year's films
            WHEN ts.year IS NOT NULL AND ls.films IS NOT NULL THEN array_distinct(array_union(ts.films, ls.films))      -- Concatenate films if both years have films
        END AS films,
        ts.average_rating,
        CASE
            WHEN ts.average_rating > 8 THEN 'star'
            WHEN ts.average_rating > 7 AND ts.average_rating <= 8 THEN 'good'
            WHEN ts.average_rating > 6 AND ts.average_rating <= 7 THEN 'average'
            WHEN ts.average_rating <= 6 THEN 'bad'
        END AS quality_class, -- defines the quality class or grading based on the average rating
        ts.year IS NOT NULL AS is_active,  -- Indicates if the actor is active in the current year
        COALESCE(ts.year, ls.current_year + 1) AS current_year  -- Use current year or next year if current year is NULL
    FROM
        last_year_films ls
        FULL OUTER JOIN this_year_films ts ON ls.actor_id = ts.actor_id  -- Full outer join to combine data from both years
    """
    return query

def job_1(spark_session: SparkSession, input_actors_df: DataFrame,input_films_df: DataFrame ):
    input_table_name_actors = "actors"
    input_table_name_films = "actor_films"
    input_actors_df.createOrReplaceTempView(input_table_name_actors)
    input_films_df.createOrReplaceTempView(input_table_name_films)
    input_films_df = spark_session.sql(query_1(input_table_name_actors, input_table_name_films)) 
    return input_films_df

def main():
    output_table_name: str = "amaliah21315.actors"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_1")
        .getOrCreate()
    )
    output_df = job_1(spark_session, spark_session.table(output_table_name))
    output_df.write.mode("overwrite").insertInto(output_table_name)