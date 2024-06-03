from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    IntegerType,
    ArrayType,
    FloatType,
)


def query_1(
    input_table_name: str, output_table_name: str, last_year: int = 1913
) -> str:
    this_year = last_year + 1
    query = f"""
    WITH last_year as (
            SELECT *
            FROM {output_table_name}
            WHERE current_year = {last_year}
        ),
        this_year as (
            SELECT actor,
                actor_id,
                COLLECT_LIST(STRUCT(film, votes, rating, film_id, year)) as films,
                avg(rating) as av_rating,
                year
            FROM {input_table_name}
            WHERE year = {this_year}
            GROUP BY actor,
                actor_id,
                year
        )
    SELECT COALESCE(l.actor, t.actor) AS actor,
        COALESCE(l.actor_id, t.actor_id) AS actor_id,
        CASE
            WHEN t.films IS NULL THEN l.films
            WHEN t.films IS NOT NULL
            AND l.films IS NULL THEN t.films
            WHEN t.films IS NOT NULL
            AND l.films IS NOT NULL THEN t.films || l.films
        END AS films,
        CASE
            WHEN t.av_rating IS NULL then l.quality_class
            WHEN t.av_rating IS NOT NULL
            AND t.av_rating > 8 THEN 'star'
            WHEN t.av_rating IS NOT NULL
            AND t.av_rating > 7
            AND t.av_rating <= 8 THEN 'good'
            WHEN t.av_rating IS NOT NULL
            AND t.av_rating > 6
            AND t.av_rating <= 7 THEN 'average'
            WHEN t.av_rating IS NOT NULL
            AND t.av_rating <= 6 THEN 'bad'
        END AS quality_class,
        t.year IS NOT NULL as is_active,
        COALESCE(t.year, l.current_year + 1) AS current_year
    FROM last_year AS l
        FULL OUTER JOIN this_year AS t ON l.actor_id = t.actor_id
    """
    return query


def job_1(
    spark_session: SparkSession,
    dataframe: DataFrame,
    output_table_name: str,
    input_table_name: str,
    last_year: int = 1914,
) -> Optional[DataFrame]:
    # Setup the input table as a view
    dataframe.createOrReplaceTempView(input_table_name)
    # Not setting up the output table as a view here because it's a Spark Query
    return spark_session.sql(
        query_1(
            input_table_name=input_table_name,
            output_table_name=output_table_name,
            last_year=last_year,
        )
    )
