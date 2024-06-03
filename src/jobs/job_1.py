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
    """Incremental insertion query for the actors change data capture table

    :param input_table_name: A table containing the actors and their films for a
                             given year
    :type input_table_name: str
    :param output_table_name: A table with actors cumulatively by year
    :type output_table_name: str
    :param last_year: The previous year used in the query to determine the current year
    and previous year, defaults to 1913
    :type last_year: int, optional
    :return: Query
    :rtype: str
    """
    this_year = last_year + 1
    query = f"""
    -- Incremental insertion query
    -- Insertion for the first year of the actors change data capture table knowing that:
    -- Min year 1914
    -- Max year 2021
    WITH last_year as (
            SELECT *
            FROM {output_table_name}
            WHERE current_year = {last_year}
        ),
        -- Aggregate data since unlike the NBA dataset which is on seasons
        -- This dataset is by films not by years so this CTE turns it into by years for
        -- each actor
        this_year as (
            SELECT actor,
                actor_id,
                COLLECT_LIST(STRUCT(film, votes, rating, film_id, year)) as films,
                -- Average the rating of all the movies for the actor for that year
                avg(rating) as av_rating,
                year
            FROM {input_table_name}
            WHERE year = {this_year}
            GROUP BY actor,
                actor_id,
                year
        )
    -- The main query
    SELECT COALESCE(l.actor, t.actor) AS actor,
        COALESCE(l.actor_id, t.actor_id) AS actor_id,
        -- Aggregating the films for the actor
        CASE
            -- If the actor is not in films this year persist the films from last year
            WHEN t.films IS NULL THEN l.films
            -- If the actor is in films for the first time this year and 
            -- therefore not in actors table last year then set this year's films 
            WHEN t.films IS NOT NULL
            AND l.films IS NULL THEN t.films
            -- If the actor is in films this year and last year persist the films 
            -- from both years
            WHEN t.films IS NOT NULL
            AND l.films IS NOT NULL THEN t.films || l.films
        END AS films,
        -- Set the quality class based on the average rating of the actor
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
        -- Determine if the actor is active based on if they are in films this year
        t.year IS NOT NULL as is_active,
        -- Set current year to the correct year based on if the actor is in films this year
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
    """Incremental insertion job for the actors change data capture table

    :param spark_session: Spark session
    :type spark_session: SparkSession
    :param dataframe: Input dataframe
    :type dataframe: DataFrame
    :param output_table_name: The actors change data capture table
    :type output_table_name: str
    :param input_table_name: The table containing the actors and their films for a given year
    :type input_table_name: str
    :param last_year: The previous year used in the query to determine the current year
                        and previous year, defaults to 1913
    :type last_year: int, optional
    :return: Spark dataframe
    :rtype: Optional[DataFrame]
    """
    # Setup the input table as a view
    dataframe.createOrReplaceTempView(input_table_name)

    # Not setting up the output table as a view here because it's a Spark Query
    # and insertion can be handled differently. But it does need to exist for this
    # query to work.
    return spark_session.sql(
        query_1(
            input_table_name=input_table_name,
            output_table_name=output_table_name,
            last_year=last_year,
        )
    )


def main():
    input_table_name: str = "actor_films"
    output_table_name: str = "actors"
    spark_session: SparkSession = (
        SparkSession.builder.master("local").appName("job_1").getOrCreate()
    )

    input_schema = StructType(
        [
            StructField("actor", StringType(), True),
            StructField("actor_id", StringType(), True),
            StructField("film", StringType(), True),
            StructField("year", IntegerType(), True),
            StructField("votes", IntegerType(), True),
            StructField("rating", FloatType(), True),
            StructField("film_id", StringType(), True),
        ]
    )

    films_schema = StructType(
        [
            StructField("film", StringType(), True),
            StructField("votes", LongType(), True),
            StructField("rating", LongType(), True),
            StructField("film_id", StringType(), True),
            StructField("year", LongType(), True),
        ]
    )
    output_schema = StructType(
        [
            StructField("actor", StringType(), False),
            StructField("actor_id", StringType(), False),
            StructField("films", ArrayType(films_schema), False),
            StructField("quality_class", StringType(), True),
            StructField("is_active", StringType(), True),
            StructField("current_year", IntegerType(), True),
        ]
    )
    output_df = spark_session.createDataFrame(data=[], schema=output_schema)
    # output_df.createOrReplaceTempView(output_table_name)
    input_df = spark_session.createDataFrame(data=[], schema=input_schema)
    # input_df.createOrReplaceTempView(input_table_name)
    output_df = job_1(
        spark_session=spark_session,
        dataframe=input_df,
        output_table_name=output_table_name,
        input_table_name=input_table_name,
        last_year=1913,
    )
    # output_df.write.mode("overwrite").insertInto(output_table_name)
