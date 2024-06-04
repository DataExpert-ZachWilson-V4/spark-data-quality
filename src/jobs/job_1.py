from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import udf

def query_1(actor_films_table: str, actors_table: str, current_year: int, last_year: int) -> str:
    """
    Constructs the SQL query to calculate actor film data and ratings.

    Args:
    - actor_films_table (str): Name of the table containing actor films data.
    - actors_table (str): Name of the table containing actors data.
    - current_year (int): Year for which to calculate film data.
    - last_year (int): Year to fallback for film data if current year is missing.

    Returns:
    - str: The SQL query string.
    """
    query = f"""
    WITH
        last_year AS (
            SELECT * FROM {actors_table}
            WHERE current_year = {last_year}
        ),
        this_year AS (
            SELECT
                actor,
                actor_id, 
                ARRAY_AGG(
                    DISTINCT ROW(
                        film, 
                        votes, 
                        rating, 
                        film_id,
                        year
                    )) as films,
                AVG(rating) as avg_rating,
                year as current_year
            FROM {actor_films_table}
            WHERE year = {current_year}
            GROUP BY actor, actor_id, year
        )
    SELECT 
        COALESCE(ly.actor, ty.actor) AS actor,
        COALESCE(ly.actor_id, ty.actor_id) AS actor_id,
        CASE
            WHEN ty.films IS NULL THEN ly.films
            WHEN ty.films IS NOT NULL AND ly.films IS NULL THEN ty.films
            WHEN ty.films IS NOT NULL AND ly.films IS NOT NULL THEN array_union(ty.films, ly.films)
        END AS films,
        CASE
            WHEN ty.avg_rating IS NOT NULL THEN
                CASE
                    WHEN ty.avg_rating > 8 THEN 'star'
                    WHEN ty.avg_rating > 7 THEN 'good'
                    WHEN ty.avg_rating > 6 THEN 'average'
                    ELSE 'bad'
                END
        END AS rating
    FROM last_year ly
    FULL OUTER JOIN this_year ty
    ON ly.actor_id = ty.actor_id
    """
    return query

def job_1(spark_session: SparkSession, actor_films_table: str, actors_table: str, current_year: int, last_year: int, output_table_name: str) -> Optional[DataFrame]:
    """
    Executes the job to calculate actor film data and ratings.

    Args:
    - spark_session (SparkSession): The Spark session object.
    - actor_films_table (str): Name of the table containing actor films data.
    - actors_table (str): Name of the table containing actors data.
    - current_year (int): Year for which to calculate film data.
    - last_year (int): Year to fallback for film data if current year is missing.
    - output_table_name (str): The name of the output table.

    Returns:
    - Optional[DataFrame]: The resulting DataFrame after the calculation.
    """
    # Execute the SQL query
    output_df = spark_session.sql(query_1(actor_films_table, actors_table, current_year, last_year))
    
    # Create or replace the temporary view with the output DataFrame
    output_df.createOrReplaceTempView(output_table_name)
    
    return output_df

def main():
    # Specify the input parameters
    actor_films_table: str = "actor_films"
    actors_table: str = "actors"
    current_year: int = 1914
    last_year: int = 1913
    output_table_name: str = "output_table"
    
    # Initialize Spark session
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_1")
        .getOrCreate()
    )
    
    # Execute the job
    output_df = job_1(spark_session, actor_films_table, actors_table, current_year, last_year, output_table_name)
    
    # Write the output DataFrame to the output table
    output_df.write.mode("overwrite").insertInto(output_table_name)

    # Stop the Spark session
    spark_session.stop()

if __name__ == "__main__":
    main()