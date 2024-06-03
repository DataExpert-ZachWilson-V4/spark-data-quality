from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


def query_1(output_table_name: str) -> str:
    query = f"""
    WITH cy AS (
        SELECT 
            actor,
            actor_id,
            year,
            COLLECT_LIST(NAMED_STRUCT('film', film, 'film_id', film_id, 'year', year, 'votes', votes, 'rating', rating)) AS films,
            CASE
                WHEN AVG(rating) > 8 THEN 'star'
                WHEN AVG(rating) > 7 THEN 'good'
                WHEN AVG(rating) > 6 THEN 'average'
                ELSE 'bad'
            END AS quality_class
        FROM actor_films
        WHERE year = 1914
        GROUP BY actor_id, actor, year
    ),
    ly AS (
        SELECT 
            actor,
            actor_id,
            films,
            quality_class,
            is_active,
            current_year
        FROM actors
        WHERE current_year = 1913
    )
    SELECT 
        COALESCE(cy.actor, ly.actor) AS actor,
        COALESCE(cy.actor_id, ly.actor_id) AS actor_id,
        CASE
            WHEN cy.films IS NULL THEN ly.films
            WHEN ly.films IS NULL THEN cy.films
            ELSE CONCAT(cy.films, ly.films)
        END AS films,
        COALESCE(cy.quality_class, ly.quality_class) AS quality_class,
        CASE
            WHEN cy.films IS NULL THEN FALSE
            ELSE TRUE
        END AS is_active,
        COALESCE(cy.year, ly.current_year + 1) AS current_year
    FROM cy
    FULL OUTER JOIN ly ON cy.actor_id = ly.actor_id
    """
    return query


def job_1(spark_session: SparkSession, output_table_name: str) -> Optional[DataFrame]:
    output_df = spark_session.table(output_table_name)
    output_df.createOrReplaceTempView(output_table_name)
    return spark_session.sql(query_1(output_table_name))


def main():
    output_table_name: str = "rudnickipm91007.actors"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_1")
        .getOrCreate()
    )
    output_df = job_1(spark_session, output_table_name)
    output_df.write.mode("overwrite").insertInto(output_table_name)
