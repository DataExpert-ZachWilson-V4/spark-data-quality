from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


def query_1(output_table_name: str, current_year: int) -> str:
    query = f"""
        -- CTE to hold last years data
        WITH last_year AS (
            SELECT
                *
            FROM
                {output_table_name}
            WHERE
                current_year = {current_year}
        ),
        -- CTE to hold this years data
        current_year AS (
            SELECT
                actor,
                actor_id,
                ARRAY_AGG(STRUCT(film, votes, rating, film_id)) AS films, -- Array of films attributes
                CASE -- Categorical information for quality class
                    WHEN AVG(rating) > 8 THEN 'star'
                    WHEN AVG(rating) > 7 THEN 'good'
                    WHEN AVG(rating) > 6 THEN 'average'
                    ELSE 'bad'
                END AS quality_class,
                actor_id IS NOT NULL AS is_active,
                year AS current_year
            FROM
                actor_films
            WHERE
                year = {current_year + 1}
            GROUP BY 
                actor,
                actor_id,
                year
        )
        -- Generate final data
        SELECT
            COALESCE(ly.actor, cy.actor) AS actor,
            COALESCE(ly.actor_id, cy.actor_id) AS actor_id,
            CASE
                WHEN ly.films IS NULL THEN cy.films
                WHEN cy.films IS NOT NULL THEN cy.films || ly.films
                ELSE ly.films
            END AS films,
            COALESCE(ly.quality_class, cy.quality_class) AS quality_class,
            cy.is_active IS NOT NULL AS is_active,
            COALESCE(cy.current_year, ly.current_year+1) AS current_year
        FROM
            last_year ly 
        FULL OUTER JOIN current_year cy 
        ON (ly.actor_id = cy.actor_id)
    """
    return query


def job_1(spark_session: SparkSession, output_table_name: str, current_year: int) -> Optional[DataFrame]:
    output_df = spark_session.table(output_table_name)
    output_df.createOrReplaceTempView(output_table_name)
    return spark_session.sql(query_1(output_table_name, current_year))


def main():
    output_table_name: str = "actors"
    current_year = 2010
    spark_session: SparkSession = (
        SparkSession.builder.master("local").appName("job_1").getOrCreate()
    )
    output_df = job_1(spark_session, output_table_name, current_year)
    output_df.write.mode("overwrite").insertInto(output_table_name)
