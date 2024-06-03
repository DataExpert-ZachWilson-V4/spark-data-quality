from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_1(output_table_name: str) -> str:
    query = f"""
    WITH 
        previous_year AS (
            SELECT * 
            FROM {output_table_name}
            WHERE current_year = 2013
        ),

        current_year AS (
            SELECT actor, 
                actor_id, 
                year, 
                COLLECT_LIST(STRUCT(year, film, votes, rating, film_id)) AS films,
                AVG(rating) AS avg_rating
            FROM  actor_films
            WHERE year = 2014
            GROUP BY actor, actor_id, year
        )


    SELECT COALESCE(py.actor, cy.actor) AS actor,
        COALESCE(py.actor_id, cy.actor_id) AS actor_id,
        CASE
            WHEN cy.year IS NULL THEN py.films
            WHEN cy.year IS NOT NULL 
                AND py.films IS NULL 
                THEN cy.films 
            WHEN cy.year IS NOT NULL 
                AND py.films IS NOT NULL 
                THEN cy.films || py.films 
        END AS films,
        CASE 
            WHEN cy.avg_rating IS NULL THEN py.quality_class  
            WHEN cy.avg_rating > 8 THEN 'star'
            WHEN cy.avg_rating > 7 AND cy.avg_rating <= 8 THEN 'good'
            WHEN cy.avg_rating > 6 AND cy.avg_rating <= 7 THEN 'average'
            WHEN cy.avg_rating <= 6 THEN 'bad'
        END AS quality_class,
        cy.year IS NOT NULL AS is_active,
        COALESCE(cy.year, py.current_year + 1) as current_year
    FROM previous_year py
    FULL OUTER JOIN current_year cy 
        ON py.actor_id = cy.actor_id
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
