from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_1(output_table_name: str) -> str:
    query = f"""

    -- 1914 is the first year with films
    -- select * from bootcamp.actor_films where year = 1914
    -- select max(current_year) from xeno.actors 

    with prev_year as (select * from actors where current_year = 1913),
    cur_year as (select * from bootcamp.actor_films where year = 1914)

    -- I handle when an actor/actress has multiple films in the same year by using group by

    select 
    -- I have used COALESCE to have a value as it is a FULL OUTER JOIN
    COALESCE(py.actor,cy.actor) as actor,
    COALESCE(py.actor_id,cy.actor_id) as actor_id,

    -- flatten the array when there are actors with multiple movies in the same year
    flatten(array_agg(
    CASE
    -- actor has not done any new movie
    WHEN cy.film IS NULL THEN py.films
    -- new actor entered the market
    WHEN cy.film IS NOT NULL AND py.films IS NULL THEN ARRAY[
        ROW(
            cy.film,
            cy.votes,
            cy.rating,
            cy.film_id
        )
        ]

    -- actor already existing
    WHEN cy.film IS NOT NULL AND py.films IS NOT NULL THEN 
    ARRAY[
        ROW(
            cy.film,
            cy.votes,
            cy.rating,
        cy.film_id
        )
        ] || py.films
        
    END)) as films,

    -- handle the rating
    CASE
        WHEN AVG(cy.rating) > 8 THEN 'star'
        WHEN AVG(cy.rating) > 7 AND AVG(cy.rating) <= 8 THEN 'good'
        WHEN AVG(cy.rating) > 6 AND AVG(cy.rating) <= 7 THEN 'average'
        WHEN AVG(cy.rating) <= 6 THEN 'bad'
    END AS quality_class,

    -- if there are any films, I consider the actor as active
    CASE
    WHEN COUNT(cy.film) > 0 THEN true else false
    END as is_active,

    -- since the year can be null, I pick one either from the previous or the current
    COALESCE(py.current_year+1,cy.year) as current_year

    from prev_year py FULL OUTER JOIN cur_year cy on py.actor_id = cy.actor_id

    group by py.actor, cy.actor, py.actor_id, cy.actor_id, py.current_year, cy.year

    """
    return query

def job_1(spark_session: SparkSession, output_table_name: str) -> Optional[DataFrame]:
  output_df = spark_session.table(output_table_name)
  output_df.createOrReplaceTempView(output_table_name)
  return spark_session.sql(query_1(output_table_name))

def main():
    output_table_name: str = "actors_spark"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_1")
        .getOrCreate()
    )
    output_df = job_1(spark_session, output_table_name)
    output_df.write.mode("overwrite").insertInto(output_table_name)
