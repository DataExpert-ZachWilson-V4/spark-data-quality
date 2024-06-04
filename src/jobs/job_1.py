from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_1(output_table_name: str) -> str:

    query = f"""
    <YOUR QUERY HERE -using week 1 - script to load actors table>

    < previous_year CTE>
    WITH previous_year AS (
     SELECT * FROM {output_table_name}
     WHERE current_year = 1915
),

<create_films_cte is needed to prepare the films array to store this year's details>
 create_films_cte AS (
   SELECT 
      actor,
      actor_id,
     year,
<prepare the films array>
    ARRAY_AGG(
    ROW(
    film,
    votes,
    rating,
    film_id
    )
    ) AS films,    
< to handle quality_class bucketing for the main table>
    AVG(rating) AS avg_rating
FROM bootcamp.actor_films
WHERE year = 1916
GROUP BY actor, actor_id, year
    ),
    
    
<now prepare current_year_cte>
current_year AS (
SELECT actor,
      actor_id,
      year,
      films,
      CASE 
      WHEN avg_rating > 8 THEN 'star'
      WHEN avg_rating > 7 and avg_rating <= 8 THEN 'good'
      WHEN avg_rating > 6 and avg_rating <=7 THEN 'average'
      WHEN avg_rating <=6 THEN 'bad'
       END AS quality_class
 FROM create_films_cte
)

SELECT 
COALESCE(py.actor,cy.actor) AS actor,
COALESCE(py.actor_id,cy.actor_id) AS actor_id,
CASE
    WHEN cy.films IS NULL THEN py.films
    WHEN cy.films IS NOT NULL and py.films IS NULL THEN cy.films
    WHEN cy.films IS NOT NULL and py.films IS NOT NULL THEN cy.films || py.films
END AS films,
COALESCE(py.quality_class,cy.quality_class) AS quality_class,
(cy.actor_id IS NOT NULL) AS is_active,
cy.year AS current_year
FROM previous_year py
FULL OUTER JOIN current_year cy
ON py.actor = cy.actor

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
