from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_1(input_table_name: str, base_table: str, current_year: int) -> str:
    query = f"""
            WITH
        last_year AS(
            -- Fetch all actor info for last year
            SELECT
            *
            FROM
            {input_table_name}
            WHERE
            current_year = {current_year - 1}
        ),
        this_year AS(
            -- Fetch all actor info for this year
            SELECT
            actor,
            actor_id,
            ARRAY_AGG(
                (
                film,
                votes,
                rating,
                film_id
                )
            ) AS films,
            AVG(rating) AS avg_rating,
            MAX(year) AS current_year
            FROM
            {base_table}
            WHERE
            year = {current_year}
            GROUP BY
            actor,
            actor_id
        )
        SELECT
            COALESCE(ly.actor, ty.actor) AS actor,
            COALESCE(ly.actor_id, ty.actor_id) AS actor_id,
            CASE 
            WHEN ty.current_year IS NULL THEN ly.films
            WHEN ty.current_year IS NOT NULL
            AND ly.current_year IS NULL THEN ty.films
            WHEN ty.current_year IS NOT NULL
            AND ly.current_year IS NOT NULL THEN ty.films || ly.films
            END AS films,
            CASE
            WHEN ty.avg_rating IS NULL THEN ly.quality_class
            ELSE
                CASE
                WHEN ty.avg_rating  <= 6 THEN 'bad'
                WHEN ty.avg_rating <= 7 THEN 'average'
                WHEN ty.avg_rating <= 8 THEN 'good'
                ELSE 'star' END
            END AS quality_class,
            ty.current_year IS NOT NULL is_active,
            COALESCE(ty.current_year, ly.current_year + 1) AS current_year
        FROM
        last_year ly
        FULL OUTER JOIN this_year ty ON ly.actor_id = ty.actor_id
    """
    return query

def job_1(spark_session: SparkSession, 
          input_table_name: str,
          base_table: str,
          current_year: int
)-> Optional[DataFrame]:
  return spark_session.sql(query_1(input_table_name, base_table, current_year))

def main():
    input_table_name = "sundeep.actors"
    base_table = "bootcamp.actor_films"
    current_year = 1915
    output_table_name: str = input_table_name
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_1")
        .getOrCreate()
    )
    output_df = job_1(
       spark_session, 
       input_table_name,
       base_table,
       current_year
    )
    output_df.write.mode("append").insertInto(output_table_name)

if __name__ == "__main__":
   main()
