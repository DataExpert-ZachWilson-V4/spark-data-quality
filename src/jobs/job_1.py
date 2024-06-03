from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_1(output_table_name: str) -> str:
    query = f"""
    WITH last_year_cte AS (
        SELECT *
        FROM 
            {output_table_name}
        WHERE 
            current_year=1913 
    ),
    this_year_agg_cte AS (
        SELECT 
            ty.actor, 
            ty.actor_id,
            ARRAY_AGG(ROW(film, votes, rating, film_id, year)) as films, 
            CASE 
                WHEN AVG(rating) > 8 THEN 'star' 
                WHEN AVG(rating) > 7 THEN 'good' 
                WHEN AVG(rating) > 6 THEN 'average' 
            ELSE 'bad' 
            END as quality_class,
            ty.year as year
        FROM actor_films AS ty
        WHERE year = 1914
        GROUP BY ty.actor, ty.actor_id, ty.year
    )
    SELECT 
        COALESCE(ly.actor, tya.actor) AS actor, 
        COALESCE(ly.actor_id, tya.actor_id) AS actor_id,
        CASE
            WHEN ly.films IS NULL AND tya.films IS NOT NULL THEN tya.films 
            WHEN ly.films IS NOT NULL AND tya.films IS NOT NULL THEN tya.films || ly.films 
            WHEN tya.films IS NULL THEN ly.films 
        END AS films,
        COALESCE (tya.quality_class, ly.quality_class) AS quality_class, 
        tya.year IS NOT NULL AS is_active, 
        COALESCE(tya.year, ly.current_year + 1) AS current_year 
    FROM last_year_cte AS ly
    FULL OUTER JOIN this_year_agg_cte AS tya 
        ON ly.actor_id = tya.actor_id 
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
